"""Provider-neutral collection coordinator; raw retention never implies acceptance."""

from __future__ import annotations

import json
import tempfile
import time
from collections.abc import Callable, Iterator
from datetime import UTC, datetime
from pathlib import Path
from uuid import uuid4, uuid5

from urbanlens.ingestion.acceptance import AcceptanceService
from urbanlens.ingestion.contracts import (
    CollectionCycle,
    Contract,
    Counts,
    Failure,
    FailureClass,
    Partition,
    Run,
    Snapshot,
    Stage,
    ValidationReport,
    advance_run,
)
from urbanlens.ingestion.errors import IngestionError, LeaseLost
from urbanlens.ingestion.interfaces import ArtifactStore, Layer, ProviderAdapter, artifact_key
from urbanlens.ingestion.registry import Lease, Registry, safe_log
from urbanlens.ingestion.storage import gzip_chunks
from urbanlens.ingestion.validation import ValidationRules, Validator


def metadata_key(cycle: CollectionCycle, label: str) -> str:
    return (
        f"manifests/{cycle.provider.value}/{cycle.scope}/{cycle.window_start.date()}/"
        f"{cycle.cycle_id}/{label}.json"
    )


def load_model[Model: Contract](store: ArtifactStore, key: str, model: type[Model]) -> Model | None:
    artifact = store.stat(key)
    if artifact is None:
        return None
    if artifact.byte_count > 2_000_000:
        raise IngestionError(FailureClass.BUDGET, "manifest-size-budget")
    return model.model_validate_json(b"".join(store.read_verified(artifact)))


def save_model(store: ArtifactStore, key: str, model: Contract) -> None:
    store.put_immutable(
        key,
        iter([model.model_dump_json().encode()]),
        media_type="application/json",
        compression="none",
    )


class Collector:
    def __init__(
        self,
        store: ArtifactStore,
        registry: Registry,
        *,
        max_seconds: float = 300,
        max_records: int = 1_000_000,
        log: Callable[[str], None] | None = None,
        acceptance: AcceptanceService | None = None,
    ) -> None:
        self.store, self.registry = store, registry
        self.max_seconds, self.max_records = max_seconds, max_records
        self.log = log or (lambda message: None)
        self.acceptance = acceptance

    def budget(self, start: float, count: int = 0) -> None:
        if time.monotonic() - start > self.max_seconds or count > self.max_records:
            raise IngestionError(FailureClass.BUDGET, "collection-runtime-or-record-budget")

    def archive(self, lease: Lease, cycle: CollectionCycle, run: Run) -> None:
        state, _ = self.registry.owned(lease)
        for previous in state.pending_history:
            save_model(self.store, f"manifests/runs/{previous.run_id}.json", previous)
        self.registry.update(lease, run, history_saved=True)

    def collect(
        self,
        cycle: CollectionCycle,
        adapter: ProviderAdapter,
        rules: ValidationRules,
        *,
        pipeline_version: str,
    ) -> Snapshot:
        if adapter.provider != cycle.provider:
            raise ValueError("Adapter/provider mismatch")
        # Never reinterpret a pinned cycle with a changed query or partition definition.
        save_model(self.store, metadata_key(cycle, "cycle"), cycle)
        existing = load_model(self.store, metadata_key(cycle, "accepted"), Snapshot)
        current = self.registry.current(cycle)
        if existing is None and current is not None and current.cycle.cycle_id == cycle.cycle_id:
            existing = current  # Repair interruption between pointer commit and accepted index.
        if existing is not None:
            if existing.cycle != cycle or existing.run.status != Stage.ACCEPTED:
                raise IngestionError(FailureClass.INTEGRITY, "accepted-cycle-mismatch")
            save_model(self.store, metadata_key(cycle, "accepted"), existing)
            return existing
        run = Run(
            run_id=uuid4(),
            cycle_id=cycle.cycle_id,
            pipeline_version=pipeline_version,
            started_at=datetime.now(UTC),
        )
        lease = self.registry.acquire(cycle, run)
        run = lease.run
        partitions: list[Partition] = []
        active_identity: str | None = None
        committed = False
        started = time.monotonic()
        try:
            self.archive(lease, cycle, run)
            with tempfile.TemporaryDirectory(prefix="urbanlens-validate-") as temporary:
                validator = Validator(rules, Path(temporary) / "duplicates.sqlite")
                try:
                    for identity in cycle.expected_partitions:
                        active_identity = identity
                        self.budget(started, validator.count)
                        self.registry.update(lease, run)
                        cached = load_model(
                            self.store, metadata_key(cycle, "parts/" + identity), Partition
                        )
                        fetched = cached or adapter.fetch(cycle, run, identity, self.store)
                        if (
                            fetched.partition_id != identity
                            or fetched.cycle_id != cycle.cycle_id
                            or fetched.pipeline_version != pipeline_version
                            or fetched.source_edition != cycle.source_edition
                        ):
                            raise IngestionError(
                                FailureClass.INTEGRITY, "partition-identity-mismatch"
                            )
                        partitions.append(fetched)
                        save_model(
                            self.store,
                            metadata_key(cycle, f"attempts/{run.run_id}/{identity}-fetched"),
                            fetched,
                        )
                        if fetched.failure:
                            raise IngestionError(
                                fetched.failure.category,
                                fetched.failure.code,
                                retryable=fetched.failure.retryable,
                            )
                        # Check every byte even if the provider parser reads only a prefix.
                        for artifact in fetched.artifacts:
                            for _ in self.store.read_verified(artifact):
                                self.budget(started)
                        accepted, parsed = 0, 0

                        def records(partition: Partition = fetched) -> Iterator[bytes]:
                            nonlocal accepted, parsed
                            for row in adapter.parse(partition, self.store):
                                weight = validator.weight(row)
                                parsed += weight
                                self.budget(started, validator.count + weight)
                                if validator.consume(row):
                                    accepted += weight
                                    yield (
                                        json.dumps(
                                            dict(row),
                                            ensure_ascii=False,
                                            sort_keys=True,
                                            allow_nan=False,
                                        )
                                        + "\n"
                                    ).encode()

                        key = artifact_key(
                            Layer.SILVER, cycle, run.run_id, "records.jsonl.gz", identity
                        )
                        output = self.store.put_immutable(
                            key,
                            gzip_chunks(records()),
                            media_type="application/x-ndjson",
                            compression="gzip",
                        )
                        self.budget(started, validator.count)
                        save_model(
                            self.store,
                            metadata_key(cycle, f"attempts/{run.run_id}/{identity}-silver"),
                            output,
                        )
                        raw = fetched.counts.raw if fetched.counts.raw is not None else parsed
                        complete = raw == parsed
                        part = Partition.model_validate(
                            {
                                **fetched.model_dump(),
                                "status": "complete" if complete else "parsed",
                                "counts": Counts(
                                    raw=raw,
                                    parsed=parsed,
                                    accepted=accepted,
                                    rejected=parsed - accepted,
                                ),
                            }
                        )
                        partitions[-1] = part
                        if complete:
                            save_model(self.store, metadata_key(cycle, "parts/" + identity), part)
                        active_identity = None
                    complete = all(p.status == "complete" for p in partitions)
                    previous_count = None
                    if current and self.acceptance is None:
                        previous_count = sum(p.counts.parsed or 0 for p in current.partitions)
                    validation = validator.report(complete=complete, previous_count=previous_count)
                    self.store.put_immutable(
                        metadata_key(cycle, f"attempts/{run.run_id}/rejection-reasons"),
                        iter([json.dumps(dict(validator.reasons), sort_keys=True).encode()]),
                        media_type="application/json",
                        compression="none",
                    )
                finally:
                    validator.close()
            if self.acceptance is not None:
                validation = self.acceptance.evaluate(
                    cycle, run, tuple(partitions), validation, current
                )
            for stage in (Stage.FETCHED, Stage.PARSED):
                run = advance_run(run, stage)
                self.registry.update(lease, run)
            if complete:
                run = advance_run(run, Stage.COMPLETE)
                self.registry.update(lease, run)
            status = Stage.ACCEPTED if validation.passed and complete else Stage.REJECTED
            run = advance_run(run, status, ended_at=datetime.now(UTC))
            snapshot = Snapshot(
                snapshot_id=uuid5(cycle.cycle_id, "source-snapshot"),
                cycle=cycle,
                run=run,
                completeness="complete" if complete else "partial",
                partitions=tuple(partitions),
                validation=validation,
            )
            save_model(self.store, metadata_key(cycle, f"attempts/{run.run_id}/snapshot"), snapshot)
            self.budget(started)
            # Lease and current pointer change in the SAME conditional state update.
            self.registry.update(lease, run, current=snapshot if status == Stage.ACCEPTED else None)
            committed = True
            save_model(self.store, f"manifests/runs/{run.run_id}.json", run)
            if status == Stage.ACCEPTED:
                save_model(self.store, metadata_key(cycle, "accepted"), snapshot)
            self.log(safe_log(run))
            return snapshot
        except BaseException as error:
            failure = (
                error.failure
                if isinstance(error, IngestionError)
                else Failure(
                    category=FailureClass.SCHEMA
                    if isinstance(error, ValueError)
                    else FailureClass.INTERNAL,
                    code="worker-interrupted"
                    if isinstance(error, (KeyboardInterrupt, SystemExit))
                    else "parse-or-contract-error"
                    if isinstance(error, ValueError)
                    else "collection-error",
                    retryable=not isinstance(error, ValueError),
                )
            )
            if not committed:
                run = Run.model_validate(
                    {
                        **run.model_dump(),
                        "status": Stage.FAILED,
                        "ended_at": datetime.now(UTC),
                        "failure": failure,
                    }
                )
            if active_identity is not None:
                previous_part = (
                    partitions[-1]
                    if partitions and partitions[-1].partition_id == active_identity
                    else None
                )
                failed_part = Partition.model_validate(
                    {
                        **(
                            previous_part.model_dump()
                            if previous_part
                            else {
                                "partition_id": active_identity,
                                "cycle_id": cycle.cycle_id,
                                "run_id": run.run_id,
                                "schema_version": "unavailable",
                                "pipeline_version": pipeline_version,
                                "source_edition": cycle.source_edition,
                            }
                        ),
                        "status": "failed",
                        "failure": failure,
                    }
                )
                if previous_part:
                    partitions[-1] = failed_part
                else:
                    partitions.append(failed_part)
            try:
                self.registry.update(lease, run)
                save_model(self.store, f"manifests/runs/{run.run_id}.json", run)
                if active_identity:
                    save_model(
                        self.store,
                        metadata_key(cycle, f"attempts/{run.run_id}/{active_identity}-failed"),
                        partitions[-1],
                    )
                failed = Snapshot(
                    snapshot_id=uuid5(run.run_id, "failed-snapshot"),
                    cycle=cycle,
                    run=run,
                    completeness="failed",
                    partitions=tuple(partitions),
                    validation=ValidationReport(rule_version=rules.version),
                )
                save_model(
                    self.store, metadata_key(cycle, f"attempts/{run.run_id}/snapshot"), failed
                )
            except (IngestionError, ValueError):
                # An expired worker must not publish; the registry retains its last checkpoint.
                pass
            self.log(safe_log(run))
            if isinstance(error, (KeyboardInterrupt, SystemExit)):
                raise
            raise IngestionError(
                failure.category, failure.code, retryable=failure.retryable
            ) from None
        finally:
            try:
                self.registry.release(lease)
            except LeaseLost:
                pass

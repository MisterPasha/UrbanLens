"""Read-only source replay and explicitly synthetic recovery drills for Bronze acceptance."""

from __future__ import annotations

import hashlib
import json
import tempfile
from collections.abc import Callable, Iterator, Mapping
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any
from uuid import NAMESPACE_URL, uuid4, uuid5

from urbanlens.ingestion.cadence import month_window
from urbanlens.ingestion.contracts import (
    Artifact,
    CollectionCycle,
    Counts,
    FailureClass,
    Partition,
    Provider,
    Run,
    Snapshot,
    Stage,
    advance_run,
)
from urbanlens.ingestion.engine import Collector, load_model, metadata_key, save_model
from urbanlens.ingestion.errors import IngestionError
from urbanlens.ingestion.interfaces import ArtifactStore, Layer, ProviderAdapter, artifact_key
from urbanlens.ingestion.payload import payload
from urbanlens.ingestion.registry import Registry
from urbanlens.ingestion.storage import gzip_chunks
from urbanlens.ingestion.validation import ValidationRules, Validator


def replay_snapshot(
    snapshot: Snapshot, adapter: ProviderAdapter, rules: ValidationRules, store: ArtifactStore
) -> dict[str, Any]:
    """Reparse original retained bytes, compare normalized hashes, never promote a snapshot."""
    if snapshot.run.status != Stage.ACCEPTED:
        raise ValueError("Replay comparison requires an accepted original")
    run = Run(
        run_id=uuid4(),
        cycle_id=snapshot.cycle.cycle_id,
        pipeline_version=snapshot.run.pipeline_version,
        started_at=datetime.now(UTC),
        mode="replay",
        replay_of_snapshot_id=snapshot.snapshot_id,
    )
    matched = []
    compressed_matches = []
    partition_hashes = []
    with tempfile.TemporaryDirectory(prefix="bronze-replay-") as directory:
        validator = Validator(rules, Path(directory) / "ids.sqlite")
        try:
            for part in snapshot.partitions:
                for artifact in part.artifacts:
                    for _ in store.read_verified(artifact):
                        pass

                record_digest = hashlib.sha256()
                record_bytes = 0

                def records(
                    partition: Partition = part,
                    update_hash: Callable[[bytes], None] = record_digest.update,
                ) -> Iterator[bytes]:
                    nonlocal record_bytes
                    for row in adapter.parse(partition, store):
                        if validator.consume(row):
                            block = (
                                json.dumps(
                                    dict(row), ensure_ascii=False, sort_keys=True, allow_nan=False
                                )
                                + "\n"
                            ).encode()
                            update_hash(block)
                            record_bytes += len(block)
                            yield block

                digest = hashlib.sha256()
                for block in gzip_chunks(records()):
                    digest.update(block)
                previous = load_model(
                    store,
                    metadata_key(
                        snapshot.cycle, f"attempts/{snapshot.run.run_id}/{part.partition_id}-silver"
                    ),
                    Artifact,
                )
                if previous is None:
                    raise ValueError("Missing normalized artifact reference")
                with payload(store, previous) as stream:
                    original_digest = hashlib.sha256()
                    while block := stream.read(64 * 1024):
                        original_digest.update(block)
                    expected_digest = original_digest.hexdigest()
                    expected_size = stream.tell()
                # Stored-byte SHA remains mandatory for every retained artifact.
                # Replay equality uses exact decoded JSONL bytes: gzip headers and
                # deflate output can differ between Windows/Linux zlib runtimes.
                matched.append(
                    expected_digest == record_digest.hexdigest() and expected_size == record_bytes
                )
                compressed_matches.append(previous.sha256 == digest.hexdigest())
                partition_hashes.append(
                    {
                        "partition": part.partition_id,
                        "original_stored_sha256": previous.sha256,
                        "replayed_stored_sha256": digest.hexdigest(),
                        "original_records_sha256": expected_digest,
                        "replayed_records_sha256": record_digest.hexdigest(),
                        "original_records_bytes": expected_size,
                        "replayed_records_bytes": record_bytes,
                    }
                )
            count = validator.count
            expected = sum(p.counts.raw or 0 for p in snapshot.partitions)
            report = validator.report(complete=count == expected, previous_count=None)
            if not all(matched) or not report.passed:
                raise ValueError("Source replay did not reproduce accepted data")
        finally:
            validator.close()
    run = advance_run(run, Stage.FETCHED)
    run = advance_run(run, Stage.PARSED, ended_at=datetime.now(UTC))
    save_model(store, f"manifests/runs/{run.run_id}.json", run)
    return {
        "run": run.model_dump(mode="json"),
        "snapshot_id": str(snapshot.snapshot_id),
        "records": count,
        "normalized_hashes_match": all(matched),
        "normalized_hash_basis": "uncompressed-utf8-jsonl",
        "stored_compressed_hashes_match": all(compressed_matches),
        "partition_hashes": partition_hashes,
        "source_fetched_at": [
            p.fetched_at.isoformat() if p.fetched_at else None for p in snapshot.partitions
        ],
        "new_observations": 0,
    }


class RecoveryFixture:
    """Invented rows in an isolated scope; cannot be mistaken for provider observations."""

    provider = Provider.FSA

    def __init__(self, fail: str | None = None) -> None:
        self.fail = fail
        self.fetched: list[str] = []

    def fetch(
        self, cycle: CollectionCycle, run: Run, partition_id: str, store: ArtifactStore
    ) -> Partition:
        self.fetched.append(partition_id)
        if partition_id == self.fail:
            raise IngestionError(FailureClass.NETWORK, "synthetic-provider-failure", retryable=True)
        raw = store.put_immutable(
            artifact_key(Layer.BRONZE, cycle, run.run_id, "synthetic.json", partition_id),
            iter([json.dumps({"id": partition_id, "synthetic": True}).encode()]),
            media_type="application/json",
            compression="none",
        )
        return Partition(
            partition_id=partition_id,
            cycle_id=cycle.cycle_id,
            run_id=run.run_id,
            schema_version="synthetic-recovery-v1",
            pipeline_version=run.pipeline_version,
            status="fetched",
            fetched_at=datetime.now(UTC),
            artifacts=(raw,),
            counts=Counts(raw=1),
        )

    def parse(self, partition: Partition, store: ArtifactStore) -> Iterator[Mapping[str, object]]:
        yield json.loads(b"".join(store.read_verified(partition.artifacts[0])))


def recovery_drill(store: ArtifactStore, registry: Registry) -> dict[str, Any]:
    results = []
    start, end = month_window(datetime.now(UTC).date())
    previous_start, _ = month_window((start - timedelta(days=1)).date())
    drill_id = uuid4()
    for mode, failed_part in (("partition", "part-02"), ("provider", "part-01")):
        scope = f"synthetic-step18-{mode}-{drill_id}"

        def cycle(version: int, scope: str = scope) -> CollectionCycle:
            return CollectionCycle(
                cycle_id=uuid5(NAMESPACE_URL, f"{scope}/{version}"),
                provider=Provider.FSA,
                scope=scope,
                query_version="synthetic-recovery-v1",
                query_sha256=hashlib.sha256(str(version).encode()).hexdigest(),
                window_start=previous_start if version == 1 else start,
                window_end=start if version == 1 else end,
                expected_partitions=("part-01", "part-02"),
            )

        rules = ValidationRules(
            version="synthetic-recovery-v1",
            identity_field="id",
            required_fields=("synthetic",),
            min_records=2,
            max_records=2,
        )
        collector = Collector(store, registry)
        baseline = collector.collect(
            cycle(1), RecoveryFixture(), rules, pipeline_version="synthetic-recovery-v1"
        )
        try:
            collector.collect(
                cycle(2),
                RecoveryFixture(failed_part),
                rules,
                pipeline_version="synthetic-recovery-v1",
            )
        except IngestionError as error:
            if error.failure.code != "synthetic-provider-failure":
                raise
        else:
            raise ValueError("Injected provider failure was not recorded")
        state, _ = registry.state(Registry.key(cycle(2)))
        assert state.active_run is not None
        failed = load_model(
            store,
            metadata_key(cycle(2), f"attempts/{state.active_run.run_id}/snapshot"),
            Snapshot,
        )
        if failed is None:
            raise ValueError("Failed provider lacks a durable snapshot manifest")
        preserved = registry.current(cycle(2)) == baseline
        adapter = RecoveryFixture()
        recovered = collector.collect(
            cycle(2), adapter, rules, pipeline_version="synthetic-recovery-v1"
        )
        expected_fetches = ["part-02"] if mode == "partition" else ["part-01", "part-02"]
        if (
            failed.run.status != Stage.FAILED
            or not preserved
            or recovered.run.status != Stage.ACCEPTED
            or adapter.fetched != expected_fetches
        ):
            raise ValueError("Recovery drill failed")
        repeated = collector.collect(
            cycle(2), RecoveryFixture("part-01"), rules, pipeline_version="synthetic-recovery-v1"
        )
        if repeated != recovered:
            raise ValueError("Recovered cycle was not idempotent")
        results.append(
            {
                "mode": mode,
                "failed_run": str(failed.run.run_id),
                "recovered_run": str(recovered.run.run_id),
                "previous_current_preserved": preserved,
                "resumed_fetches": adapter.fetched,
                "identical_rerun": True,
            }
        )
    return {"synthetic": True, "production_scopes_unchanged": True, "results": results}

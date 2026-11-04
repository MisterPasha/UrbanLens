"""Raw fetch and retained-data replay. Provider acceptance is implemented by later adapters."""

from __future__ import annotations

import argparse
import csv
import gzip
import hashlib
import io
import json
import tempfile
import time
import xml.etree.ElementTree as ET
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from uuid import NAMESPACE_URL, UUID, uuid4, uuid5

from pydantic import NonNegativeInt

from urbanlens.ingestion.contracts import (
    CollectionCycle,
    Contract,
    Failure,
    FailureClass,
    Partition,
    Provider,
    Run,
    Snapshot,
    Stage,
    ValidationReport,
    advance_run,
    validate_replay,
)
from urbanlens.ingestion.engine import Collector, load_model, metadata_key, save_model
from urbanlens.ingestion.errors import IngestionError, LeaseLost
from urbanlens.ingestion.http import HttpDownloader, HttpPolicy
from urbanlens.ingestion.interfaces import Layer, artifact_key
from urbanlens.ingestion.registry import Registry, SQLiteStateBackend, safe_log
from urbanlens.ingestion.storage import LocalArtifactStore, chunks, gzip_chunks


def fetch_raw(
    cycle: CollectionCycle,
    url: str,
    store: LocalArtifactStore,
    registry: Registry,
    downloader: HttpDownloader,
) -> Snapshot:
    if downloader.provider != cycle.provider or cycle.expected_partitions != ("full",):
        raise ValueError("Raw fetch requires a matching provider and one full partition")
    if cycle.query_sha256 != hashlib.sha256(url.encode()).hexdigest():
        raise ValueError("Source URL differs from the pinned raw collection query")
    save_model(store, metadata_key(cycle, "cycle"), cycle)
    existing = load_model(store, metadata_key(cycle, "raw-snapshot"), Snapshot)
    if existing:
        return existing
    run = Run(
        run_id=uuid4(),
        cycle_id=cycle.cycle_id,
        pipeline_version="raw-http-v1",
        started_at=datetime.now(UTC),
    )
    lease = registry.acquire(cycle, run)
    run = lease.run
    try:
        Collector(store, registry).archive(lease, cycle, run)
        with tempfile.TemporaryFile() as downloaded:
            downloader.download(url, downloaded)
            fetched_at = datetime.now(UTC)
            artifact = store.put_immutable(
                artifact_key(Layer.BRONZE, cycle, run.run_id, "source.bin.gz", "full"),
                gzip_chunks(chunks(downloaded)),
                media_type="application/octet-stream",
                compression="gzip",
            )
        part = Partition(
            partition_id="full",
            cycle_id=cycle.cycle_id,
            run_id=run.run_id,
            source_edition=cycle.source_edition,
            schema_version="uninterpreted-v1",
            pipeline_version=run.pipeline_version,
            status="fetched",
            fetched_at=fetched_at,
            artifacts=(artifact,),
        )
        run = advance_run(run, Stage.FETCHED, ended_at=datetime.now(UTC))
        snapshot = Snapshot(
            snapshot_id=uuid5(cycle.cycle_id, "raw-snapshot"),
            cycle=cycle,
            run=run,
            completeness="pending",
            partitions=(part,),
            validation=ValidationReport(rule_version="provider-validation-not-run"),
        )
        save_model(store, metadata_key(cycle, f"attempts/{run.run_id}/snapshot"), snapshot)
        registry.update(lease, run)
        save_model(store, f"manifests/runs/{run.run_id}.json", run)
        save_model(store, metadata_key(cycle, "raw-snapshot"), snapshot)
        return snapshot
    except BaseException as error:
        failure = (
            error.failure
            if isinstance(error, IngestionError)
            else Failure(
                category=FailureClass.INTERNAL, code="raw-fetch-interrupted", retryable=True
            )
        )
        if run.ended_at is None:
            run = advance_run(run, Stage.FAILED, ended_at=datetime.now(UTC), failure=failure)
        try:
            registry.update(lease, run)
            save_model(store, f"manifests/runs/{run.run_id}.json", run)
        except IngestionError:
            pass
        if isinstance(error, (KeyboardInterrupt, SystemExit)):
            raise
        raise IngestionError(failure.category, failure.code, retryable=failure.retryable) from None
    finally:
        try:
            registry.release(lease)
        except LeaseLost:
            pass


class ReplaySummary(Contract):
    run: Run
    original_snapshot_id: UUID
    parsed_records: NonNegativeInt
    acceptance: str = "not-evaluated"


def replay_raw(
    original: Snapshot,
    store: LocalArtifactStore,
    registry: Registry,
    *,
    format: str,
    records_key: str | None = None,
    record_tag: str | None = None,
    max_bytes: int = 16 * 1024 * 1024,
    max_records: int = 1_000_000,
    max_seconds: float = 120,
) -> ReplaySummary:
    run = Run(
        run_id=uuid4(),
        cycle_id=original.cycle.cycle_id,
        pipeline_version="raw-replay-v1",
        mode="replay",
        replay_of_snapshot_id=original.snapshot_id,
        started_at=datetime.now(UTC),
    )
    validate_replay(run, original)
    lease = registry.acquire(original.cycle, run)
    run = lease.run
    started, count, processed_bytes = time.monotonic(), 0, 0

    def budget(size: int = 0) -> None:
        if size > max_bytes or count > max_records or time.monotonic() - started > max_seconds:
            raise IngestionError(FailureClass.BUDGET, "replay-budget")

    try:
        Collector(store, registry).archive(lease, original.cycle, run)
        run = advance_run(run, Stage.FETCHED)
        registry.update(lease, run)
        for partition in original.partitions:
            if not partition.artifacts:
                raise IngestionError(FailureClass.INCOMPLETE, "replay-missing-artifact")
            for artifact in partition.artifacts:
                with tempfile.TemporaryFile() as stored, tempfile.TemporaryFile() as decoded:
                    for block in store.read_verified(artifact):
                        stored.write(block)
                        processed_bytes += len(block)
                        budget(processed_bytes)
                    stored.seek(0)
                    if artifact.compression == "zip":
                        raise IngestionError(FailureClass.SCHEMA, "zip-requires-provider-parser")
                    source = (
                        gzip.GzipFile(fileobj=stored) if artifact.compression == "gzip" else stored
                    )
                    try:
                        while block := source.read(64 * 1024):
                            decoded.write(block)
                            processed_bytes += len(block)
                            budget(processed_bytes)
                    finally:
                        if source is not stored:
                            source.close()
                    decoded.seek(0)
                    if format == "json":
                        value = json.load(decoded)
                        if records_key is not None:
                            if not isinstance(value, dict) or records_key not in value:
                                raise ValueError("Missing record collection")
                            value = value[records_key]
                        if not isinstance(value, list):
                            raise ValueError("JSON replay requires a list of records")
                        count += len(value)
                        budget()
                    elif format == "xml":
                        if not record_tag:
                            raise ValueError("XML replay requires --record-tag")
                        events = ET.iterparse(decoded, events=("start", "end"))
                        _, root = next(events)
                        for event, element in events:
                            if event != "end":
                                continue
                            if element.tag == record_tag:
                                count += 1
                            element.clear()
                            root.clear()
                            budget()
                    elif format == "csv":
                        wrapper = io.TextIOWrapper(decoded, encoding="utf-8-sig", newline="")
                        try:
                            rows = csv.DictReader(wrapper)
                            if not rows.fieldnames or len(set(rows.fieldnames)) != len(
                                rows.fieldnames
                            ):
                                raise ValueError("Invalid CSV header")
                            for row in rows:
                                if None in row or None in row.values():
                                    raise ValueError("Malformed CSV record")
                                count += 1
                                budget()
                        finally:
                            wrapper.detach()
                    else:
                        raise ValueError("Unsupported replay format")
        budget()
        run = advance_run(run, Stage.PARSED, ended_at=datetime.now(UTC))
        result = ReplaySummary(
            run=run, original_snapshot_id=original.snapshot_id, parsed_records=count
        )
        save_model(store, metadata_key(original.cycle, f"replays/{run.run_id}"), result)
        registry.update(lease, run)  # Replay never supplies a new current snapshot.
        save_model(store, f"manifests/runs/{run.run_id}.json", run)
        return result
    except BaseException as error:
        failure = (
            error.failure
            if isinstance(error, IngestionError)
            else Failure(category=FailureClass.SCHEMA, code="replay-parse-failed", retryable=False)
        )
        if run.ended_at is None:
            run = advance_run(run, Stage.FAILED, ended_at=datetime.now(UTC), failure=failure)
        try:
            registry.update(lease, run)
            save_model(store, f"manifests/runs/{run.run_id}.json", run)
        except IngestionError:
            pass
        if isinstance(error, (KeyboardInterrupt, SystemExit)):
            raise
        raise IngestionError(failure.category, failure.code, retryable=failure.retryable) from None
    finally:
        try:
            registry.release(lease)
        except LeaseLost:
            pass


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=Path("data/ingestion"))
    tasks = parser.add_subparsers(dest="task", required=True)
    fetch = tasks.add_parser("fetch", help="Retain one raw response; do not accept it")
    fetch.add_argument("--provider", type=Provider, choices=list(Provider), required=True)
    fetch.add_argument("--scope", required=True)
    fetch.add_argument("--url", required=True)
    fetch.add_argument("--date", type=date.fromisoformat, default=datetime.now(UTC).date())
    fetch.add_argument("--source-edition")
    replay = tasks.add_parser(
        "replay", help="Parse retained raw data; do not refresh source freshness"
    )
    replay.add_argument("snapshot", type=Path)
    replay.add_argument("--format", choices=["json", "xml", "csv"], required=True)
    replay.add_argument("--records-key")
    replay.add_argument("--record-tag")
    inspect = tasks.add_parser("inspect", help="Show snapshot metadata; no provider request")
    inspect.add_argument("snapshot", type=Path)
    args = parser.parse_args()
    store = LocalArtifactStore(args.root / "objects")
    registry = Registry(SQLiteStateBackend(args.root / "registry.sqlite"))
    try:
        if args.task == "fetch":
            sha = hashlib.sha256(args.url.encode()).hexdigest()
            start = datetime.combine(args.date, datetime.min.time(), tzinfo=UTC)
            cycle = CollectionCycle(
                cycle_id=uuid5(NAMESPACE_URL, f"{args.provider}/{args.scope}/{args.date}/{sha}"),
                provider=args.provider,
                scope=args.scope,
                query_version="raw-http-v1",
                query_sha256=sha,
                window_start=start,
                window_end=start + timedelta(days=1),
                source_edition=args.source_edition,
            )
            snapshot = fetch_raw(
                cycle, args.url, store, registry, HttpDownloader(args.provider, HttpPolicy())
            )
            print(safe_log(snapshot.run))
            print(store.path(metadata_key(cycle, "raw-snapshot")))
        else:
            snapshot = Snapshot.model_validate_json(args.snapshot.read_text(encoding="utf-8"))
            if args.task == "inspect":
                print(snapshot.model_dump_json(indent=2))
            else:
                result = replay_raw(
                    snapshot,
                    store,
                    registry,
                    format=args.format,
                    records_key=args.records_key,
                    record_tag=args.record_tag,
                )
                print(safe_log(result.run))
                print(f"Parsed {result.parsed_records} records; provider acceptance not evaluated")
    except IngestionError as error:
        raise SystemExit(error.failure.model_dump_json()) from None
    except (ValueError, OSError):
        raise SystemExit("Invalid command input or unavailable local artifact") from None


if __name__ == "__main__":
    main()

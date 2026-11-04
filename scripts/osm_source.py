"""Collect a full OSM query or verify deterministic replay of its retained snapshot."""

from __future__ import annotations

import argparse
import json
import tempfile
from collections import Counter
from collections.abc import Iterator
from datetime import UTC, datetime
from pathlib import Path
from uuid import uuid4

from scripts.source_pipeline import acceptance_service
from urbanlens.ingestion.cadence import legacy_month_snapshot
from urbanlens.ingestion.contracts import (
    Artifact,
    Failure,
    FailureClass,
    Partition,
    Run,
    Stage,
    advance_run,
)
from urbanlens.ingestion.engine import Collector, load_model, metadata_key, save_model
from urbanlens.ingestion.errors import IngestionError, LeaseLost
from urbanlens.ingestion.interfaces import Layer, artifact_key
from urbanlens.ingestion.osm import PIPELINE, OsmAdapter, OsmConfig, local_source
from urbanlens.ingestion.payload import payload
from urbanlens.ingestion.registry import Registry, SQLiteStateBackend, safe_log
from urbanlens.ingestion.storage import LocalArtifactStore, gzip_chunks
from urbanlens.ingestion.validation import Validator

ROOT = Path(__file__).resolve().parents[1]


def check(config: OsmConfig, store: LocalArtifactStore, registry: Registry) -> dict[str, object]:
    snapshot = registry.current(config.cycle(datetime.now(UTC).date()))
    if snapshot is None:
        raise IngestionError(FailureClass.INCOMPLETE, "osm-no-accepted-snapshot")
    run = Run(
        run_id=uuid4(),
        cycle_id=snapshot.cycle.cycle_id,
        pipeline_version=PIPELINE,
        mode="replay",
        replay_of_snapshot_id=snapshot.snapshot_id,
        started_at=datetime.now(UTC),
    )
    lease = registry.acquire(snapshot.cycle, run)
    run = lease.run
    try:
        Collector(store, registry).archive(lease, snapshot.cycle, run)
        run = advance_run(run, Stage.FETCHED)
        registry.update(lease, run)
        # Replay the original pinned configuration, even after a cadence/config change.
        with payload(store, snapshot.partitions[0].artifacts[1]) as stream:
            original_config = OsmConfig.model_validate(json.load(stream)["config"])
        adapter = OsmAdapter(original_config)
        locations: Counter[str] = Counter()
        features: Counter[str] = Counter()
        matches = []
        with tempfile.TemporaryDirectory(prefix="osm-replay-") as temporary:
            validator = Validator(config.rules(), Path(temporary) / "ids.sqlite")
            try:
                for part in snapshot.partitions:

                    def records(partition: Partition = part) -> Iterator[bytes]:
                        for row in adapter.parse(partition, store):
                            locations[str(row["geometry_state"])] += 1
                            features["in_city" if row["in_city"] else "outside_or_unlocated"] += 1
                            features[
                                "venue_tag" if row["venue_tag"] else "fhrs_reference_only"
                            ] += 1
                            if row["crosses_city_boundary"]:
                                features["crosses_city_boundary"] += 1
                            if row["source_version"] is None:
                                features["missing_source_version"] += 1
                            if validator.consume(row):
                                yield (
                                    json.dumps(
                                        dict(row),
                                        ensure_ascii=False,
                                        sort_keys=True,
                                        allow_nan=False,
                                    )
                                    + "\n"
                                ).encode()

                    rebuilt = store.put_immutable(
                        artifact_key(
                            Layer.SILVER,
                            snapshot.cycle,
                            run.run_id,
                            "records.jsonl.gz",
                            part.partition_id,
                        ),
                        gzip_chunks(records()),
                        media_type="application/x-ndjson",
                        compression="gzip",
                    )
                    previous = load_model(
                        store,
                        metadata_key(
                            snapshot.cycle,
                            f"attempts/{snapshot.run.run_id}/{part.partition_id}-silver",
                        ),
                        Artifact,
                    )
                    if previous is None:
                        raise IngestionError(
                            FailureClass.INTEGRITY, "osm-missing-normalized-reference"
                        )
                    for _ in store.read_verified(previous):
                        pass
                    matches.append(previous.sha256 == rebuilt.sha256)
                expected = sum(p.counts.raw or 0 for p in snapshot.partitions)
                valid = validator.report(complete=validator.count == expected, previous_count=None)
                if not valid.passed or not all(matches):
                    raise IngestionError(FailureClass.INTEGRITY, "osm-replay-does-not-reconcile")
                count = validator.count
            finally:
                validator.close()
        run = advance_run(run, Stage.PARSED, ended_at=datetime.now(UTC))
        registry.update(lease, run)
        save_model(store, f"manifests/runs/{run.run_id}.json", run)
        report: dict[str, object] = {
            "checked_at": datetime.now(UTC).isoformat(),
            "mode": "replay",
            "snapshot_id": str(snapshot.snapshot_id),
            "source_fetched_at": [
                p.fetched_at.isoformat() if p.fetched_at else None for p in snapshot.partitions
            ],
            "source_extracted_at": [
                p.source_extracted_at.isoformat() if p.source_extracted_at else None
                for p in snapshot.partitions
            ],
            "records": count,
            "normalized_hashes_match": all(matches),
            "geometry_states": dict(locations),
            "features": dict(features),
            "current_snapshot_unchanged": registry.current(snapshot.cycle) == snapshot,
        }
        store.put_immutable(
            metadata_key(snapshot.cycle, f"replays/{run.run_id}-check"),
            iter([json.dumps(report, sort_keys=True).encode()]),
            media_type="application/json",
            compression="none",
        )
        return report
    except BaseException as error:
        failure = (
            error.failure
            if isinstance(error, IngestionError)
            else Failure(category=FailureClass.INTERNAL, code="osm-replay-failed", retryable=False)
        )
        if run.ended_at is None:
            run = advance_run(run, Stage.FAILED, ended_at=datetime.now(UTC), failure=failure)
            registry.update(lease, run)
            save_model(store, f"manifests/runs/{run.run_id}.json", run)
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
    parser.add_argument("task", choices=["fetch", "check"])
    args = parser.parse_args()
    config = OsmConfig.model_validate_json((ROOT / "config/osm-source.json").read_text())
    directory = ROOT / "data/ingestion"
    store = LocalArtifactStore(directory / "objects")
    registry = Registry(SQLiteStateBackend(directory / "registry.sqlite"))
    try:
        if args.task == "fetch":
            cycle = config.cycle(datetime.now(UTC).date())
            result = legacy_month_snapshot(registry, cycle, config, "osm-venues-v1") or Collector(
                store, registry, log=print, acceptance=acceptance_service(store)
            ).collect(
                cycle,
                OsmAdapter(config, local_source(ROOT)[1]),
                config.rules(),
                pipeline_version=PIPELINE,
            )
            print(safe_log(result.run))
            if result.run.status != Stage.ACCEPTED:
                raise IngestionError(FailureClass.SCHEMA, "osm-collection-rejected")
            print(store.path(metadata_key(result.cycle, "accepted")))
        else:
            report = check(config, store, registry)
            (directory / "osm-check.json").write_text(
                json.dumps(report, indent=2) + "\n", encoding="utf-8"
            )
            print(json.dumps(report, indent=2))
    except IngestionError as error:
        raise SystemExit(error.failure.model_dump_json()) from None


if __name__ == "__main__":
    main()

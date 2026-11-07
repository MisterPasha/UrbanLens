"""Assess accepted monthly cycles and retain normalised, private event evidence."""

from __future__ import annotations

import argparse
import hashlib
import json
from collections import Counter
from typing import Any, cast

from scripts.database import credential
from scripts.silver import ROOT, bundle_for, environment
from urbanlens.database.migrations import connect
from urbanlens.ingestion.contracts import Artifact, Snapshot, Stage
from urbanlens.ingestion.fsa import FsaConfig
from urbanlens.ingestion.interfaces import ArtifactStore
from urbanlens.ingestion.osm import local_source
from urbanlens.silver.classification import save_json
from urbanlens.silver.eligibility import digest
from urbanlens.silver.history import project, required
from urbanlens.silver.load import load


def query_transitions() -> frozenset[tuple[str, str]]:
    """The owner-approved cadence change alters only the source version field."""
    fsa = FsaConfig.model_validate_json(
        (ROOT / "config/fsa-source.json").read_text(encoding="utf-8")
    )
    osm, _ = local_source(ROOT)
    return frozenset(
        (
            hashlib.sha256(
                config.model_copy(update={"version": legacy}).model_dump_json().encode()
            ).hexdigest(),
            hashlib.sha256(config.model_dump_json().encode()).hexdigest(),
        )
        for config, legacy in ((fsa, "fsa-authority-v1"), (osm, "osm-venues-v1"))
    )


def failed_attempts(
    store: ArtifactStore, snapshots: list[Snapshot]
) -> tuple[tuple[Snapshot, Artifact], ...]:
    """Read attempt manifests only; never fetch raw provider payloads or source APIs."""
    remote = cast(Any, store)
    found = []
    scanned = 0
    scopes = {(s.cycle.provider.value, s.cycle.scope) for s in snapshots}
    for provider, scope in sorted(scopes):
        prefix = remote.prefix + f"manifests/{provider}/{scope}/"
        for page in remote.client.get_paginator("list_objects_v2").paginate(
            Bucket=remote.bucket, Prefix=prefix
        ):
            for item in page.get("Contents", []):
                scanned += 1
                if scanned > 10000:
                    raise ValueError(
                        "History manifest scan exceeded its budget; archive/checkpoint required"
                    )
                key = item["Key"]
                if not key.endswith("/snapshot.json") or "/attempts/" not in key:
                    continue
                artifact = store.stat(key[len(remote.prefix) :])
                if artifact is None or artifact.byte_count > 2_000_000:
                    raise ValueError("Missing or oversized history attempt manifest")
                snapshot = Snapshot.model_validate_json(b"".join(store.read_verified(artifact)))
                if snapshot.run.status in {
                    Stage.FAILED,
                    Stage.REJECTED,
                } or snapshot.completeness in {"partial", "failed"}:
                    found.append((snapshot, artifact))
    return tuple(sorted(found, key=lambda x: (x[0].cycle.window_start, x[0].run.started_at)))


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("task", choices=["run", "sources", "project"])
    parser.add_argument("--target", choices=["local", "neon"], default="local")
    args = parser.parse_args()
    _, store, _, _, _, _ = environment("s3")
    with connect(credential(args.target, "pipeline")) as connection:
        with connection.transaction():
            connection.execute("SELECT pg_advisory_xact_lock(192020,1)")
            source_report = None
            if args.task != "project":
                bundle, _ = bundle_for(store, "s3")
                snapshots = list(
                    {
                        b.snapshot.snapshot_id: b.snapshot
                        for b in bundle.batches
                        if b.snapshot is not None
                    }.values()
                )
                source_report = load(
                    connection,
                    store,
                    bundle,
                    interruptions=failed_attempts(store, snapshots),
                    compatible_queries=query_transitions(),
                )
            events = {}
            if args.task != "sources":
                for kind in ("establishment", "company"):
                    events[kind] = project(connection, store, kind)
            counts = dict(
                connection.execute(
                    "SELECT observation_state,count(*) FROM ul_silver.identity_scope_state "
                    "GROUP BY observation_state"
                ).fetchall()
            )
            event_counts = dict(
                Counter(
                    {
                        f"{kind}/{event}": n
                        for kind, event, n in connection.execute(
                            "SELECT entity_type,event_type,count(*) FROM "
                            "ul_silver.change_event GROUP BY entity_type,event_type"
                        )
                    }
                )
            )
            scope_count = required(
                connection.execute("SELECT count(*) FROM ul_silver.history_scope").fetchone()
            )[0]
            source_state_hash = digest(
                connection.execute(
                    "SELECT scope_id::text,identity_id::text,current_observation_id::text,"
                    "last_seen_snapshot_id::text,last_assessed_snapshot_id::text,observation_state,"
                    "value->>'record_hash',processing_version,absence_interrupted "
                    "FROM ul_silver.identity_scope_state ORDER BY scope_id,identity_id"
                ).fetchall()
            )
            projection_hash = digest(
                connection.execute(
                    "SELECT entity_type,entity_key,value,processing_sha256,support_sha256 "
                    "FROM ul_silver.history_projection ORDER BY entity_type,entity_key"
                ).fetchall()
            )
            baselines = [
                r[0]
                for r in connection.execute(
                    "SELECT jsonb_build_object('provider',s.provider,'scope',s.scope_key,"
                    "'baseline_snapshot_id',s.baseline_snapshot_id,'current_snapshot_id',h.snapshot_id,"
                    "'current_cycle_id',h.cycle_id,'query_sha256',h.query_sha256) "
                    "FROM ul_silver.history_scope h JOIN ul_control.source_scope s USING(scope_id) "
                    "ORDER BY s.provider,s.scope_key"
                )
            ]
            report = dict(
                engine="observed-history-v1",
                source_states=counts,
                baselined_scopes=scope_count,
                baselines=baselines,
                source_state_sha256=source_state_hash,
                projection_sha256=projection_hash,
                event_counts=event_counts,
                new_projection_events=events,
                provider_requests=0,
                observation_meaning="Observed changes are not verified openings/closures",
            )
            artifact = save_json(store, "evidence/history-runs", report)
    result = dict(**report, artifact=artifact.model_dump(mode="json"), source_load=source_report)
    (ROOT / f"data/silver/history-{args.target}.json").write_text(
        json.dumps(result, indent=2) + "\n", encoding="utf-8"
    )
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()

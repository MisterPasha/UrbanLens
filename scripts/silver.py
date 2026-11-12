"""Normalise accepted evidence, verify portable artifacts and load bounded working state."""

from __future__ import annotations

import argparse
import json
import os
import sqlite3
from collections import Counter
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import psycopg

from scripts.database import credential
from scripts.source_pipeline import area_codes
from urbanlens.database.migrations import connect
from urbanlens.ingestion.archive import load_catalog
from urbanlens.ingestion.cloud import sdk_client
from urbanlens.ingestion.contracts import Artifact, Snapshot
from urbanlens.ingestion.interfaces import ArtifactStore
from urbanlens.ingestion.postcodes import CacheEntry
from urbanlens.ingestion.registry import Registry, S3StateBackend, ScopeState
from urbanlens.ingestion.storage import LocalArtifactStore, S3ArtifactStore
from urbanlens.silver.load import load
from urbanlens.silver.models import Observation
from urbanlens.silver.pipeline import Bundle, build, json_line, verified_observations

ROOT = Path(__file__).resolve().parents[1]


class CachedEvidence:
    """Verified local bytes are an optional cache of catalogued S3 evidence."""

    def __init__(self, remote: S3ArtifactStore, catalog_key: str) -> None:
        self.remote = remote
        self.relocated = load_catalog(remote, catalog_key)
        self.local = LocalArtifactStore(ROOT / "data/ingestion/objects")
        self.originals = {
            a.uri: Artifact.model_validate({**a.model_dump(), "uri": uri})
            for uri, a in self.relocated.aliases.items()
        }

    def resolve(self, artifact: Artifact) -> Artifact:
        target = self.relocated.aliases.get(artifact.uri, artifact)
        if not target.uri.startswith("s3://") or (target.sha256, target.byte_count) != (
            artifact.sha256,
            artifact.byte_count,
        ):
            raise ValueError("Evidence is not available as identical portable S3 bytes")
        return target

    def stat(self, key: str) -> Artifact | None:
        return self.remote.stat(key)

    def put_immutable(
        self, key: str, chunks: Iterator[bytes], *, media_type: str, compression: str
    ) -> Artifact:
        return self.remote.put_immutable(
            key, chunks, media_type=media_type, compression=compression
        )

    def read_verified(self, artifact: Artifact) -> Iterator[bytes]:
        target = self.resolve(artifact)
        original = self.originals.get(target.uri)
        if original is not None:
            # Missing local cache is fine; corruption is an integrity failure, not a miss.
            from urllib.parse import unquote, urlsplit
            from urllib.request import url2pathname

            path = Path(url2pathname(unquote(urlsplit(original.uri).path)))
            if path.is_file():
                yield from self.local.read_verified(original)
                return
        yield from self.remote.read_verified(target)


def local_snapshots() -> list[Snapshot]:
    with sqlite3.connect(ROOT / "data/ingestion/registry.sqlite") as connection:
        states = [
            ScopeState.model_validate_json(row[0])
            for row in connection.execute("SELECT body FROM scope_state")
        ]
    if any(s.current is None or s.owner is not None for s in states):
        raise ValueError("Accepted snapshots required; stop collectors before local normalisation")
    return [s.current for s in states if s.current is not None]


def environment(
    storage: str,
) -> tuple[ArtifactStore, ArtifactStore, list[Snapshot], list[CacheEntry], Artifact, Any]:
    if storage == "local":
        source = LocalArtifactStore(ROOT / "data/ingestion/objects")
        output = LocalArtifactStore(ROOT / "data/silver/objects")
        with sqlite3.connect(ROOT / "data/ingestion/postcodes.sqlite") as connection:
            entries = [
                CacheEntry.model_validate_json(row[0])
                for row in connection.execute("SELECT entry FROM postcode_cache ORDER BY postcode")
            ]
        body = json_line(
            {
                "version": "postcode-cache-export-v1",
                "entries": [e.model_dump(mode="json") for e in entries],
            }
        )
        import hashlib

        exported = output.put_immutable(
            f"evidence/postcodes/{hashlib.sha256(body).hexdigest()}.json",
            iter([body]),
            media_type="application/json",
            compression="none",
        )
        return source, output, local_snapshots(), entries, exported, lambda a: a
    state = json.loads((ROOT / ".cache/bronze-state.json").read_text())
    ca = ROOT / ".cache/aws-system-ca.pem"
    if ca.exists():
        os.environ.setdefault("AWS_CA_BUNDLE", str(ca))
    client = sdk_client(
        "s3", profile=None if os.environ.get("URBANLENS_CLOUD_RUNTIME") == "1" else "urbanlens_dev"
    )
    remote = S3ArtifactStore(client, state["outputs"]["Bucket"])
    if os.environ.get("URBANLENS_CLOUD_RUNTIME") != "1":
        active = S3StateBackend(client, state["outputs"]["Bucket"], "operations").load(
            "active.json"
        )
        if active and json.loads(active[0]).get("run_key"):
            raise ValueError(
                "A production workflow is active; wait before running manual pipeline operations"
            )
    cached = CachedEvidence(remote, state["catalog_key"])
    registry = Registry(S3StateBackend(client, state["outputs"]["Bucket"]))
    snapshots = []
    for baseline in state["baseline_snapshots"]:
        original = Snapshot.model_validate(baseline)
        current = registry.current(original.cycle)
        if current is None:
            raise ValueError("Cloud accepted source head is missing")
        snapshots.append(current)
    export = Artifact.model_validate(state["postcode_evidence"])
    body = json.loads(b"".join(remote.read_verified(export)))
    entries = [CacheEntry.model_validate(e) for e in body["entries"]]
    return cached, remote, snapshots, entries, export, cached.resolve


def bundle_for(store: ArtifactStore, storage: str) -> tuple[Bundle, Artifact]:
    manifest = Artifact.model_validate_json((ROOT / f".cache/silver-{storage}.json").read_text())
    return Bundle.model_validate_json(b"".join(store.read_verified(manifest))), manifest


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("task", choices=["schema", "build", "check", "load"])
    parser.add_argument("--storage", choices=["local", "s3"], default="local")
    parser.add_argument("--target", choices=["local", "neon"], default="local")
    args = parser.parse_args()
    if args.task == "schema":
        path = ROOT / "reference/silver/observation.schema.json"
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(
            json.dumps(Observation.model_json_schema(), indent=2) + "\n", encoding="utf-8"
        )
        print("Wrote reference/silver/observation.schema.json")
        return
    source, output, snapshots, entries, export, resolve = environment(args.storage)
    if args.task == "build":
        bundle, manifest = build(
            snapshots,
            source,
            output,
            resolve=resolve,
            entries=entries,
            postcode_manifest=export,
            area_codes=area_codes(),
            log=lambda m: print(m, flush=True),
        )
        path = ROOT / f".cache/silver-{args.storage}.json"
        path.parent.mkdir(exist_ok=True)
        path.write_text(manifest.model_dump_json(indent=2) + "\n")
        print(f"Created {len(bundle.batches)} immutable Silver partitions")
        return
    bundle, manifest = bundle_for(output, args.storage)
    if args.task == "check":
        counts: Counter[str] = Counter()
        states: Counter[str] = Counter()
        seen: set[tuple[str, str, str]] = set()
        for batch in bundle.batches:
            for record in verified_observations(output, batch):
                key = (record.provider.value, record.scope, record.source_key)
                if key in seen:
                    raise ValueError("Duplicate source identity across partitions")
                seen.add(key)
                counts[record.provider.value] += 1
                states["address/" + record.address.state] += 1
                states["location/" + record.location.quality] += 1
        report: dict[str, Any] = {
            "counts": dict(counts),
            "states": dict(states),
            "records_verified": len(seen),
            "all_semantic_hashes_valid": True,
            "manifest": manifest.model_dump(mode="json"),
        }
    else:
        if args.storage != "s3":
            raise ValueError("Database loads require --storage s3 for portable provenance")
        with connect(credential(args.target, "pipeline")) as connection:
            from scripts.history import failed_attempts, query_transitions

            snapshots = list(
                {
                    b.snapshot.snapshot_id: b.snapshot
                    for b in bundle.batches
                    if b.snapshot is not None
                }.values()
            )
            report = load(
                connection,
                output,
                bundle,
                interruptions=failed_attempts(output, snapshots),
                compatible_queries=query_transitions(),
            )
        report["target"] = args.target
    path = ROOT / f"data/silver/{args.storage}-{args.task}-{args.target}.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps({k: v for k, v in report.items() if k != "manifest"}, indent=2))


if __name__ == "__main__":
    try:
        main()
    except (ValueError, psycopg.Error) as error:
        path = ROOT / ".cache/silver-error.txt"
        path.parent.mkdir(exist_ok=True)
        path.write_text(str(error))
        raise SystemExit(
            f"Silver operation failed ({type(error).__name__}); "
            "inspect private .cache/silver-error.txt"
        ) from None

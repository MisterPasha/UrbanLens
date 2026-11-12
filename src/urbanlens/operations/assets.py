"""Checksum-verified, nonsecret runtime inputs. Editable review files are never deployed."""

from __future__ import annotations

import json
import os
import shutil
from pathlib import Path
from typing import Any

from urbanlens.ingestion.cloud import collect, sdk_client
from urbanlens.ingestion.contracts import Artifact
from urbanlens.ingestion.storage import S3ArtifactStore

ASSETS = frozenset(
    {
        "data/reference/geography/city-coverage-wgs84.geojson",
        "data/reference/geography/boundary-item.json",
        "data/reference/geography/lookup-item.json",
        "data/reference/geography/sam-item.json",
    }
)


def configuration() -> tuple[S3ArtifactStore, dict[str, Any]]:
    store = S3ArtifactStore(sdk_client("s3"), os.environ["BRONZE_BUCKET"])
    key = os.environ["RUNTIME_CONFIG_KEY"]
    if not key.startswith("deployment/config/") or not key.endswith(".json"):
        raise ValueError("Runtime config must use an immutable deployment key")
    artifact = store.stat(key)
    if artifact is None or artifact.byte_count > 2_000_000:
        raise ValueError("Missing or oversized runtime configuration")
    body = json.loads(b"".join(store.read_verified(artifact)))
    if (
        body["version"] != "production-runtime-v1"
        or body["bronze"]["outputs"]["Bucket"] != store.bucket
    ):
        raise ValueError("Runtime configuration identity mismatch")
    return store, body


def hydrate(root: Path, store: S3ArtifactStore, config: dict[str, Any]) -> None:
    if set(config["assets"]) != ASSETS:
        raise ValueError("Unexpected runtime asset paths")
    for name, definition in config["assets"].items():
        artifact = Artifact.model_validate(definition)
        if artifact.byte_count > 50_000_000:
            raise ValueError("Runtime asset exceeds its size budget")
        path = root / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(b"".join(store.read_verified(artifact)))


def collector_handler(event: dict[str, Any], context: Any) -> dict[str, Any]:
    provider = event.get("provider")
    if provider not in {"fsa", "osm", "companies-house-profiles"}:
        raise ValueError("Unsupported provider")
    store, config = configuration()
    root = Path("/tmp/urbanlens-assets")
    project = Path("/var/task")
    for name in ("config", "reference"):
        shutil.copytree(project / name, root / name, dirs_exist_ok=True)
    hydrate(root, store, config)
    snapshot = collect(provider, store.bucket, config["bronze"]["catalog_key"], root=root)
    return {"status": snapshot.run.status.value, "snapshot_id": str(snapshot.snapshot_id)}

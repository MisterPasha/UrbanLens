"""Copy retained evidence without rewriting its dates, URIs or review digests."""

from __future__ import annotations

import json
from collections.abc import Iterator
from typing import Any

from urbanlens.ingestion.contracts import Artifact
from urbanlens.ingestion.interfaces import ArtifactStore
from urbanlens.ingestion.storage import LocalArtifactStore, S3ArtifactStore, safe_key


class RelocatedStore:
    """Resolve original local artifact references through a verified private S3 catalog."""

    def __init__(self, store: ArtifactStore, entries: list[dict[str, Any]]) -> None:
        self.store = store
        self.aliases: dict[str, Artifact] = {}
        for entry in entries:
            original = Artifact.model_validate(entry["original"])
            stored = Artifact.model_validate(entry["stored"])
            if original.sha256 != stored.sha256 or original.byte_count != stored.byte_count:
                raise ValueError("Relocation changed retained bytes")
            if original.uri in self.aliases and self.aliases[original.uri] != stored:
                raise ValueError("Ambiguous artifact relocation")
            self.aliases[original.uri] = stored

    def stat(self, key: str) -> Artifact | None:
        return self.store.stat(key)

    def put_immutable(
        self, key: str, chunks: Iterator[bytes], *, media_type: str, compression: str
    ) -> Artifact:
        return self.store.put_immutable(key, chunks, media_type=media_type, compression=compression)

    def read_verified(self, artifact: Artifact) -> Iterator[bytes]:
        target = self.aliases.get(artifact.uri, artifact)
        if target.sha256 != artifact.sha256 or target.byte_count != artifact.byte_count:
            raise ValueError("Artifact disagrees with relocation catalog")
        yield from self.store.read_verified(target)


def archive_objects(local: LocalArtifactStore, remote: S3ArtifactStore) -> dict[str, Any]:
    """Allowlisted object tree only: never upload .env, SQLite credentials or repo files.

    Each source is checked before upload and each destination is downloaded and checked.
    Conditional creates make interrupted uploads resumable without replacing evidence.
    """
    entries: list[dict[str, Any]] = []
    for directory in ("bronze", "silver", "manifests", "evidence"):
        for path in sorted((local.root / directory).rglob("*")):
            if not path.is_file():
                continue
            if path.is_symlink():
                raise ValueError("Do not archive symlinks")
            key = safe_key(path.relative_to(local.root).as_posix())
            original = local.stat(key)
            assert original is not None
            stored = remote.stat(key)
            if stored is None:
                stored = remote.put_immutable(
                    key,
                    local.read_verified(original),
                    media_type=original.media_type,
                    compression=original.compression,
                )
            if stored.sha256 != original.sha256 or stored.byte_count != original.byte_count:
                raise ValueError("Existing S3 object conflicts with bootstrap evidence")
            for _ in remote.read_verified(stored):
                pass
            entries.append(
                {"key": key, "original": original.model_dump(), "stored": stored.model_dump()}
            )
    return {"version": "bronze-relocation-v1", "objects": entries}


def load_catalog(store: S3ArtifactStore, key: str) -> RelocatedStore:
    if not safe_key(key).startswith("evidence/bootstrap/"):
        raise ValueError("Bootstrap catalog must be retained evidence")
    artifact = store.stat(key)
    if artifact is None or artifact.byte_count > 2_000_000:
        raise ValueError("Missing or oversized bootstrap catalog")
    catalog = json.loads(b"".join(store.read_verified(artifact)))
    if catalog["version"] != "bronze-relocation-v1":
        raise ValueError("Unsupported bootstrap catalog")
    return RelocatedStore(store, catalog["objects"])

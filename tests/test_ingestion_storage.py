"""Atomic writes, retained-byte verification and local/shared lease fencing."""

import gzip
import hashlib
import io
import os
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any
from uuid import uuid4

import pytest

from urbanlens.ingestion.contracts import CollectionCycle, Provider, Run
from urbanlens.ingestion.errors import Conflict, IngestionError, LeaseLost
from urbanlens.ingestion.registry import Registry, S3StateBackend, SQLiteStateBackend
from urbanlens.ingestion.storage import (
    LocalArtifactStore,
    S3ArtifactStore,
    gzip_chunks,
    resolved_path,
)


class S3Error(Exception):
    def __init__(self, code: str) -> None:
        self.response = {"Error": {"Code": code}}


class MemoryS3:
    """S3's documented conditional-write behavior; no credentials or live requests."""

    def __init__(self) -> None:
        self.objects: dict[str, dict[str, Any]] = {}

    def put_object(self, **kwargs: Any) -> dict[str, Any]:
        key = str(kwargs["Key"])
        previous = self.objects.get(key)
        if kwargs.get("IfNoneMatch") == "*" and previous:
            raise S3Error("PreconditionFailed")
        if "IfMatch" in kwargs and (not previous or previous["ETag"] != kwargs["IfMatch"]):
            raise S3Error("PreconditionFailed")
        body = kwargs["Body"]
        data = body if isinstance(body, bytes) else body.read()
        etag = '"' + hashlib.md5(data, usedforsecurity=False).hexdigest() + '"'
        self.objects[key] = {**kwargs, "data": data, "ETag": etag}
        return {"ETag": etag}

    def get_object(self, **kwargs: Any) -> dict[str, Any]:
        if kwargs["Key"] not in self.objects:
            raise S3Error("NoSuchKey")
        obj = self.objects[kwargs["Key"]]
        return {"Body": io.BytesIO(obj["data"]), "ETag": obj["ETag"]}

    def head_object(self, **kwargs: Any) -> dict[str, Any]:
        if kwargs["Key"] not in self.objects:
            raise S3Error("404")
        obj = self.objects[kwargs["Key"]]
        return {
            "ContentLength": len(obj["data"]),
            "Metadata": obj.get("Metadata", {}),
            "ContentType": obj.get("ContentType"),
        }


@pytest.mark.parametrize("remote", [False, True])
def test_immutable_stores_are_idempotent_and_detect_conflicts(tmp_path: Path, remote: bool) -> None:
    store = (
        S3ArtifactStore(MemoryS3(), "test-bucket", "pilot")
        if remote
        else LocalArtifactStore(tmp_path)
    )
    first = store.put_immutable(
        "bronze/source.gz",
        gzip_chunks(iter([b'[{"id":"1"}]'])),
        media_type="application/json",
        compression="gzip",
    )
    repeated = store.put_immutable(
        "bronze/source.gz",
        gzip_chunks(iter([b'[{"id":"1"}]'])),
        media_type="application/json",
        compression="gzip",
    )
    assert first == repeated
    assert gzip.decompress(b"".join(store.read_verified(first))) == b'[{"id":"1"}]'
    with pytest.raises(Conflict):
        store.put_immutable(
            "bronze/source.gz",
            iter([b"different"]),
            media_type="application/json",
            compression="none",
        )


@pytest.mark.parametrize("remote", [False, True])
def test_interrupted_write_never_leaves_a_finalized_object(tmp_path: Path, remote: bool) -> None:
    store = S3ArtifactStore(MemoryS3(), "test-bucket") if remote else LocalArtifactStore(tmp_path)

    def interrupted() -> Any:
        yield b"partial"
        raise KeyboardInterrupt()

    with pytest.raises(KeyboardInterrupt):
        store.put_immutable(
            "bronze/data", interrupted(), media_type="application/json", compression="none"
        )
    assert store.stat("bronze/data") is None
    assert not list(tmp_path.rglob(".pending-*"))


def test_local_atomic_create_has_one_winner_for_different_bytes(tmp_path: Path) -> None:
    store = LocalArtifactStore(tmp_path)

    def write(data: bytes) -> str:
        try:
            return store.put_immutable(
                "bronze/file", iter([data]), media_type="text/plain", compression="none"
            ).sha256
        except Conflict:
            return "conflict"

    with ThreadPoolExecutor(max_workers=2) as executor:
        results = list(executor.map(write, [b"one", b"two"]))
    assert results.count("conflict") == 1


def test_corruption_and_path_escape_are_rejected(tmp_path: Path) -> None:
    store = LocalArtifactStore(tmp_path)
    artifact = store.put_immutable(
        "raw/file", iter([b"right"]), media_type="text/plain", compression="none"
    )
    store.path("raw/file").write_bytes(b"wrong")
    with pytest.raises(IngestionError):
        list(store.read_verified(artifact))
    for key in ["../outside", "C:/outside", "/absolute", "a/../b", "a\\b", "a//b"]:
        with pytest.raises(ValueError):
            store.path(key)


def test_artifact_byte_budget_applies_before_commit(tmp_path: Path) -> None:
    store = LocalArtifactStore(tmp_path, max_bytes=2)
    with pytest.raises(IngestionError, match="artifact-byte-budget"):
        store.put_immutable("raw/file", iter([b"123"]), media_type="text/plain", compression="none")
    assert store.stat("raw/file") is None


@pytest.mark.skipif(os.name != "nt", reason="Windows extended-path aliases")
def test_windows_extended_path_is_the_same_storage_root(tmp_path: Path) -> None:
    assert resolved_path(Path("\\\\?\\" + str(tmp_path))) == resolved_path(tmp_path)


@pytest.mark.parametrize("remote", [False, True])
def test_expired_owner_cannot_update_after_another_worker_claims_scope(
    tmp_path: Path, remote: bool
) -> None:
    backend = (
        S3StateBackend(MemoryS3(), "test-bucket")
        if remote
        else SQLiteStateBackend(tmp_path / "registry.sqlite")
    )
    clock = [datetime.now(UTC)]
    registry = Registry(backend, lease_seconds=5, now=lambda: clock[0])
    cycle = CollectionCycle(
        cycle_id=uuid4(),
        provider=Provider.FSA,
        scope="test",
        query_version="v1",
        query_sha256="a" * 64,
        window_start=clock[0],
        window_end=clock[0] + timedelta(days=1),
    )
    run = Run(run_id=uuid4(), cycle_id=cycle.cycle_id, pipeline_version="v1", started_at=clock[0])
    first = registry.acquire(cycle, run)
    with pytest.raises(IngestionError, match="scope-already-running"):
        registry.acquire(cycle, run)
    clock[0] += timedelta(seconds=6)
    replacement = Run(
        run_id=uuid4(), cycle_id=cycle.cycle_id, pipeline_version="v1", started_at=clock[0]
    )
    second = registry.acquire(cycle, replacement)
    assert second.run.attempt == 2 and second.run.retry_of_run_id == first.run.run_id
    with pytest.raises(LeaseLost):
        registry.update(first, first.run)
    with pytest.raises(LeaseLost):
        registry.release(first)
    state, _ = registry.owned(second)
    assert state.pending_history[0].failure is not None
    assert state.pending_history[0].failure.code == "worker-interrupted"
    registry.release(second)


def test_sqlite_registry_survives_reopening_and_compare_set_race(tmp_path: Path) -> None:
    path = tmp_path / "registry.sqlite"
    first, second = SQLiteStateBackend(path), SQLiteStateBackend(path)
    revision = first.compare_set("scope", None, b"initial")
    assert second.load("scope") == (b"initial", revision)
    first.compare_set("scope", revision, b"new")
    with pytest.raises(LeaseLost):
        second.compare_set("scope", revision, b"stale")


def test_s3_store_cannot_read_another_bucket_or_prefix() -> None:
    from urbanlens.ingestion.contracts import Artifact

    store = S3ArtifactStore(MemoryS3(), "test-bucket", "allowed")
    for uri in [
        "s3://other/allowed/a",
        "s3://test-bucket/other/a",
        "s3://test-bucket/allowed/../secret",
    ]:
        artifact = Artifact(uri=uri, sha256="a" * 64, byte_count=1, media_type="text/plain")
        with pytest.raises(ValueError):
            list(store.read_verified(artifact))


def test_s3_artifacts_and_shared_registry_coordinate_a_complete_collection() -> None:
    from collections.abc import Iterator, Mapping

    from urbanlens.ingestion.contracts import Partition
    from urbanlens.ingestion.engine import Collector
    from urbanlens.ingestion.interfaces import ArtifactStore, Layer, artifact_key
    from urbanlens.ingestion.validation import ValidationRules

    client = MemoryS3()
    store = S3ArtifactStore(client, "test-bucket", "objects")
    registry = Registry(S3StateBackend(client, "test-bucket", "control"))
    now = datetime.now(UTC)
    cycle = CollectionCycle(
        cycle_id=uuid4(),
        provider=Provider.FSA,
        scope="test-city",
        query_version="test-v1",
        query_sha256="a" * 64,
        window_start=now,
        window_end=now + timedelta(days=1),
    )

    class EmptyAdapter:
        provider = Provider.FSA
        calls = 0

        def fetch(
            self, cycle: CollectionCycle, run: Run, partition_id: str, store: ArtifactStore
        ) -> Partition:
            self.calls += 1
            artifact = store.put_immutable(
                artifact_key(Layer.BRONZE, cycle, run.run_id, "source.json.gz", partition_id),
                gzip_chunks(iter([b"[]"])),
                media_type="application/json",
                compression="gzip",
            )
            return Partition(
                partition_id=partition_id,
                cycle_id=cycle.cycle_id,
                run_id=run.run_id,
                schema_version="test-v1",
                pipeline_version=run.pipeline_version,
                status="fetched",
                fetched_at=datetime.now(UTC),
                artifacts=(artifact,),
            )

        def parse(
            self, partition: Partition, store: ArtifactStore
        ) -> Iterator[Mapping[str, object]]:
            assert gzip.decompress(b"".join(store.read_verified(partition.artifacts[0]))) == b"[]"
            return iter(())

    adapter = EmptyAdapter()
    rules = ValidationRules(version="test-v1", identity_field="id", required_fields=("id",))
    original = Collector(store, registry).collect(cycle, adapter, rules, pipeline_version="test-v1")
    reopened = Registry(S3StateBackend(client, "test-bucket", "control"))
    repeated = Collector(store, reopened).collect(cycle, adapter, rules, pipeline_version="test-v1")
    assert original.successful_empty and original == repeated and adapter.calls == 1
    assert reopened.current(cycle) == original


def test_deep_paths_keep_normal_file_uris_and_verified_io(tmp_path: Path) -> None:
    store = LocalArtifactStore(tmp_path / "objects")
    key = "/".join(["deep-" + "a" * 45] * 6) + "/source.json"
    artifact = store.put_immutable(
        key, iter([b"{}"]), media_type="application/json", compression="none"
    )
    assert "%3F" not in artifact.uri and b"".join(store.read_verified(artifact)) == b"{}"
    found = store.stat(key)
    assert found is not None and found.sha256 == artifact.sha256

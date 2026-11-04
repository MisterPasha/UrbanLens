"""Private archive relocation, distributed request budgets and recovery acceptance."""

import json
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from uuid import uuid4

import pytest
from test_ingestion_storage import MemoryS3

from urbanlens.ingestion.acceptance import AcceptanceService
from urbanlens.ingestion.archive import RelocatedStore, archive_objects, load_catalog
from urbanlens.ingestion.cloud import CloudProfileBudget, handler
from urbanlens.ingestion.companies import ProfileRequestBudget
from urbanlens.ingestion.contracts import Artifact, CollectionCycle, Provider, Snapshot
from urbanlens.ingestion.engine import Collector
from urbanlens.ingestion.errors import IngestionError
from urbanlens.ingestion.postcodes import PostcodeService
from urbanlens.ingestion.registry import Registry, S3StateBackend, ScopeState, SQLiteStateBackend
from urbanlens.ingestion.storage import LocalArtifactStore, S3ArtifactStore
from urbanlens.ingestion.validation import ValidationRules
from urbanlens.ingestion.verification import RecoveryFixture, recovery_drill, replay_snapshot


def test_archive_retains_original_manifest_and_resolves_old_uri(tmp_path: Path) -> None:
    local = LocalArtifactStore(tmp_path / "objects")
    original = local.put_immutable(
        "bronze/test/raw.json",
        iter([b'{"a":1}']),
        media_type="application/json",
        compression="none",
    )
    content = json.dumps(
        {"fetched_at": "2026-11-01T10:00:00Z", "artifact": original.model_dump()}
    ).encode()
    local.put_immutable(
        "manifests/test/source.json",
        iter([content]),
        media_type="application/json",
        compression="none",
    )
    (local.root / ".env").write_text("PRIVATE_KEY=must-not-upload")
    remote = S3ArtifactStore(MemoryS3(), "private-test")
    catalog = archive_objects(local, remote)
    assert len(catalog["objects"]) == 2
    assert all(e["key"] != ".env" for e in catalog["objects"])
    store = RelocatedStore(remote, catalog["objects"])
    assert b"".join(store.read_verified(original)) == b'{"a":1}'
    manifest = store.stat("manifests/test/source.json")
    assert manifest is not None
    assert b"".join(store.read_verified(manifest)) == content
    assert archive_objects(local, remote) == catalog
    altered = Artifact.model_validate({**original.model_dump(), "sha256": "0" * 64})
    with pytest.raises(ValueError, match="disagrees"):
        list(store.read_verified(altered))


def test_partial_archive_resumes_without_replacing_other_bytes(tmp_path: Path) -> None:
    local = LocalArtifactStore(tmp_path)
    for i in range(3):
        local.put_immutable(
            f"bronze/{i}.json",
            iter([str(i).encode()]),
            media_type="application/json",
            compression="none",
        )
    client = MemoryS3()
    remote = S3ArtifactStore(client, "private-test")
    first = local.stat("bronze/0.json")
    assert first is not None
    remote.put_immutable(
        "bronze/0.json",
        local.read_verified(first),
        media_type="application/json",
        compression="none",
    )
    assert len(archive_objects(local, remote)["objects"]) == 3
    client.objects["bronze/1.json"]["data"] = b"corrupt"
    with pytest.raises((IngestionError, ValueError)):
        archive_objects(local, remote)


def test_catalog_rejects_changed_bytes_and_unsafe_location() -> None:
    remote = S3ArtifactStore(MemoryS3(), "private-test")
    with pytest.raises(ValueError, match="Unsafe relative storage key"):
        load_catalog(remote, "bronze/../key")
    source = {
        "uri": "file:///original/source.json",
        "sha256": "0" * 64,
        "byte_count": 2,
        "media_type": "application/json",
    }
    stored = {**source, "uri": "s3://private-test/bronze/source.json", "byte_count": 3}
    with pytest.raises(ValueError, match="changed retained bytes"):
        RelocatedStore(remote, [{"original": source, "stored": stored}])


def test_shared_profile_budget_is_persistent_and_fails_closed() -> None:
    backend = S3StateBackend(MemoryS3(), "private-test", "budgets")

    def attempt(_: int) -> bool:
        try:
            CloudProfileBudget(backend, 5, "2026-11").claim()
            return True
        except IngestionError:
            return False

    with ThreadPoolExecutor(max_workers=4) as workers:
        assert sum(workers.map(attempt, range(20))) == 5
    with pytest.raises(IngestionError, match="monthly-request-budget"):
        CloudProfileBudget(backend, 5, "2026-11").claim()
    CloudProfileBudget(backend, 5, "2026-12").claim()


def test_recovery_uses_shared_s3_state_and_preserves_last_accepted() -> None:
    client = MemoryS3()
    store = S3ArtifactStore(client, "private-test")
    registry = Registry(S3StateBackend(client, "private-test"))
    report = recovery_drill(store, registry)
    assert report["synthetic"] is True
    assert report["results"][0]["resumed_fetches"] == ["part-02"]
    assert report["results"][1]["resumed_fetches"] == ["part-01", "part-02"]
    failures = []
    for key in client.objects:
        if key.endswith("snapshot.json"):
            snapshot = Snapshot.model_validate_json(client.objects[key]["data"])
            if snapshot.run.failure:
                failures.append(snapshot)
    assert len(failures) == 2


def test_cloud_entry_rejects_arbitrary_provider_before_aws_access() -> None:
    with pytest.raises(ValueError, match="Unsupported cloud provider"):
        handler({"provider": "arbitrary-url", "url": "https://example.org"}, None)
    with pytest.raises(ValueError, match="Unsupported cloud operation"):
        handler({"provider": "fsa", "operation": "delete"}, None)


@pytest.mark.parametrize("variant", ["same", "gzip-header", "changed-records"])
def test_source_replay_reproduces_hashes_without_advancing_current(
    monkeypatch: pytest.MonkeyPatch, variant: str
) -> None:
    from urbanlens.ingestion import engine
    from urbanlens.ingestion.storage import gzip_chunks

    if variant == "gzip-header":

        def other_platform_gzip(data: Any) -> Any:
            encoded = bytearray(b"".join(gzip_chunks(data)))
            encoded[9] = (encoded[9] + 1) % 256  # Legal gzip OS header; decoded bytes identical.
            yield bytes(encoded)

        monkeypatch.setattr(engine, "gzip_chunks", other_platform_gzip)
    client = MemoryS3()
    store = S3ArtifactStore(client, "private-test")
    registry = Registry(S3StateBackend(client, "private-test"))
    cycle = CollectionCycle(
        cycle_id=uuid4(),
        provider=Provider.FSA,
        scope="synthetic-replay",
        query_version="synthetic-recovery-v1",
        query_sha256="a" * 64,
        window_start=datetime(2026, 11, 1, tzinfo=UTC),
        window_end=datetime(2026, 12, 1, tzinfo=UTC),
    )
    rules = ValidationRules(
        version="synthetic-recovery-v1", identity_field="id", required_fields=("synthetic",)
    )
    adapter = RecoveryFixture()
    snapshot = Collector(store, registry).collect(
        cycle, adapter, rules, pipeline_version="synthetic-recovery-v1"
    )
    if variant == "changed-records":

        def changed_records(*args: Any) -> Any:
            yield {"id": "full", "synthetic": False}

        monkeypatch.setattr(adapter, "parse", changed_records)
        with pytest.raises(ValueError, match="did not reproduce"):
            replay_snapshot(snapshot, adapter, rules, store)
        assert registry.current(cycle) == snapshot
        return
    report = replay_snapshot(snapshot, adapter, rules, store)
    assert report["normalized_hashes_match"]
    assert report["new_observations"] == 0
    assert report["normalized_hash_basis"] == "uncompressed-utf8-jsonl"
    assert report["stored_compressed_hashes_match"] is (variant != "gzip-header")
    assert registry.current(cycle) == snapshot
    assert report["run"]["mode"] == "replay"
    assert report["source_fetched_at"] == [
        p.fetched_at.isoformat() for p in snapshot.partitions if p.fetched_at
    ]


def test_cloud_same_month_uses_s3_accepted_cycle_without_provider_requests(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from urbanlens.ingestion import cloud
    from urbanlens.ingestion.fsa import FsaConfig
    from urbanlens.ingestion.http import HttpDownloader

    client = MemoryS3()
    store = S3ArtifactStore(client, "private-test")
    registry = Registry(S3StateBackend(client, "private-test"))
    config = FsaConfig(min_records=1)
    cycle = config.cycle(datetime.now(UTC).date())
    rules = ValidationRules(
        version="synthetic-recovery-v1", identity_field="id", required_fields=("synthetic",)
    )
    snapshot = Collector(store, registry).collect(
        cycle, RecoveryFixture(), rules, pipeline_version="fsa-source-v1"
    )
    (tmp_path / "config").mkdir()
    (tmp_path / "config/fsa-source.json").write_text(config.model_dump_json())
    (tmp_path / "config/source-acceptance.json").write_text(
        json.dumps(
            {"version": "source-acceptance-v1", "change_records": 20, "change_fraction": 0.2}
        )
    )
    monkeypatch.setattr(cloud, "ROOT", tmp_path)
    monkeypatch.setattr(cloud, "sdk_client", lambda *a, **kw: client)
    monkeypatch.setattr(cloud, "load_catalog", lambda *a, **kw: store)

    def forbidden(*args: Any, **kwargs: Any) -> None:
        raise AssertionError("Accepted monthly rerun must not access a provider")

    monkeypatch.setattr(HttpDownloader, "download", forbidden)
    assert cloud.collect("fsa", "private-test", "unused-test-catalog") == snapshot
    assert registry.current(cycle) == snapshot


def test_schedules_require_matching_live_evidence(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from scripts import bronze

    monkeypatch.setattr(bronze, "DIRECTORY", tmp_path)
    monkeypatch.setattr(bronze, "STATE", tmp_path / "state.json")
    bronze.write(
        bronze.STATE, {"catalog_key": "evidence/bootstrap/test.json", "package_sha256": "a" * 64}
    )
    with pytest.raises(ValueError, match="verification, replay and recovery"):
        bronze.enable({}, {"Bucket": "private-test"})


@pytest.mark.parametrize("corrupt", [False, True])
def test_bootstrap_seeds_only_hash_verified_reviewed_source_evidence(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, corrupt: bool
) -> None:
    from scripts import bronze

    monkeypatch.setattr(bronze, "DIRECTORY", tmp_path)
    monkeypatch.setattr(bronze, "STATE", tmp_path / "state.json")
    local = LocalArtifactStore(tmp_path / "objects")
    registry = Registry(SQLiteStateBackend(tmp_path / "registry.sqlite"))
    cycle = CollectionCycle(
        cycle_id=uuid4(),
        provider=Provider.FSA,
        scope="synthetic-bootstrap",
        query_version="synthetic-recovery-v1",
        query_sha256="a" * 64,
        window_start=datetime(2026, 11, 1, tzinfo=UTC),
        window_end=datetime(2026, 12, 1, tzinfo=UTC),
    )
    rules = ValidationRules(
        version="synthetic-recovery-v1", identity_field="id", required_fields=("synthetic",)
    )
    snapshot = Collector(local, registry).collect(
        cycle, RecoveryFixture(), rules, pipeline_version="synthetic-recovery-v1"
    )
    AcceptanceService(local).record(
        cycle,
        snapshot.partitions,
        reviewer="synthetic inspector",
        reviewed_at=datetime.now(UTC),
        decision="approve",
        reason="Fixture validation",
    )
    monkeypatch.setattr(bronze, "source_states", lambda: [ScopeState(current=snapshot)])
    PostcodeService(tmp_path / "postcodes.sqlite", local, set())
    ProfileRequestBudget(tmp_path / "profile-requests.sqlite", 100)
    for name in ("postcode-check.json", "reference-edition-check.json", "source-inspection.json"):
        (tmp_path / name).write_text("{}")
    client = MemoryS3()
    if corrupt:
        raw_path = next((local.root / "bronze").rglob("synthetic.json"))
        raw_path.write_bytes(b"tampered")
        with pytest.raises(IngestionError):
            bronze.bootstrap({"s3": client}, {"Bucket": "private-test"})
        assert not client.objects
    else:
        bronze.bootstrap({"s3": client}, {"Bucket": "private-test"})
        remote_registry = Registry(S3StateBackend(client, "private-test"))
        assert remote_registry.current(cycle) == snapshot
        bronze.bootstrap({"s3": client}, {"Bucket": "private-test"})
        assert remote_registry.current(cycle) == snapshot


def test_stack_is_private_with_monthly_disabled_schedules() -> None:
    import yaml  # type: ignore[import-untyped]

    root = Path(__file__).resolve().parents[1]
    # BaseLoader accepts CloudFormation intrinsic tags without executing constructors.
    template: dict[str, Any] = yaml.load(
        (root / "infra/bronze/template.yaml").read_text(), Loader=yaml.BaseLoader
    )
    resources = template["Resources"]
    bucket = resources["Artifacts"]
    assert bucket["DeletionPolicy"] == "Retain"
    props = bucket["Properties"]
    assert set(props["PublicAccessBlockConfiguration"].values()) == {"true"}
    assert props["LifecycleConfiguration"]["Rules"][0]["Prefix"] == "bronze/"
    assert props["LifecycleConfiguration"]["Rules"][0]["ExpirationInDays"] == "90"
    rules = [v["Properties"] for v in resources.values() if v["Type"] == "AWS::Events::Rule"]
    assert len(rules) == 3
    assert all(r["State"] == "DISABLED" and " 7 * ? *)" in r["ScheduleExpression"] for r in rules)
    assert resources["Collector"]["Properties"]["Timeout"] == "300"

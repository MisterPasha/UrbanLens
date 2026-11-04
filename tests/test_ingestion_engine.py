"""Acceptance and durable recovery with retained synthetic artifacts, never live data."""

import gzip
import hashlib
import json
from collections import Counter
from collections.abc import Iterator, Mapping
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from urbanlens.ingestion.commands import fetch_raw, replay_raw
from urbanlens.ingestion.contracts import (
    CollectionCycle,
    Counts,
    FailureClass,
    Partition,
    Provider,
    Run,
    Stage,
)
from urbanlens.ingestion.engine import Collector, metadata_key
from urbanlens.ingestion.errors import IngestionError
from urbanlens.ingestion.http import HttpDownloader, HttpPolicy
from urbanlens.ingestion.interfaces import ArtifactStore, Layer, artifact_key
from urbanlens.ingestion.registry import Registry, SQLiteStateBackend, safe_log
from urbanlens.ingestion.storage import LocalArtifactStore, gzip_chunks
from urbanlens.ingestion.validation import ValidationRules


def cycle(day: int = 4, *, parts: tuple[str, ...] = ("full",)) -> CollectionCycle:
    from uuid import NAMESPACE_URL, uuid5

    start = datetime(2026, 11, day, tzinfo=UTC)
    return CollectionCycle(
        cycle_id=uuid5(NAMESPACE_URL, f"synthetic-{day}-{parts}"),
        provider=Provider.FSA,
        scope="test-city",
        query_version="test-v1",
        query_sha256="a" * 64,
        window_start=start,
        window_end=start + timedelta(days=1),
        expected_partitions=parts,
    )


class RecordedAdapter:
    provider = Provider.FSA

    def __init__(self, data: dict[str, bytes]) -> None:
        self.data = data
        self.calls: Counter[str] = Counter()
        self.fail_part: str | None = None
        self.interrupt = False
        self.expected_count: int | None = None

    def fetch(
        self, cycle: CollectionCycle, run: Run, partition_id: str, store: ArtifactStore
    ) -> Partition:
        self.calls[partition_id] += 1
        if self.fail_part == partition_id:
            self.fail_part = None
            raise IngestionError(FailureClass.TIMEOUT, "fixture-timeout", retryable=True)
        if self.interrupt:
            self.interrupt = False
            raise KeyboardInterrupt()
        artifact = store.put_immutable(
            artifact_key(Layer.BRONZE, cycle, run.run_id, "data.json.gz", partition_id),
            gzip_chunks(iter([self.data[partition_id]])),
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
            source_edition=cycle.source_edition,
            artifacts=(artifact,),
            counts=Counts(raw=self.expected_count),
        )

    def parse(self, partition: Partition, store: ArtifactStore) -> Iterator[Mapping[str, object]]:
        payload = b"".join(store.read_verified(partition.artifacts[0]))
        rows = json.loads(gzip.decompress(payload))
        if not isinstance(rows, list):
            raise ValueError("Fixture requires a list")
        yield from rows


def setup(tmp_path: Path) -> tuple[Collector, LocalArtifactStore, Registry, ValidationRules]:
    store = LocalArtifactStore(tmp_path / "objects")
    registry = Registry(SQLiteStateBackend(tmp_path / "registry.sqlite"))
    rules = ValidationRules(
        version="test-v1",
        identity_field="id",
        required_fields=("id", "city"),
        scope=lambda row: row.get("city") == "test-city",
    )
    return Collector(store, registry), store, registry, rules


def test_complete_collection_is_idempotent_and_current_survives_reopening(tmp_path: Path) -> None:
    collector, store, registry, rules = setup(tmp_path)
    adapter = RecordedAdapter({"full": b'[{"id":"001","city":"test-city"}]'})
    first = collector.collect(cycle(), adapter, rules, pipeline_version="test-v1")
    repeated = collector.collect(cycle(), adapter, rules, pipeline_version="test-v1")
    assert first == repeated and adapter.calls["full"] == 1
    assert first.run.status == Stage.ACCEPTED
    reopened = Registry(SQLiteStateBackend(tmp_path / "registry.sqlite"))
    assert reopened.current(cycle()) == first
    assert store.stat(metadata_key(cycle(), "accepted")) is not None


@pytest.mark.parametrize("data", [b"[]", b'[{"id":"001","city":"test-city"}]'])
def test_empty_is_success_only_after_complete_validation(tmp_path: Path, data: bytes) -> None:
    collector, _, registry, rules = setup(tmp_path)
    result = collector.collect(
        cycle(), RecordedAdapter({"full": data}), rules, pipeline_version="test-v1"
    )
    assert result.successful_empty == (data == b"[]")
    assert registry.current(cycle()) == result


def test_failed_partition_resumes_without_refetching_finished_parts(tmp_path: Path) -> None:
    collector, store, registry, rules = setup(tmp_path)
    planned = cycle(parts=("a", "b"))
    adapter = RecordedAdapter(
        {"a": b'[{"id":"a","city":"test-city"}]', "b": b'[{"id":"b","city":"test-city"}]'}
    )
    adapter.fail_part = "b"
    with pytest.raises(IngestionError, match="fixture-timeout"):
        collector.collect(planned, adapter, rules, pipeline_version="test-v1")
    assert registry.current(planned) is None
    old = registry.state(registry.key(planned))[0].active_run
    assert old is not None and old.status == Stage.FAILED
    assert store.stat(f"manifests/runs/{old.run_id}.json") is not None
    result = collector.collect(planned, adapter, rules, pipeline_version="test-v1")
    assert adapter.calls == {"a": 1, "b": 2}
    assert result.run.attempt == 2 and result.run.retry_of_run_id == old.run_id
    assert len(result.partitions) == 2 and result.run.status == Stage.ACCEPTED


@pytest.mark.parametrize(
    "bad",
    [
        b'[{"id":"001","city":"outside"}]',
        b'[{"city":"test-city"}]',
        b'[{"id":"1","city":"test-city"},{"id":"1","city":"test-city"}]',
    ],
)
def test_scope_schema_and_duplicate_rejection_preserve_last_current(
    tmp_path: Path, bad: bytes
) -> None:
    collector, store, registry, rules = setup(tmp_path)
    original = collector.collect(
        cycle(),
        RecordedAdapter({"full": b'[{"id":"1","city":"test-city"}]'}),
        rules,
        pipeline_version="test-v1",
    )
    rejected = collector.collect(
        cycle(5), RecordedAdapter({"full": bad}), rules, pipeline_version="test-v1"
    )
    assert rejected.run.status == Stage.REJECTED
    assert registry.current(cycle(5)) == original
    assert rejected.partitions[0].artifacts
    assert store.stat(metadata_key(cycle(5), "accepted")) is None


def test_count_drop_and_incomplete_parse_cannot_replace_current(tmp_path: Path) -> None:
    collector, _, registry, rules = setup(tmp_path)
    original = collector.collect(
        cycle(),
        RecordedAdapter({"full": b'[{"id":"1","city":"test-city"}]'}),
        rules,
        pipeline_version="test-v1",
    )
    drop = collector.collect(
        cycle(5), RecordedAdapter({"full": b"[]"}), rules, pipeline_version="test-v1"
    )
    assert drop.run.status == Stage.REJECTED and not drop.successful_empty
    adapter = RecordedAdapter({"full": b'[{"id":"1","city":"test-city"}]'})
    adapter.expected_count = 10
    partial = collector.collect(cycle(6), adapter, rules, pipeline_version="test-v1")
    assert partial.completeness == "partial" and partial.run.status == Stage.REJECTED
    assert registry.current(cycle(6)) == original


def test_malformed_input_and_interruption_leave_durable_failure(tmp_path: Path) -> None:
    collector, store, registry, rules = setup(tmp_path)
    with pytest.raises(IngestionError):
        collector.collect(
            cycle(), RecordedAdapter({"full": b'[{"id":'}), rules, pipeline_version="test-v1"
        )
    state, _ = registry.state(registry.key(cycle()))
    assert state.active_run is not None and state.active_run.status == Stage.FAILED
    assert store.stat(f"manifests/runs/{state.active_run.run_id}.json") is not None
    assert not list(store.root.rglob(".pending-*"))
    adapter = RecordedAdapter({"full": b"[]"})
    adapter.interrupt = True
    with pytest.raises(KeyboardInterrupt):
        collector.collect(cycle(5), adapter, rules, pipeline_version="test-v1")
    interrupted = registry.state(registry.key(cycle()))[0].active_run
    assert interrupted is not None and interrupted.failure is not None
    assert interrupted.failure.code == "worker-interrupted" and registry.current(cycle()) is None


def test_duplicate_identity_across_parts_is_rejected(tmp_path: Path) -> None:
    collector, _, registry, rules = setup(tmp_path)
    adapter = RecordedAdapter({p: b'[{"id":"same","city":"test-city"}]' for p in ("a", "b")})
    result = collector.collect(cycle(parts=("a", "b")), adapter, rules, pipeline_version="test-v1")
    assert result.run.status == Stage.REJECTED and registry.current(cycle()) is None


def test_raw_fetch_and_replay_never_accept_or_refresh_source(tmp_path: Path) -> None:
    import httpx

    _, store, registry, _ = setup(tmp_path)
    downloader = HttpDownloader(
        Provider.FSA,
        HttpPolicy(),
        transport=httpx.MockTransport(
            lambda request: httpx.Response(200, stream=httpx.ByteStream(b'[{"id":"1"}]'))
        ),
    )
    planned = CollectionCycle.model_validate(
        {
            **cycle().model_dump(),
            "query_sha256": hashlib.sha256(b"https://example.test/venues").hexdigest(),
        }
    )
    raw = fetch_raw(planned, "https://example.test/venues", store, registry, downloader)
    assert raw.run.status == Stage.FETCHED and raw.completeness == "pending"
    assert raw.partitions[0].counts.raw is None and registry.current(cycle()) is None
    repeated = fetch_raw(planned, "https://example.test/venues", store, registry, downloader)
    assert repeated == raw and downloader.requests == 1
    with pytest.raises(ValueError, match="Source URL differs"):
        fetch_raw(planned, "https://example.test/another", store, registry, downloader)
    replay = replay_raw(raw, store, registry, format="json")
    assert replay.parsed_records == 1 and replay.run.mode == "replay"
    assert (
        replay.run.replay_of_snapshot_id == raw.snapshot_id and replay.acceptance == "not-evaluated"
    )
    assert raw.partitions[0].fetched_at == repeated.partitions[0].fetched_at
    assert registry.current(cycle()) is None


def test_failed_snapshot_cannot_be_passed_to_current_pointer(tmp_path: Path) -> None:
    collector, _, registry, rules = setup(tmp_path)
    rejected = collector.collect(
        cycle(),
        RecordedAdapter({"full": b'[{"id":"1","city":"outside"}]'}),
        rules,
        pipeline_version="test-v1",
    )
    from uuid import uuid4

    pending = Run(
        run_id=uuid4(),
        cycle_id=rejected.cycle.cycle_id,
        pipeline_version="test-v1",
        started_at=datetime.now(UTC),
    )
    lease = registry.acquire(cycle(), pending)
    with pytest.raises(ValueError):
        registry.update(lease, pending, current=rejected)
    assert registry.current(cycle()) is None
    registry.release(lease)


def test_structured_log_is_an_allowlist(tmp_path: Path) -> None:
    collector, _, _, rules = setup(tmp_path)
    result = collector.collect(
        cycle(), RecordedAdapter({"full": b"[]"}), rules, pipeline_version="test-v1"
    )
    log = json.loads(safe_log(result.run))
    assert set(log) == {"run_id", "cycle_id", "mode", "stage", "attempt", "failure"}
    assert "uri" not in log and "url" not in log


def test_historical_idempotent_invocation_never_rolls_current_back(tmp_path: Path) -> None:
    collector, _, registry, rules = setup(tmp_path)
    adapter = RecordedAdapter({"full": b'[{"id":"1","city":"test-city"}]'})
    original = collector.collect(cycle(), adapter, rules, pipeline_version="test-v1")
    latest = collector.collect(cycle(5), adapter, rules, pipeline_version="test-v1")
    assert collector.collect(cycle(), adapter, rules, pipeline_version="test-v1") == original
    assert registry.current(cycle()) == latest and adapter.calls["full"] == 2
    with pytest.raises(IngestionError, match="older-cycle-cannot-replace-current"):
        collector.collect(cycle(3), adapter, rules, pipeline_version="test-v1")
    assert registry.current(cycle()) == latest


def test_replay_of_accepted_data_preserves_the_exact_current_snapshot(tmp_path: Path) -> None:
    collector, store, registry, rules = setup(tmp_path)
    original = collector.collect(
        cycle(),
        RecordedAdapter({"full": b'[{"id":"1","city":"test-city"}]'}),
        rules,
        pipeline_version="test-v1",
    )
    result = replay_raw(original, store, registry, format="json")
    assert result.parsed_records == 1
    assert registry.current(cycle()) == original


@pytest.mark.parametrize(
    "format,data,tag,expected",
    [
        ("csv", b"id,name\n1,One\n2,Two\n", None, 2),
        ("xml", b"<Root><Business><Id>1</Id></Business></Root>", "Business", 1),
    ],
)
def test_raw_inspection_parsers_replay_retained_bytes(
    tmp_path: Path, format: str, data: bytes, tag: str | None, expected: int
) -> None:
    import httpx

    _, store, registry, _ = setup(tmp_path)
    downloader = HttpDownloader(
        Provider.FSA,
        transport=httpx.MockTransport(
            lambda request: httpx.Response(200, stream=httpx.ByteStream(data))
        ),
    )
    planned = CollectionCycle.model_validate(
        {
            **cycle().model_dump(),
            "query_sha256": hashlib.sha256(b"https://example.test/data").hexdigest(),
        }
    )
    original = fetch_raw(planned, "https://example.test/data", store, registry, downloader)
    result = replay_raw(original, store, registry, format=format, record_tag=tag)
    assert result.parsed_records == expected and registry.current(cycle()) is None


def test_corrupt_replay_fails_without_a_new_snapshot(tmp_path: Path) -> None:
    collector, store, registry, rules = setup(tmp_path)
    original = collector.collect(
        cycle(), RecordedAdapter({"full": b"[]"}), rules, pipeline_version="test-v1"
    )
    from urllib.parse import urlsplit
    from urllib.request import url2pathname

    artifact = original.partitions[0].artifacts[0]
    Path(url2pathname(urlsplit(artifact.uri).path)).write_bytes(b"corrupt")
    with pytest.raises(IngestionError):
        replay_raw(original, store, registry, format="json")
    state, _ = registry.state(registry.key(cycle()))
    assert state.active_run is not None and state.active_run.mode == "replay"
    assert state.active_run.status == Stage.FAILED and state.current == original


def test_cli_fetch_replay_inspect_with_injected_transport(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    import httpx

    from urbanlens.ingestion import commands

    calls = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(request)
        return httpx.Response(200, stream=httpx.ByteStream(b'[{"id":"1"}]'))

    def downloader(provider: Provider, policy: HttpPolicy) -> HttpDownloader:
        return HttpDownloader(provider, policy, transport=httpx.MockTransport(handler))

    monkeypatch.setattr(commands, "HttpDownloader", downloader)
    monkeypatch.setattr(
        "sys.argv",
        [
            "ingest",
            "--root",
            str(tmp_path),
            "fetch",
            "--provider",
            "fsa",
            "--scope",
            "test-city",
            "--url",
            "https://example.test/data",
        ],
    )
    commands.main()
    raw_path = next((tmp_path / "objects").rglob("raw-snapshot.json"))
    capsys.readouterr()
    monkeypatch.setattr(
        "sys.argv", ["ingest", "--root", str(tmp_path), "replay", str(raw_path), "--format", "json"]
    )
    commands.main()
    assert "Parsed 1 records" in capsys.readouterr().out
    monkeypatch.setattr("sys.argv", ["ingest", "--root", str(tmp_path), "inspect", str(raw_path)])
    commands.main()
    assert json.loads(capsys.readouterr().out)["completeness"] == "pending"
    assert len(calls) == 1


def test_collection_record_budget_never_commits_partial_silver(tmp_path: Path) -> None:
    collector, store, registry, rules = setup(tmp_path)
    collector.max_records = 1
    adapter = RecordedAdapter(
        {"full": b'[{"id":"1","city":"test-city"},{"id":"2","city":"test-city"}]'}
    )
    with pytest.raises(IngestionError, match="collection-runtime-or-record-budget"):
        collector.collect(cycle(), adapter, rules, pipeline_version="test-v1")
    assert registry.current(cycle()) is None
    assert not list(store.root.rglob("records.jsonl.gz"))
    assert not list(store.root.rglob(".pending-*"))


def test_automatic_run_retry_ceiling_prevents_another_fetch(tmp_path: Path) -> None:
    collector, _, registry, rules = setup(tmp_path)
    registry.max_attempts = 1
    adapter = RecordedAdapter({"full": b"[]"})
    adapter.fail_part = "full"
    with pytest.raises(IngestionError, match="fixture-timeout"):
        collector.collect(cycle(), adapter, rules, pipeline_version="test-v1")
    with pytest.raises(IngestionError, match="run-retry-budget"):
        collector.collect(cycle(), adapter, rules, pipeline_version="test-v1")
    assert adapter.calls["full"] == 1 and registry.current(cycle()) is None


def test_replay_lease_cannot_be_repurposed_as_a_fresh_collection(tmp_path: Path) -> None:
    from uuid import uuid4

    _, _, registry, _ = setup(tmp_path)
    replay = Run(
        run_id=uuid4(),
        cycle_id=cycle().cycle_id,
        pipeline_version="test-v1",
        mode="replay",
        replay_of_snapshot_id=uuid4(),
        started_at=datetime.now(UTC),
    )
    lease = registry.acquire(cycle(), replay)
    forged = Run.model_validate(
        {**replay.model_dump(), "mode": "collect", "replay_of_snapshot_id": None}
    )
    with pytest.raises(ValueError, match="identity/mode"):
        registry.update(lease, forged)
    registry.release(lease)


def test_registry_cannot_skip_stages_or_reopen_a_finished_run(tmp_path: Path) -> None:
    from uuid import uuid4

    from urbanlens.ingestion.contracts import advance_run

    _, _, registry, _ = setup(tmp_path)
    pending = Run(
        run_id=uuid4(),
        cycle_id=cycle().cycle_id,
        pipeline_version="test-v1",
        started_at=datetime.now(UTC),
    )
    lease = registry.acquire(cycle(), pending)
    skipped = Run.model_validate({**pending.model_dump(), "status": Stage.ACCEPTED})
    with pytest.raises(ValueError, match="Invalid stage transition"):
        registry.update(lease, skipped)
    finished = advance_run(pending, Stage.FETCHED, ended_at=datetime.now(UTC))
    registry.update(lease, finished)
    with pytest.raises(ValueError, match="Finished run"):
        registry.update(lease, pending)
    registry.release(lease)

"""FSA source fidelity and last-valid behavior; fixtures contain invented premises."""

import gzip
import json
from datetime import date
from pathlib import Path
from typing import Any

import httpx
import pytest

from scripts.fsa_source import check
from urbanlens.ingestion.contracts import Snapshot, Stage
from urbanlens.ingestion.engine import Collector, metadata_key
from urbanlens.ingestion.errors import IngestionError
from urbanlens.ingestion.fsa import PIPELINE, FsaAdapter, FsaConfig
from urbanlens.ingestion.http import HttpDownloader, HttpPolicy
from urbanlens.ingestion.registry import Registry, SQLiteStateBackend
from urbanlens.ingestion.storage import LocalArtifactStore

FIXTURES = Path(__file__).parent / "fixtures/fsa"


def test_complete_manifest_example_matches_synthetic_raw_artifacts() -> None:
    path = Path(__file__).parents[1] / "reference/ingestion/examples/fsa-complete.json"
    example = Snapshot.model_validate_json(path.read_text(encoding="utf-8"))
    assert example.run.status == Stage.ACCEPTED
    assert example.partitions[0].counts.accepted == 4
    assert example.partitions[0].source_extract_date == date(2026, 11, 1)
    assert example.partitions[0].source_extracted_at is None
    assert all(a.uri.startswith("file:///synthetic/") for a in example.partitions[0].artifacts)


def setup(
    tmp_path: Path, xml: bytes, *, authority: dict[str, Any] | None = None
) -> tuple[FsaConfig, FsaAdapter, LocalArtifactStore, Registry, list[str]]:
    config = FsaConfig(min_records=0)
    calls: list[str] = []

    def request(request: httpx.Request) -> httpx.Response:
        calls.append(str(request.url))
        content = (
            json.dumps(authority or json.loads((FIXTURES / "authority.json").read_text())).encode()
            if "/Authorities/" in str(request.url)
            else xml
        )
        return httpx.Response(200, stream=httpx.ByteStream(content))

    client = HttpDownloader(
        config.cycle(date(2026, 11, 4)).provider,
        HttpPolicy(max_requests=6, backoff_seconds=0),
        transport=httpx.MockTransport(request),
    )
    store = LocalArtifactStore(tmp_path / "objects")
    registry = Registry(SQLiteStateBackend(tmp_path / "registry.sqlite"))
    return config, FsaAdapter(config, client), store, registry, calls


def test_full_authority_preserves_all_types_and_ratings_and_privacy(tmp_path: Path) -> None:
    raw = (FIXTURES / "valid.xml").read_bytes()
    config, adapter, store, registry, calls = setup(tmp_path, raw)
    cycle = config.cycle(date(2026, 11, 4))
    snapshot = Collector(store, registry).collect(
        cycle, adapter, config.rules(), pipeline_version=PIPELINE
    )
    assert snapshot.run.status == Stage.ACCEPTED and len(calls) == 2
    part = snapshot.partitions[0]
    assert part.counts.raw == part.counts.accepted == 4 and part.counts.rejected == 0
    assert str(part.source_extract_date) == "2026-11-01" and part.source_extracted_at is None
    assert gzip.decompress(b"".join(store.read_verified(part.artifacts[0]))) == raw
    rows = list(adapter.parse(part, store))
    assert rows[0]["fhrs_id"] == "000001" and rows[0]["rating_score"] == 5
    assert rows[1]["published_address"] is None and rows[1]["longitude"] is None
    assert "Must not be exposed" not in json.dumps(rows[1])
    assert rows[1]["rating_state"] == "awaiting-inspection" and rows[1]["rating_score"] is None
    assert rows[2]["business_type_id"] == "4613" and rows[2]["location_state"] == "missing"
    assert rows[2]["rating_state"] == "exempt" and rows[2]["schema_valid"]
    assert rows[3]["business_type_id"] == "7845" and rows[3]["location_state"] == "invalid"
    assert (
        rows[3]["rating_state"] == "awaiting-publication" and rows[3]["new_rating_pending"] is True
    )
    repeated = Collector(store, registry).collect(
        cycle, adapter, config.rules(), pipeline_version=PIPELINE
    )
    assert repeated == snapshot and len(calls) == 2


@pytest.mark.parametrize(
    "old,new",
    [
        (b"<ReturnCode>Success</ReturnCode>", b"<ReturnCode>Error</ReturnCode>"),
        (b"<ItemCount>4</ItemCount>", b"<ItemCount>5</ItemCount>"),
        (b"<ExtractDate>2026-11-01</ExtractDate>", b"<ExtractDate>invalid</ExtractDate>"),
        (b"</FHRSEstablishment>", b""),
        (
            b"</EstablishmentCollection>",
            b"<Error>upstream failed</Error></EstablishmentCollection>",
        ),
    ],
)
def test_malformed_or_incomplete_keeps_raw_and_cannot_be_current(
    tmp_path: Path, old: bytes, new: bytes
) -> None:
    raw = (FIXTURES / "valid.xml").read_bytes().replace(old, new)
    config, adapter, store, registry, _ = setup(tmp_path, raw)
    cycle = config.cycle(date(2026, 11, 4))
    with pytest.raises(IngestionError):
        Collector(store, registry).collect(
            cycle, adapter, config.rules(), pipeline_version=PIPELINE
        )
    assert registry.current(cycle) is None
    state, _ = registry.state(registry.key(cycle))
    assert state.active_run is not None and state.active_run.status == Stage.FAILED
    raw_artifacts = list(store.root.rglob("establishments.xml.gz"))
    assert len(raw_artifacts) == 1 and gzip.decompress(raw_artifacts[0].read_bytes()) == raw
    assert not list(store.root.rglob("records.jsonl.gz"))


@pytest.mark.parametrize(
    "old,new,reason",
    [
        (b"<FHRSID>000004</FHRSID>", b"<FHRSID>000001</FHRSID>", "duplicate"),
        (b"<FHRSID>000004</FHRSID>", b"<FHRSID/>", "invalid-fhrs-id"),
        (
            b"<BusinessTypeID>7845</BusinessTypeID>",
            b"<BusinessTypeID/>",
            "missing-business-type-id",
        ),
        (
            b"<LocalAuthorityCode>875</LocalAuthorityCode>",
            b"<LocalAuthorityCode>999</LocalAuthorityCode>",
            "wrong-authority",
        ),
    ],
)
def test_invalid_keys_scope_and_duplicates_reject_with_reasons(
    tmp_path: Path, old: bytes, new: bytes, reason: str
) -> None:
    raw = (FIXTURES / "valid.xml").read_bytes().replace(old, new)
    config, adapter, store, registry, _ = setup(tmp_path, raw)
    cycle = config.cycle(date(2026, 11, 4))
    result = Collector(store, registry).collect(
        cycle, adapter, config.rules(), pipeline_version=PIPELINE
    )
    assert result.run.status == Stage.REJECTED and registry.current(cycle) is None
    assert result.partitions[0].counts.rejected
    artifact = store.stat(metadata_key(cycle, f"attempts/{result.run.run_id}/rejection-reasons"))
    assert artifact is not None
    assert reason in json.loads(b"".join(store.read_verified(artifact)))


def test_bad_authority_mapping_does_not_download_another_authority(tmp_path: Path) -> None:
    authority = json.loads((FIXTURES / "authority.json").read_text())
    authority["LocalAuthorityIdCode"] = "999"
    config, adapter, store, registry, calls = setup(tmp_path, b"", authority=authority)
    with pytest.raises(IngestionError, match="fsa-authority-mapping-mismatch"):
        Collector(store, registry).collect(
            config.cycle(date(2026, 11, 4)), adapter, config.rules(), pipeline_version=PIPELINE
        )
    assert len(calls) == 1 and list(store.root.rglob("authority.json.gz"))


def test_count_floor_rejects_plausible_but_tiny_complete_authority(tmp_path: Path) -> None:
    config, adapter, store, registry, _ = setup(tmp_path, (FIXTURES / "valid.xml").read_bytes())
    strict = FsaConfig(min_records=1000)
    # The query is pinned to the rules/config used for this collection.
    adapter.config = strict
    result = Collector(store, registry).collect(
        strict.cycle(date(2026, 11, 4)), adapter, strict.rules(), pipeline_version=PIPELINE
    )
    assert result.run.status == Stage.REJECTED and registry.current(result.cycle) is None


def test_replay_uses_retained_data_and_has_identical_normalized_hash(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config, adapter, store, registry, calls = setup(tmp_path, (FIXTURES / "valid.xml").read_bytes())
    snapshot = Collector(store, registry).collect(
        config.cycle(date(2026, 11, 4)), adapter, config.rules(), pipeline_version=PIPELINE
    )

    def no_network(*args: object, **kwargs: object) -> None:
        raise AssertionError("Replay must never download")

    monkeypatch.setattr(HttpDownloader, "download", no_network)
    report = check(config, store, registry)
    assert report["normalized_hashes_match"] and report["current_snapshot_unchanged"]
    assert report["records"] == 4 and len(calls) == 2
    assert registry.current(snapshot.cycle) == snapshot


def test_bad_followup_never_replaces_accepted_snapshot(tmp_path: Path) -> None:
    raw = (FIXTURES / "valid.xml").read_bytes()
    config, adapter, store, registry, _ = setup(tmp_path, raw)
    original = Collector(store, registry).collect(
        config.cycle(date(2026, 11, 4)), adapter, config.rules(), pipeline_version=PIPELINE
    )
    _, broken, _, _, _ = setup(
        tmp_path / "unused", raw.replace(b"<ItemCount>4</ItemCount>", b"<ItemCount>100</ItemCount>")
    )
    with pytest.raises(IngestionError):
        Collector(store, registry).collect(
            config.cycle(date(2026, 12, 4)), broken, config.rules(), pipeline_version=PIPELINE
        )
    assert registry.current(original.cycle) == original

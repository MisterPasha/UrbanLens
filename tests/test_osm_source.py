"""OSM source integrity, geometry and cached replay with invented features only."""

import gzip
import hashlib
import json
from datetime import date
from pathlib import Path
from typing import Any

import httpx
import pytest
from shapely.geometry import Point, box, shape

from scripts.osm_source import check
from urbanlens.ingestion.contracts import Provider, Stage
from urbanlens.ingestion.engine import Collector
from urbanlens.ingestion.errors import IngestionError
from urbanlens.ingestion.http import HttpDownloader, HttpPolicy
from urbanlens.ingestion.osm import PIPELINE, OsmAdapter, OsmConfig, definition, geometry
from urbanlens.ingestion.registry import Registry, SQLiteStateBackend
from urbanlens.ingestion.storage import LocalArtifactStore

FIXTURES = Path(__file__).parent / "fixtures/osm"
ROOT = Path(__file__).parents[1]
DAY = date(2026, 11, 4)


@pytest.fixture(autouse=True)
def fast_offline_requests(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("urbanlens.ingestion.http.RATES", {p: 100_000.0 for p in Provider})


def setup(
    tmp_path: Path, raw: bytes | None = None
) -> tuple[OsmConfig, OsmAdapter, LocalArtifactStore, Registry, list[str]]:
    boundary = (FIXTURES / "city.geojson").read_bytes()
    template = (ROOT / "reference/ingestion/osm-query.overpassql").read_bytes()
    config = OsmConfig(
        boundary_sha256=hashlib.sha256(boundary).hexdigest(),
        template_sha256=hashlib.sha256(template).hexdigest(),
        min_records=0,
    )
    calls = []
    content = (FIXTURES / "complete.json").read_bytes() if raw is None else raw

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(str(request.url))
        return httpx.Response(200, stream=httpx.ByteStream(content))

    downloader = HttpDownloader(
        Provider.OSM, HttpPolicy(max_requests=2), transport=httpx.MockTransport(handler)
    )
    adapter = OsmAdapter(config, definition(config, boundary, template), downloader)
    return (
        config,
        adapter,
        LocalArtifactStore(tmp_path / "objects"),
        Registry(SQLiteStateBackend(tmp_path / "registry.sqlite")),
        calls,
    )


def test_complete_source_retains_types_raw_geometry_metadata_and_ambiguous_links(
    tmp_path: Path,
) -> None:
    config, adapter, store, registry, calls = setup(tmp_path)
    snapshot = Collector(store, registry).collect(
        config.cycle(DAY), adapter, config.rules(), pipeline_version=PIPELINE
    )
    assert snapshot.run.status == Stage.ACCEPTED
    part = snapshot.partitions[0]
    assert part.counts.raw == part.counts.accepted == 4 and part.counts.rejected == 0
    assert part.source_extracted_at is not None and part.source_extracted_at.hour == 10
    assert (
        gzip.decompress(b"".join(store.read_verified(part.artifacts[0])))
        == (FIXTURES / "complete.json").read_bytes()
    )
    rows = list(adapter.parse(part, store))
    assert (
        len({r["id"] for r in rows}) == 4
    )  # Same numeric ID across three OSM types is legitimate.
    assert rows[0]["source_version"] == 3 and rows[0]["source_timestamp"]
    assert rows[0]["fhrs_ids"] == ["000123", "456"] and rows[0]["invalid_fhrs_tokens"] == ["bad"]
    assert rows[1]["original_geometry"] == {
        k: json.loads((FIXTURES / "complete.json").read_bytes())["elements"][1][k]
        for k in ("nodes", "geometry")
    }
    assert not rows[3]["venue_tag"] and rows[3]["fhrs_ids"] == ["789"]
    assert rows[3]["geometry_state"] == "unresolved-open-way" and rows[3]["in_city"] is None
    assert (
        Collector(store, registry).collect(
            config.cycle(DAY), adapter, config.rules(), pipeline_version=PIPELINE
        )
        == snapshot
    )
    assert len(calls) == 1


def test_padded_extraction_captures_crossing_polygon_with_no_vertex_inside_city(
    tmp_path: Path,
) -> None:
    config, adapter, store, registry, _ = setup(tmp_path)
    snapshot = Collector(store, registry).collect(
        config.cycle(DAY), adapter, config.rules(), pipeline_version=PIPELINE
    )
    row: Any = list(adapter.parse(snapshot.partitions[0], store))[1]
    assert adapter.source is not None
    city = shape(adapter.source["boundary"]["geometry"])
    assert all(
        not city.covers(Point(p["lon"], p["lat"])) for p in row["original_geometry"]["geometry"]
    )
    south, west, north, east = adapter.source["bbox"]
    assert all(
        box(west, south, east, north).covers(Point(p["lon"], p["lat"]))
        for p in row["original_geometry"]["geometry"]
    )
    assert row["in_city"] and row["crosses_city_boundary"]
    polygon = shape(row["geometry"])
    assert polygon.covers(Point(row["longitude"], row["latitude"]))
    assert not city.covers(polygon)


def test_multipolygon_point_avoids_courtyard_and_original_members_remain(tmp_path: Path) -> None:
    config, adapter, store, registry, _ = setup(tmp_path)
    snapshot = Collector(store, registry).collect(
        config.cycle(DAY), adapter, config.rules(), pipeline_version=PIPELINE
    )
    row: Any = list(adapter.parse(snapshot.partitions[0], store))[2]
    polygon = shape(row["geometry"])
    assert not polygon.covers(polygon.centroid)
    assert polygon.covers(Point(row["longitude"], row["latitude"]))
    assert len(row["original_geometry"]["members"]) == 2


@pytest.mark.parametrize(
    "change",
    [
        "remark",
        "error",
        "truncated",
        "missing-count",
        "wrong-count",
        "wrong-type-count",
        "fractional-count",
        "extra-type",
        "missing-base",
        "bad-base",
        "bad-id",
        "bad-version",
        "bad-tags",
        "edit-after-base",
        "unrelated-tag",
        "outside-scope",
    ],
)
def test_partial_and_malformed_responses_never_replace_current(tmp_path: Path, change: str) -> None:
    config, adapter, store, registry, _ = setup(tmp_path)
    previous = Collector(store, registry).collect(
        config.cycle(DAY), adapter, config.rules(), pipeline_version=PIPELINE
    )
    value = json.loads((FIXTURES / "complete.json").read_bytes())
    if change in {"remark", "error"}:
        value[change] = "runtime error: timeout"
    if change == "missing-count":
        value["elements"].pop()
    if change == "wrong-count":
        value["elements"][-1]["tags"]["total"] = "999"
    if change == "wrong-type-count":
        value["elements"][-1]["tags"]["nodes"] = "2"
    if change == "fractional-count":
        value["elements"][-1]["tags"]["nodes"] = 1.9
    if change == "extra-type":
        value["elements"][0]["type"] = "unknown"
    if change == "missing-base":
        value["osm3s"].pop("timestamp_osm_base")
    if change == "bad-base":
        value["osm3s"]["timestamp_osm_base"] = "not-a-time"
    if change == "bad-id":
        value["elements"][0]["id"] = True
    if change == "bad-version":
        value["elements"][0]["version"] = 0
    if change == "bad-tags":
        value["elements"][0]["tags"]["name"] = 42
    if change == "edit-after-base":
        value["elements"][0]["timestamp"] = "2026-11-04T11:00:00Z"
    if change == "unrelated-tag":
        value["elements"][0]["tags"] = {"shop": "books"}
    if change == "outside-scope":
        value["elements"][0]["lon"] = 10
    raw = json.dumps(value).encode()
    if change == "truncated":
        raw = raw[:-5]
    _, broken, _, _, _ = setup(tmp_path / "unused", raw)
    with pytest.raises(IngestionError):
        Collector(store, registry).collect(
            config.cycle(date(2026, 12, 4)), broken, config.rules(), pipeline_version=PIPELINE
        )
    assert registry.current(previous.cycle) == previous
    assert any(gzip.decompress(p.read_bytes()) == raw for p in store.root.rglob("elements.json.gz"))


def test_duplicate_type_id_rejects_collection(tmp_path: Path) -> None:
    value = json.loads((FIXTURES / "complete.json").read_bytes())
    value["elements"][3]["id"] = 1
    config, adapter, store, registry, _ = setup(tmp_path, json.dumps(value).encode())
    result = Collector(store, registry).collect(
        config.cycle(DAY), adapter, config.rules(), pipeline_version=PIPELINE
    )
    assert result.run.status == Stage.REJECTED and registry.current(result.cycle) is None


def test_replay_is_offline_deterministic_and_does_not_advance_current(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config, adapter, store, registry, calls = setup(tmp_path)
    snapshot = Collector(store, registry).collect(
        config.cycle(DAY), adapter, config.rules(), pipeline_version=PIPELINE
    )

    def no_network(*args: object, **kwargs: object) -> None:
        raise AssertionError("Replay must not download")

    monkeypatch.setattr(HttpDownloader, "download", no_network)
    report = check(config, store, registry)
    assert report["normalized_hashes_match"] and report["current_snapshot_unchanged"]
    assert report["records"] == 4 and len(calls) == 1
    assert registry.current(snapshot.cycle) == snapshot


@pytest.mark.parametrize("kind", ["boundary", "template"])
def test_pin_change_requires_explicit_configuration_update(tmp_path: Path, kind: str) -> None:
    config, _, _, _, _ = setup(tmp_path)
    boundary = (FIXTURES / "city.geojson").read_bytes()
    template = (ROOT / "reference/ingestion/osm-query.overpassql").read_bytes()
    with pytest.raises(IngestionError, match="osm-.*-hash"):
        definition(
            config,
            boundary + (b" " if kind == "boundary" else b""),
            template + (b" " if kind == "template" else b""),
        )


@pytest.mark.parametrize(
    "points,state",
    [
        ([None], "unresolved-invalid-geometry"),
        ([{"lon": float("nan"), "lat": 50}], "unresolved-invalid-geometry"),
    ],
)
def test_bad_geometry_is_flagged_not_fabricated(points: Any, state: str) -> None:
    result, method = geometry(
        {"type": "way", "id": 2, "nodes": [1], "geometry": points, "tags": {"amenity": "pub"}}
    )
    assert result is None and method == state


def test_complete_but_large_count_drop_preserves_previous_current(tmp_path: Path) -> None:
    config, adapter, store, registry, _ = setup(tmp_path)
    previous = Collector(store, registry).collect(
        config.cycle(DAY), adapter, config.rules(), pipeline_version=PIPELINE
    )
    value = json.loads((FIXTURES / "complete.json").read_bytes())
    value["elements"] = [
        value["elements"][0],
        {"type": "count", "tags": {"nodes": "1", "ways": "0", "relations": "0", "total": "1"}},
    ]
    _, smaller, _, _, _ = setup(tmp_path / "unused", json.dumps(value).encode())
    result = Collector(store, registry).collect(
        config.cycle(date(2026, 12, 4)), smaller, config.rules(), pipeline_version=PIPELINE
    )
    assert result.run.status == Stage.REJECTED
    assert registry.current(previous.cycle) == previous


@pytest.mark.parametrize("field", ["query", "boundary"])
def test_retained_definition_must_match_all_pinned_inputs(tmp_path: Path, field: str) -> None:
    config, adapter, store, registry, _ = setup(tmp_path)
    assert adapter.source is not None
    if field == "query":
        adapter.source["query"] += "\n// changed"
    else:
        adapter.source["boundary"]["geometry"]["coordinates"][0][0][0] = -0.001
    with pytest.raises(IngestionError, match="osm-retained-definition-mismatch"):
        Collector(store, registry).collect(
            config.cycle(DAY), adapter, config.rules(), pipeline_version=PIPELINE
        )
    assert registry.current(config.cycle(DAY)) is None


def test_concave_polygon_uses_surface_point_instead_of_outside_centroid() -> None:
    points = [
        (0, 50),
        (0.01, 50),
        (0.01, 50.01),
        (0.008, 50.01),
        (0.008, 50.002),
        (0.002, 50.002),
        (0.002, 50.01),
        (0, 50.01),
        (0, 50),
    ]
    element = {
        "type": "way",
        "nodes": list(range(8)) + [0],
        "geometry": [{"lon": x, "lat": y} for x, y in points],
        "tags": {"amenity": "cafe"},
    }
    polygon, method = geometry(element)
    assert method == "polygon-point-on-surface"
    assert not polygon.covers(polygon.centroid)
    assert polygon.covers(polygon.representative_point())

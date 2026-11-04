"""Synthetic cases based on pilot structures; no real business/address records published."""

import json
import xml.etree.ElementTree as ET
from typing import Any

import pytest
from shapely.geometry import Point

from scripts.venue_pilot import fhrs_ids, fsa_rows, normalise, osm_elements, osm_point


def fsa_fixture(*, count: int = 1, authority: str = "875", result: str = "Success") -> bytes:
    root = ET.Element("FHRSEstablishment")
    header = ET.SubElement(root, "Header")
    for name, value in {
        "ItemCount": str(count),
        "ReturnCode": result,
        "ExtractDate": "2026-10-29",
    }.items():
        ET.SubElement(header, name).text = value
    collection = ET.SubElement(root, "EstablishmentCollection")
    row = ET.SubElement(collection, "EstablishmentDetail")
    # Missing address and outward-only postcode must remain held, not enriched from another source.
    for name, value in {
        "FHRSID": "123",
        "LocalAuthorityCode": authority,
        "BusinessTypeID": "1",
        "BusinessName": "Synthetic canteen",
        "PostCode": "BN1",
    }.items():
        ET.SubElement(row, name).text = value
    return ET.tostring(root)


@pytest.mark.parametrize("change", [{"count": 2}, {"authority": "999"}, {"result": "Failed"}])
def test_fsa_rejects_incomplete_or_wrong_authority(change: dict[str, Any]) -> None:
    with pytest.raises(ValueError):
        fsa_rows(fsa_fixture(**change))


def test_fsa_preserves_missing_and_possible_suppressed_location() -> None:
    rows, metadata = fsa_rows(fsa_fixture())
    assert metadata["ExtractDate"] == "2026-10-29"
    assert rows[0]["type"] == "1"  # This does not establish public restaurant eligibility.
    assert rows[0]["point"] is None
    assert rows[0]["possible_suppression"] and rows[0]["privacy"]
    assert not rows[0]["privacy_marker"]  # Suspected suppression is not confirmed suppression.


def test_fsa_rejects_duplicate_identity() -> None:
    root = ET.fromstring(fsa_fixture(count=2))
    collection = root.find("EstablishmentCollection")
    assert collection is not None
    collection.append(ET.fromstring(ET.tostring(collection[0])))
    with pytest.raises(ValueError, match="duplicate FHRSID"):
        fsa_rows(ET.tostring(root))


@pytest.mark.parametrize("partial", ["remark", "missing_count", "wrong_count", "wrong_type_count"])
def test_overpass_rejects_partial_responses(partial: str) -> None:
    row: dict[str, Any] = {"type": "node", "id": 123, "lon": 0, "lat": 0}
    counts: dict[str, Any] = {
        "type": "count",
        "tags": {"total": "1", "nodes": "1", "ways": "0", "relations": "0"},
    }
    payload: dict[str, Any] = {
        "osm3s": {"timestamp_osm_base": "2026-11-02T00:00:00Z"},
        "elements": [row, counts],
    }
    if partial == "remark":
        payload["remark"] = "runtime error: Query timed out"
    elif partial == "missing_count":
        payload["elements"].pop()
    elif partial == "wrong_type_count":
        counts["tags"]["nodes"] = "2"
    else:
        counts["tags"]["total"] = "2"
    with pytest.raises(ValueError):
        osm_elements(json.dumps(payload).encode())


def coords(points: list[tuple[int, int]]) -> list[dict[str, int]]:
    return [{"lon": x, "lat": y} for x, y in points]


def test_closed_concave_way_uses_point_on_surface() -> None:
    row = {
        "type": "way",
        "geometry": coords(
            [(0, 0), (4, 0), (4, 4), (3, 4), (3, 1), (1, 1), (1, 4), (0, 4), (0, 0)]
        ),
    }
    point, method, polygon = osm_point(row)
    assert polygon.covers(point)
    assert not polygon.covers(Point(2, 2))  # A bounding-box centre would be wrong.
    assert method == "polygon_point_on_surface"


def test_open_way_has_no_invented_location() -> None:
    assert osm_point({"type": "way", "geometry": coords([(0, 0), (1, 0), (1, 1)])})[0] is None


def test_relation_hole_and_incomplete_member() -> None:
    outer = {
        "type": "way",
        "role": "outer",
        "geometry": coords([(0, 0), (4, 0), (4, 4), (0, 4), (0, 0)]),
    }
    inner = {
        "type": "way",
        "role": "inner",
        "geometry": coords([(1, 1), (3, 1), (3, 3), (1, 3), (1, 1)]),
    }
    row: dict[str, Any] = {
        "type": "relation",
        "tags": {"type": "multipolygon"},
        "members": [outer, inner],
    }
    point, method, polygon = osm_point(row)
    assert polygon.covers(point) and not polygon.covers(Point(2, 2))
    assert method == "multipolygon_point_on_surface"
    row["members"].append({"type": "way", "role": "outer", "geometry": coords([(5, 0), (6, 0)])})
    assert osm_point(row)[0] is None


def test_multivalue_reference_is_not_a_single_match() -> None:
    good, bad = fhrs_ids("123; 456;123;not-an-id")
    assert good == ["123", "456"]
    assert bad == ["not-an-id"]
    assert normalise("Example Café & Bakery") == "example cafe and bakery"

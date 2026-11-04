"""One local, replayable FSA/OSM discovery snapshot; no accepted venues or cloud writes."""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import re
import ssl
import unicodedata
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET
from collections import Counter, defaultdict
from datetime import UTC, datetime
from difflib import SequenceMatcher
from pathlib import Path
from typing import Any

from pyproj import Geod, Transformer, datadir
from shapely.geometry import LineString, Point, Polygon, shape
from shapely.ops import polygonize_full, unary_union
from shapely.strtree import STRtree

ROOT = Path(__file__).resolve().parents[1]
RAW = ROOT / "data/pilots/venues/2026-11-02"
CONFIG = ROOT / "config/venue-pilot.json"
CITY = ROOT / "data/reference/geography/city-coverage-wgs84.geojson"
GEOD = Geod(ellps="WGS84")


def require(condition: bool, message: str) -> None:
    if not condition:
        raise ValueError(message)


def read_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def digest(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def write_json(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")


def coverage() -> Any:
    report = read_json(ROOT / "reference/geography/inspection.json")
    require(digest(CITY) == report["outputs"][CITY.name], "City geometry checksum mismatch")
    return shape(read_json(CITY)["geometry"])


def acquire() -> None:
    RAW.mkdir(parents=True, exist_ok=True)
    manifest_path = RAW / "manifest.json"
    manifest: dict[str, Any] = (
        read_json(manifest_path)
        if manifest_path.exists()
        else {
            "scope": "Brighton & Hove E06000043; discovery only",
            "authority_mapping": {"ons": "E06000043", "fsa_id": 286, "fsa_code": "875"},
            "city_sha256": digest(CITY),
            "config_sha256": digest(CONFIG),
            "artifacts": {},
        }
    )
    require(manifest["city_sha256"] == digest(CITY), "Snapshot geography changed")
    require(
        manifest["config_sha256"] == digest(CONFIG), "Snapshot acquisition configuration changed"
    )

    def fetch(name: str, url: str, *, data: bytes | None = None, fsa: bool = False) -> bytes:
        path = RAW / name
        records = manifest["artifacts"]
        if path.exists():
            require(
                name in records and records[name]["sha256"] == digest(path),
                f"Unpinned/corrupt {name}",
            )
            require(records[name]["url"] == url, f"URL changed for {name}")
            return path.read_bytes()
        headers = {
            "User-Agent": "UrbanLens/0.1 (bounded local source discovery)",
            "Accept": "application/json, application/xml;q=0.9, */*;q=0.1",
        }
        if fsa:
            headers.update({"x-api-version": "2", "Accept-Language": "en-GB"})
        if data is not None:
            headers["Content-Type"] = "application/x-www-form-urlencoded"
        request = urllib.request.Request(url, data=data, headers=headers)
        with urllib.request.urlopen(
            request, context=ssl.create_default_context(), timeout=120
        ) as response:
            content: bytes = response.read(50_000_001)
            require(len(content) <= 50_000_000, f"Response exceeds pilot byte limit: {name}")
            record = {
                "url": url,
                "method": "POST" if data is not None else "GET",
                "request_body_sha256": hashlib.sha256(data).hexdigest() if data else None,
                "request_headers": headers,
                "response_url": response.url,
                "fetched_at": datetime.now(UTC).isoformat(),
                "bytes": len(content),
                "sha256": hashlib.sha256(content).hexdigest(),
                "content_type": response.headers.get("Content-Type"),
                "last_modified": response.headers.get("Last-Modified"),
                "etag": response.headers.get("ETag"),
            }
        # Preserve error-bearing payloads for diagnosis, but they cannot pass offline validation.
        path.write_bytes(content)
        records[name] = record
        write_json(manifest_path, manifest)
        print(f"Saved {name}: {len(content)} bytes", flush=True)
        return content

    authorities = json.loads(
        fetch("authorities.json", "https://api.ratings.food.gov.uk/Authorities/basic", fsa=True)
    )
    matches = [a for a in authorities["authorities"] if a["LocalAuthorityId"] == 286]
    require(
        len(matches) == 1 and matches[0]["LocalAuthorityIdCode"] == "875",
        "FSA authority mapping changed",
    )
    authority = json.loads(
        fetch("authority.json", "https://api.ratings.food.gov.uk/Authorities/286", fsa=True)
    )
    require(authority["LocalAuthorityIdCode"] == "875", "FSA detail authority changed")
    extract_url = authority["FileName"]
    require(urllib.parse.urlparse(extract_url).scheme == "https", "FSA extract must use HTTPS")
    fetch("fsa.xml", extract_url, fsa=True)
    minx, miny, maxx, maxy = coverage().bounds
    # Envelope is a collection bound; exact pinned city coverage is applied during inspection.
    bbox = f"{miny - 0.001:.7f},{minx - 0.001:.7f},{maxy + 0.001:.7f},{maxx + 0.001:.7f}"
    query = (
        "[out:json][timeout:90][maxsize:33554432];\n(\n"
        f' nwr["amenity"~"(^|;)[ ]*(restaurant|cafe|fast_food|pub|bar)[ ]*(;|$)"]({bbox});\n'
        f' nwr["shop"~"(^|;)[ ]*bakery[ ]*(;|$)"]({bbox});\n'
        f' nwr["fhrs:id"]({bbox});\n);\nout body geom;\nout count;\n'
    )
    query_path = RAW / "query.overpassql"
    if query_path.exists():
        require(query_path.read_text(encoding="utf-8") == query, "Snapshot query changed")
    else:
        query_path.write_text(query, encoding="utf-8")
    manifest["query_sha256"] = digest(query_path)
    manifest["bbox_south_west_north_east"] = bbox
    write_json(manifest_path, manifest)
    body = urllib.parse.urlencode({"data": query}).encode()
    fetch("osm.json", read_json(CONFIG)["overpass_endpoint"], data=body)
    print("Acquisition saved. Run venue-pilot-check to validate and inspect; nothing accepted.")


def fsa_rows(content: bytes) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    root = ET.fromstring(content)
    header = root.find("Header")
    require(header is not None, "Missing FSA header")
    assert header is not None
    metadata = {child.tag: child.text for child in header}
    require(metadata.get("ReturnCode") == "Success", "FSA return code is not Success")
    require(bool(metadata.get("ExtractDate")), "FSA extract date missing")
    elements = root.findall("./EstablishmentCollection/EstablishmentDetail")
    require(bool(elements), "Empty FSA extract")
    require(int(metadata["ItemCount"] or "-1") == len(elements), "FSA header count mismatch")
    result = []
    seen = set()
    for element in elements:

        def field(name: str, element: ET.Element = element) -> str:
            return element.findtext(name, default="").strip()

        ident = field("FHRSID")
        require(ident.isdigit() and ident not in seen, "Invalid/duplicate FHRSID")
        seen.add(ident)
        require(field("LocalAuthorityCode") == "875", "Extract contains another FSA authority")
        lat, lon = field("Geocode/Latitude"), field("Geocode/Longitude")
        point = None
        if lat and lon:
            x, y = float(lon), float(lat)
            if -180 <= x <= 180 and -90 <= y <= 90:
                point = Point(x, y)
        # Missing location and privacy state remain distinct; never enrich either in this pilot.
        privacy = field("AddressLine1").casefold() in {"private address", "address not disclosed"}
        address = " ".join(field(f"AddressLine{i}") for i in range(1, 5)).strip()
        possible_suppression = not address and len(field("PostCode").replace(" ", "")) < 5
        result.append(
            {
                "key": ident,
                "name": field("BusinessName"),
                "type": field("BusinessTypeID"),
                "type_name": field("BusinessType"),
                "postcode": field("PostCode"),
                "address": address,
                "point": point,
                "privacy": privacy or possible_suppression,
                "privacy_marker": privacy,
                "possible_suppression": possible_suppression,
                "rating": field("RatingValue"),
                "rating_date": field("RatingDate"),
                "pending": field("NewRatingPending"),
                "fields": sorted(child.tag for child in element),
            }
        )
    return result, metadata


def osm_elements(content: bytes) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    payload = json.loads(content)
    require(
        not payload.get("remark") and not payload.get("error"),
        "OSM returned an error/partial remark",
    )
    require(bool(payload.get("osm3s", {}).get("timestamp_osm_base")), "Missing OSM base timestamp")
    values = payload.get("elements", [])
    counts = [e for e in values if e.get("type") == "count"]
    elements = [e for e in values if e.get("type") in {"node", "way", "relation"}]
    require(len(counts) == 1 and values[-1] == counts[0], "OSM missing terminal completeness count")
    require(int(counts[0]["tags"]["total"]) == len(elements), "OSM terminal count mismatch")
    require(
        len({(e["type"], e["id"]) for e in elements}) == len(elements), "OSM duplicate element keys"
    )
    for kind in ("node", "way", "relation"):
        require(
            int(counts[0]["tags"][kind + "s"]) == sum(e["type"] == kind for e in elements),
            f"OSM terminal {kind} count mismatch",
        )
    return elements, payload["osm3s"]


def osm_point(element: dict[str, Any]) -> tuple[Any, str, Any]:
    if element["type"] == "node":
        return Point(element["lon"], element["lat"]), "source_node", None
    if element["type"] == "way":
        if any(p is None for p in element.get("geometry", [])):
            return None, "unresolved_incomplete_way", None
        points = [(p["lon"], p["lat"]) for p in element.get("geometry", []) if p]
        if (
            len(points) >= 4
            and points[0] == points[-1]
            and element.get("tags", {}).get("area") != "no"
        ):
            polygon = Polygon(points)
            if polygon.is_valid and polygon.area > 0:
                return polygon.representative_point(), "polygon_point_on_surface", polygon
        return None, "unresolved_open_or_invalid_way", None
    if element.get("tags", {}).get("type") != "multipolygon":
        return None, "unresolved_nonmultipolygon_relation", None
    outers: list[Any] = []
    inners: list[Any] = []
    for member in element.get("members", []):
        if member.get("role") not in {"", "outer", "inner"} or any(
            p is None for p in member.get("geometry", [])
        ):
            return None, "unresolved_relation_member", None
        points = [(p["lon"], p["lat"]) for p in member.get("geometry", []) if p]
        if member.get("type") != "way" or len(points) < 2:
            return None, "unresolved_relation_member", None
        (inners if member.get("role") == "inner" else outers).append(LineString(points))
    if not outers:
        return None, "unresolved_relation_outer", None
    polygons, cuts, dangles, invalid = polygonize_full(outers)
    if polygons.is_empty or not all(g.is_empty for g in (cuts, dangles, invalid)):
        return None, "unresolved_relation_ring", None
    polygon = unary_union(list(polygons.geoms))
    if inners:
        holes, cuts, dangles, invalid = polygonize_full(inners)
        if holes.is_empty or not all(g.is_empty for g in (cuts, dangles, invalid)):
            return None, "unresolved_relation_inner_ring", None
        hole_geometry = unary_union(list(holes.geoms))
        if not polygon.covers(hole_geometry):
            return None, "unresolved_relation_hole_outside", None
        polygon = polygon.difference(hole_geometry)
    if polygon.is_valid and not polygon.is_empty:
        return polygon.representative_point(), "multipolygon_point_on_surface", polygon
    return None, "unresolved_relation_geometry", None


def fhrs_ids(value: str) -> tuple[list[str], list[str]]:
    tokens = [s.strip() for s in value.split(";") if s.strip()]
    return sorted({s for s in tokens if s.isdigit()}), sorted(
        {s for s in tokens if not s.isdigit()}
    )


def normalise(value: str) -> str:
    value = unicodedata.normalize("NFKD", value.casefold().replace("&", " and "))
    return " ".join(re.findall(r"[a-z0-9]+", value))


def distance(a: Any, b: Any) -> float | None:
    if a is None or b is None:
        return None
    return float(GEOD.inv(a.x, a.y, b.x, b.y)[2])


def write_csv(path: Path, rows: list[dict[str, Any]], fields: list[str]) -> None:
    with path.open("w", encoding="utf-8", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def inspect() -> None:
    manifest = read_json(RAW / "manifest.json")
    require(manifest["city_sha256"] == digest(CITY), "Geography changed since acquisition")
    require(manifest["query_sha256"] == digest(RAW / "query.overpassql"), "Query checksum mismatch")
    for name, record in manifest["artifacts"].items():
        require(record["sha256"] == digest(RAW / name), f"Source checksum mismatch: {name}")
    require(
        manifest["config_sha256"] == digest(CONFIG),
        "Pilot configuration changed; explicitly version the analysis before replay",
    )
    encoded_query = urllib.parse.urlencode(
        {"data": (RAW / "query.overpassql").read_text(encoding="utf-8")}
    ).encode()
    require(
        hashlib.sha256(encoded_query).hexdigest()
        == manifest["artifacts"]["osm.json"]["request_body_sha256"],
        "OSM request body mismatch",
    )
    settings = read_json(CONFIG)
    city = coverage()
    fsa, fsa_metadata = fsa_rows((RAW / "fsa.xml").read_bytes())
    authority = read_json(RAW / "authority.json")
    require(
        len(fsa) == authority["EstablishmentCount"],
        "FSA extract/authority count discrepancy: inspect before accepting",
    )
    elements, osm_metadata = osm_elements((RAW / "osm.json").read_bytes())
    target_types = set(settings["fsa_candidate_types"])
    candidates = [r for r in fsa if r["type"] in target_types]
    fsa_by_id = {r["key"]: r for r in fsa}
    osm = []
    for element in elements:
        point, location, polygon = osm_point(element)
        tags = element.get("tags", {})
        ids, bad = fhrs_ids(tags.get("fhrs:id", ""))
        amenity = set(s.strip() for s in tags.get("amenity", "").split(";"))
        shops = set(s.strip() for s in tags.get("shop", "").split(";"))
        eligible_tag = bool(
            amenity & {"restaurant", "cafe", "fast_food", "pub", "bar"} or "bakery" in shops
        )
        osm.append(
            {
                "key": f"{element['type']}/{element['id']}",
                "element_type": element["type"],
                "name": tags.get("name", ""),
                "postcode": tags.get("addr:postcode", ""),
                "point": point,
                "polygon": polygon,
                "location": location,
                "inside": point is not None and bool(city.covers(point)),
                "venue_tag": eligible_tag,
                "ids": ids,
                "invalid_ids": bad,
                "tags": tags,
            }
        )
    inside = [r for r in osm if r["inside"]]
    venues = [r for r in inside if r["venue_tag"]]
    references: dict[str, list[str]] = defaultdict(list)
    links = []
    for row in osm:
        for ident in row["ids"]:
            references[ident].append(row["key"])
            target = fsa_by_id.get(ident)
            metres = distance(row["point"], target["point"]) if target else None
            links.append(
                {
                    "osm": row["key"],
                    "fhrs_id": ident,
                    "in_city": row["inside"],
                    "in_extract": target is not None,
                    "fsa_type": target["type"] if target else "",
                    "distance_m": round(metres, 2) if metres is not None else "",
                    "name_similarity": round(
                        SequenceMatcher(
                            None, normalise(row["name"]), normalise(target["name"])
                        ).ratio(),
                        3,
                    )
                    if target
                    else "",
                    "review": "explicit_id_candidate_not_accepted",
                }
            )
    osm_by_key = {r["key"]: r for r in osm}
    for link in links:
        row, target = osm_by_key[link["osm"]], fsa_by_id.get(link["fhrs_id"])
        flags = []
        if not row["inside"]:
            flags.append("outside_supported_coverage")
        if not row["venue_tag"]:
            flags.append("no_target_venue_tag")
        if len(row["ids"]) > 1:
            flags.append("multiple_fhrs_ids")
        if len(references[link["fhrs_id"]]) > 1:
            flags.append("repeated_fhrs_reference")
        if target is None:
            flags.append("id_absent_from_current_extract")
        else:
            if target["privacy"]:
                flags.append("location_privacy_hold")
            if target["point"] is None:
                flags.append("missing_fsa_location")
            if target["type"] not in target_types:
                flags.append("exceptional_fsa_type")
            if link["distance_m"] != "" and link["distance_m"] > settings["candidate_radius_m"]:
                flags.append("source_locations_disagree")
            if link["name_similarity"] < 0.5:
                flags.append("source_names_disagree")
        link["flags"] = ";".join(flags)
    # Projected spatial index discovers candidates; final candidate distance is WGS84 geodesic.
    datadir.append_data_dir(str(ROOT / "data/reference/geography"))
    grid_name = "uk_os_OSTN15_NTv2_OSGBtoETRS.tif"
    geo_manifest = read_json(ROOT / "reference/geography/manifest.json")
    require(
        digest(ROOT / "data/reference/geography" / grid_name)
        == geo_manifest["artifacts"][grid_name]["sha256"],
        "Coordinate grid checksum mismatch",
    )
    project = Transformer.from_crs(
        4326, 27700, always_xy=True, allow_ballpark=False, only_best=True
    )
    located = [r for r in candidates if r["point"] is not None and not r["privacy"]]
    points = [Point(*project.transform(r["point"].x, r["point"].y)) for r in located]
    tree = STRtree(points)
    suggestions = []
    for row in venues:
        p = Point(*project.transform(row["point"].x, row["point"].y))
        for index in tree.query(
            p, predicate="dwithin", distance=settings["candidate_radius_m"] + 2
        ):
            target = located[int(index)]
            if target["key"] in row["ids"]:
                continue
            metres = distance(row["point"], target["point"])
            if metres is None or metres > settings["candidate_radius_m"]:
                continue
            left, right = normalise(row["name"]), normalise(target["name"])
            similarity = SequenceMatcher(None, left, right).ratio() if left and right else 0.0
            same_postcode = bool(
                row["postcode"] and normalise(row["postcode"]) == normalise(target["postcode"])
            )
            reason = None
            if similarity >= settings["name_similarity"]:
                reason = "name_and_proximity"
            elif same_postcode and similarity >= settings["postcode_name_similarity"]:
                reason = "postcode_name_and_proximity"
            if reason:
                suggestions.append(
                    {
                        "osm": row["key"],
                        "fhrs_id": target["key"],
                        "distance_m": round(metres, 2),
                        "name_similarity": round(similarity, 3),
                        "same_postcode": same_postcode,
                        "reason": reason,
                        "accepted": False,
                    }
                )
    suggestions.sort(key=lambda r: (r["osm"], -r["name_similarity"], r["distance_m"], r["fhrs_id"]))
    duplicates = []
    for index, a in enumerate(venues):
        for b in venues[index + 1 :]:
            metres = distance(a["point"], b["point"])
            same_ids = bool(set(a["ids"]) & set(b["ids"]))
            same_name = bool(normalise(a["name"]) and normalise(a["name"]) == normalise(b["name"]))
            contains = (a["polygon"] is not None and a["polygon"].covers(b["point"])) or (
                b["polygon"] is not None and b["polygon"].covers(a["point"])
            )
            if same_ids or (
                same_name and metres is not None and metres <= settings["duplicate_radius_m"]
            ):
                duplicates.append(
                    {
                        "osm_a": a["key"],
                        "osm_b": b["key"],
                        "distance_m": round(metres or 0, 2),
                        "same_fhrs_id": same_ids,
                        "same_name": same_name,
                        "node_area_pair": {a["element_type"], b["element_type"]} == {"node", "way"},
                        "point_in_other_polygon": bool(contains),
                        "accepted": False,
                    }
                )
    fsa_inside = [r for r in fsa if r["point"] is not None and city.covers(r["point"])]
    summary = {
        "snapshot": "2026-11-02",
        "parser_version": "venue-discovery-v1",
        "accepted_venues": 0,
        "fsa": {
            "raw_count": len(fsa),
            "header": fsa_metadata,
            "type_counts": dict(Counter(r["type"] + " " + r["type_name"] for r in fsa)),
            "candidate_type_count": len(candidates),
            "candidate_type_counts": dict(Counter(r["type"] for r in candidates)),
            "missing_coordinates": sum(r["point"] is None for r in fsa),
            "candidate_missing_coordinates": sum(r["point"] is None for r in candidates),
            "privacy_marker_count": sum(r["privacy_marker"] for r in fsa),
            "possible_suppression_count": sum(r["possible_suppression"] for r in fsa),
            "inside_city": len(fsa_inside),
            "candidate_inside_city": sum(
                r["point"] is not None and city.covers(r["point"]) for r in candidates
            ),
            "ratings": dict(Counter(r["rating"] for r in fsa)),
            "observed_fields": sorted({k for r in fsa for k in r["fields"]}),
        },
        "osm": {
            "raw_count": len(osm),
            "source": osm_metadata,
            "element_types": dict(Counter(r["element_type"] for r in osm)),
            "inside_city": len(inside),
            "venue_tag_inside_city": len(venues),
            "outside_city": sum(r["point"] is not None and not r["inside"] for r in osm),
            "location_methods": dict(Counter(r["location"] for r in osm)),
            "missing_names_inside_venues": sum(not r["name"] for r in venues),
            "multi_value_ids": sum(len(r["ids"]) > 1 for r in osm),
            "invalid_id_tokens": sum(bool(r["invalid_ids"]) for r in osm),
            "tag_counts_inside_venues": {
                key: dict(Counter(r["tags"].get(key, "<missing>") for r in venues))
                for key in ("amenity", "shop", "cuisine", "access")
            },
        },
        "overlap": {
            "explicit_reference_pairs": len(links),
            "unique_fsa_ids_referenced": len(references),
            "reference_ids_not_in_extract": sorted(set(references) - set(fsa_by_id)),
            "repeated_fsa_references": {k: v for k, v in references.items() if len(v) > 1},
            "candidate_pair_count": len(suggestions),
            "osm_with_fuzzy_candidates": len({r["osm"] for r in suggestions}),
            "suspected_osm_duplicate_pairs": len(duplicates),
            "inside_venue_features_without_fhrs_id": sum(not r["ids"] for r in venues),
            "inside_venue_direct_existing_pairs": sum(
                len(set(r["ids"]) & set(fsa_by_id)) for r in venues
            ),
            "inside_venue_unique_direct_fsa_ids": len(
                {i for r in venues for i in r["ids"] if i in fsa_by_id}
            ),
            "direct_link_flag_counts": dict(
                Counter(flag for r in links for flag in r["flags"].split(";") if flag)
            ),
            "inside_venue_features_no_direct_or_fuzzy_candidate": sum(
                not any(i in fsa_by_id for i in r["ids"])
                and r["key"] not in {s["osm"] for s in suggestions}
                for r in venues
            ),
        },
        "config_sha256": digest(CONFIG),
    }
    linked_venues: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in venues:
        for ident in row["ids"]:
            if ident in fsa_by_id:
                linked_venues[ident].append(row)
    exceptional = [fsa_by_id[i] for i in linked_venues if fsa_by_id[i]["type"] not in target_types]
    triage = [
        {
            "source": "fsa",
            "key": row["key"],
            "fsa_type": row["type"],
            "osm_venue_links": len(linked_venues[row["key"]]),
            "location_hold": row["privacy"] or row["point"] is None,
            "decision": "pending_eligibility_review",
        }
        for row in candidates + exceptional
    ]
    fuzzy_keys = {r["osm"] for r in suggestions}
    unresolved_osm = [
        r
        for r in venues
        if not any(i in fsa_by_id for i in r["ids"]) and r["key"] not in fuzzy_keys
    ]
    triage.extend(
        {
            "source": "osm",
            "key": r["key"],
            "fsa_type": "",
            "osm_venue_links": "",
            "location_hold": False,
            "decision": "pending_eligibility_and_duplicate_review",
        }
        for r in unresolved_osm
    )
    summary["review_workload"] = {
        "fsa_candidate_records": len(candidates),
        "exceptional_fsa_records": len(exceptional),
        "unresolved_osm_features": len(unresolved_osm),
        "triage_rows": len(triage),
        "warning": "Queues overlap; these are not distinct accepted venues or hours of work",
    }
    write_csv(
        RAW / "eligibility-review.csv",
        triage,
        ["source", "key", "fsa_type", "osm_venue_links", "location_hold", "decision"],
    )
    write_csv(
        RAW / "explicit-links.csv",
        links,
        [
            "osm",
            "fhrs_id",
            "in_city",
            "in_extract",
            "fsa_type",
            "distance_m",
            "name_similarity",
            "flags",
            "review",
        ],
    )
    write_csv(
        RAW / "candidate-pairs.csv",
        suggestions,
        ["osm", "fhrs_id", "distance_m", "name_similarity", "same_postcode", "reason", "accepted"],
    )
    write_csv(
        RAW / "duplicate-pairs.csv",
        duplicates,
        [
            "osm_a",
            "osm_b",
            "distance_m",
            "same_fhrs_id",
            "same_name",
            "node_area_pair",
            "point_in_other_polygon",
            "accepted",
        ],
    )
    write_json(RAW / "inspection.json", summary)
    print(
        json.dumps(
            {
                "fsa_raw": len(fsa),
                "fsa_candidate_types": len(candidates),
                "osm_raw": len(osm),
                "osm_inside_venue_features": len(venues),
                "explicit_reference_pairs": len(links),
                "additional_candidate_pairs": len(suggestions),
                "suspected_osm_duplicates": len(duplicates),
            },
            indent=2,
        )
    )
    print("Discovery only: candidates are not accepted matches or final competitor counts.")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("task", choices=["fetch", "check"])
    args = parser.parse_args()
    if args.task == "fetch":
        acquire()
    else:
        inspect()


if __name__ == "__main__":
    main()

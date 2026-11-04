"""Acquire pinned ONS reference products, then validate them offline for plan step 05."""

from __future__ import annotations

import argparse
import csv
import hashlib
import io
import json
import ssl
import urllib.parse
import urllib.request
import zipfile
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

from openpyxl import load_workbook
from pyproj import datadir
from pyproj.transformer import TransformerGroup
from shapely import transform
from shapely.geometry import Point, mapping, shape
from shapely.ops import unary_union

ROOT = Path(__file__).resolve().parents[1]
RAW = ROOT / "data/reference/geography"
OUT = ROOT / "reference/geography"
PIN = OUT / "manifest.json"
LAD = "E06000043"
SAM_LAND = "Land count (Area in Hectares)"
SAM_COAST = "Clipped to the Coastline (Area in Hectares)"
SAM_WATER = "Area of Inland Water (Hectares)"
SAM_FIELDS = [
    "LSOA21CD",
    "LSOA21NM",
    "Extent of the Realm (Area in Hectares)",
    SAM_COAST,
    SAM_WATER,
    SAM_LAND,
    "LTLA22CD",
    "LTLA22NM",
    "LTLA22NMW",
]
POP_FIELDS = [
    "LAD 2021 Code",
    "LAD 2021 Name",
    "LSOA 2021 Code",
    "LSOA 2021 Name",
    "Total",
    "F0 to 15",
    "F16 to 29",
    "F30 to 44",
    "F45 to 64",
    "F65 and over",
    "M0 to 15",
    "M16 to 29",
    "M30 to 44",
    "M45 to 64",
    "M65 and over",
]
ITEMS = {
    "boundary": "2bbaef5230694f3abae4f9145a3a9800",
    "lookup": "b9ca90c10aaa4b8d9791e9859a38ca67",
    "sam": "a488cb8fc9a74accb63cb52961e456ef",
}
SERVICE = "https://services1.arcgis.com/ESMARspQHYMw9BZ9/arcgis/rest/services/"
LOOKUP = SERVICE + "OA_LSOA_MSOA_EW_DEC_2021_LU_v3/FeatureServer/0"
BOUNDARY = (
    SERVICE + "Lower_layer_Super_Output_Areas_December_2021_Boundaries_EW_BFC_V10/FeatureServer/0"
)
POP_URL = (
    "https://www.ons.gov.uk/file?uri=/peoplepopulationandcommunity/populationandmigration/"
    "populationestimates/datasets/lowersuperoutputareamidyearpopulationestimatesnationalstatistics/"
    "mid2022revisednov2025tomid2024/sapelsoabroadage20222024.xlsx"
)
GRID = "uk_os_OSTN15_NTv2_OSGBtoETRS.tif"


def require(condition: bool, message: str) -> None:
    if not condition:
        raise ValueError(message)


def read_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def write_json(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")


def sha(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def query(base: str, **parameters: Any) -> str:
    return base + "/query?" + urllib.parse.urlencode(parameters)


def acquire() -> None:
    """No automatic snapshot replacement: existing pins must match every cached response."""
    RAW.mkdir(parents=True, exist_ok=True)
    records: dict[str, Any] = (
        read_json(RAW / "downloads.json") if (RAW / "downloads.json").exists() else {}
    )
    pins = read_json(PIN)["artifacts"] if PIN.exists() else {}

    def fetch(name: str, url: str) -> Any:
        path = RAW / name
        if path.exists():
            require(name in records, f"Unrecorded artifact: {name}; inspect before replacing")
            data = path.read_bytes()
            require(records[name]["url"] == url, f"Acquisition URL changed: {name}")
            require(sha(data) == records[name]["sha256"], f"Cached checksum mismatch: {name}")
        else:
            request = urllib.request.Request(
                url, headers={"User-Agent": "Mozilla/5.0 UrbanLens-reference-check"}
            )
            with urllib.request.urlopen(
                request, context=ssl.create_default_context(), timeout=90
            ) as response:
                data = response.read(50_000_001)
                require(len(data) <= 50_000_000, f"Unexpected response size: {name}")
                record = {
                    "url": url,
                    "response_url": response.url,
                    "retrieved_at": datetime.now(UTC).isoformat(),
                    "content_type": response.headers.get("Content-Type"),
                    "etag": response.headers.get("ETag"),
                    "last_modified": response.headers.get("Last-Modified"),
                    "bytes": len(data),
                    "sha256": sha(data),
                }
            if name in pins:
                require(
                    sha(data) == pins[name]["sha256"], f"Upstream changed pinned artifact: {name}"
                )
            path.write_bytes(data)
            records[name] = record
            write_json(RAW / "downloads.json", records)
        if name in pins:
            require(sha(data) == pins[name]["sha256"], f"Pin mismatch: {name}")
        if name.endswith(".json") or name.endswith(".geojson"):
            parsed = json.loads(data)
            require("error" not in parsed, f"Provider error: {name}: {parsed.get('error')}")
            return parsed
        return None

    for name, ident in ITEMS.items():
        fetch(
            f"{name}-item.json", f"https://www.arcgis.com/sharing/rest/content/items/{ident}?f=json"
        )
    fetch("lookup-layer.json", LOOKUP + "?f=json")
    fetch("boundary-layer.json", BOUNDARY + "?f=json")
    where = f"LAD22CD='{LAD}'"
    count = fetch("lookup-count.json", query(LOOKUP, f="json", where=where, returnCountOnly="true"))
    rows = fetch(
        "lookup-rows.json",
        query(
            LOOKUP,
            f="json",
            where=where,
            outFields="OA21CD,LSOA21CD,LSOA21NM,LAD22CD",
            returnGeometry="false",
            orderByFields="OA21CD",
            resultRecordCount=1000,
        ),
    )
    require(not rows.get("exceededTransferLimit"), "Lookup truncated; implement bounded pagination")
    require(len(rows["features"]) == count["count"], "Lookup count mismatch")
    codes = sorted({row["attributes"]["LSOA21CD"] for row in rows["features"]})
    require(bool(codes), "Empty target authority")
    for start in range(0, len(codes), 40):
        batch = codes[start : start + 40]
        clause = "LSOA21CD IN (" + ",".join(f"'{code}'" for code in batch) + ")"
        result = fetch(
            f"boundaries-{start // 40}.geojson",
            query(
                BOUNDARY,
                f="geojson",
                where=clause,
                outFields="LSOA21CD,LSOA21NM",
                returnGeometry="true",
                outSR="27700",
                orderByFields="LSOA21CD",
                resultRecordCount=2000,
            ),
        )
        require(not result.get("exceededTransferLimit"), "Boundary batch truncated")
        require(
            sorted(f["properties"]["LSOA21CD"] for f in result["features"]) == batch,
            "Boundary batch missing or duplicate codes",
        )
    fetch("sam.zip", f"https://www.arcgis.com/sharing/rest/content/items/{ITEMS['sam']}/data")
    fetch("population.xlsx", POP_URL)
    fetch(GRID, "https://cdn.proj.org/" + GRID)
    fetch(
        "grid-readme.txt",
        "https://raw.githubusercontent.com/OSGeo/PROJ-data/master/uk_os/uk_os_README.txt",
    )
    if not PIN.exists():
        write_json(
            PIN,
            {
                "schema_version": 1,
                "authority": LAD,
                "geography": "LSOA 2021",
                "boundary": "BFC V10",
                "membership": "Exact Fit V3 / LAD22CD",
                "land_area": "SAM March 2021 EW V2, land hectares / 100",
                "population": "Mid-2022 revised (Nov 2025) to mid-2024; reference 2024-06-30",
                "items": ITEMS,
                "artifacts": records,
            },
        )
    print("Reference artifacts acquired and checksum-verified. Run geography-check next.")


def land_rows(data: bytes, codes: set[str]) -> tuple[dict[str, Decimal], list[str]]:
    """Exact observed supplier schema; no positional or fuzzy header fallback."""
    reader = csv.DictReader(io.StringIO(data.decode("cp1252"), newline=None))
    require(
        reader.fieldnames == SAM_FIELDS,
        "SAM schema differs: expected documented land-count mapping to AREALHECT semantics",
    )
    selected: dict[str, Decimal] = {}
    source_authority_codes: set[str] = set()
    for row in reader:
        code = row["LSOA21CD"]
        if row["LTLA22CD"] == LAD:
            source_authority_codes.add(code)
        if code not in codes:
            continue
        require(code not in selected, f"Duplicate SAM key: {code}")
        require(row["LTLA22CD"] == LAD, f"SAM authority mismatch: {code}")
        land, coast, water = (Decimal(row[key]) for key in (SAM_LAND, SAM_COAST, SAM_WATER))
        require(
            all(v.is_finite() and v >= 0 for v in (land, coast, water)) and land > 0,
            f"Invalid official area: {code}",
        )
        require(abs(coast - water - land) <= Decimal("0.02"), f"SAM land reconciliation: {code}")
        selected[code] = land
    require(set(selected) == codes == source_authority_codes, "SAM target code set mismatch")
    return selected, SAM_FIELDS


def population_bands(row: dict[str, Any]) -> dict[str, int]:
    values = [row[name] for name in POP_FIELDS[4:]]
    require(
        all(
            isinstance(v, (int, float))
            and not isinstance(v, bool)
            and v >= 0
            and float(v).is_integer()
            for v in values
        ),
        "Invalid population cell",
    )
    total = int(row["Total"])
    young = int(row["F0 to 15"] + row["M0 to 15"])
    working = sum(
        int(row[f"{sex}{age}"]) for sex in "FM" for age in ("16 to 29", "30 to 44", "45 to 64")
    )
    older = int(row["F65 and over"] + row["M65 and over"])
    require(total > 0 and young + working + older == total, "Age bands do not reconcile")
    return {"population": total, "age_0_15": young, "age_16_64": working, "age_65_plus": older}


def populations(codes: set[str]) -> tuple[dict[str, dict[str, int]], dict[str, Any]]:
    workbook = load_workbook(RAW / "population.xlsx", read_only=True, data_only=True)
    try:
        sheet = workbook["Mid-2024 LSOA 2021"]
        header = list(next(sheet.iter_rows(min_row=4, max_row=4, values_only=True)))
        require(header == POP_FIELDS, "Population header/edition changed")
        result: dict[str, dict[str, int]] = {}
        authority_codes: set[str] = set()
        for values in sheet.iter_rows(min_row=5, values_only=True):
            row = dict(zip(header, values, strict=True))
            code = row["LSOA 2021 Code"]
            if row["LAD 2021 Code"] == LAD:
                authority_codes.add(code)
            if code not in codes:
                continue
            require(code not in result, f"Duplicate population key: {code}")
            require(row["LAD 2021 Code"] == LAD, f"Population authority mismatch: {code}")
            result[code] = population_bands(row)
        require(set(result) == codes == authority_codes, "Population target code set mismatch")
        notes = [
            [v for v in row if v is not None]
            for row in workbook["Notes"].iter_rows(values_only=True)
        ]
        return result, {
            "sheets": workbook.sheetnames,
            "selected_sheet": sheet.title,
            "header_row": 4,
            "fields": header,
            "notes": notes,
        }
    finally:
        workbook.close()


def verify_pins() -> dict[str, Any]:
    require(PIN.exists(), "No manifest: run geography-fetch first")
    pins: dict[str, Any] = read_json(PIN)
    for name, record in pins["artifacts"].items():
        path = RAW / name
        require(path.is_file(), f"Missing artifact: {name}; run geography-fetch")
        require(sha(path.read_bytes()) == record["sha256"], f"Checksum mismatch: {name}")
    return pins


def validate() -> None:
    pins = verify_pins()
    lookup = read_json(RAW / "lookup-rows.json")
    attributes = [f["attributes"] for f in lookup["features"]]
    require(not lookup.get("exceededTransferLimit"), "Lookup truncated")
    require(len(attributes) == read_json(RAW / "lookup-count.json")["count"], "Lookup incomplete")
    require(len({r["OA21CD"] for r in attributes}) == len(attributes), "Duplicate OA keys")
    require(all(r["LAD22CD"] == LAD for r in attributes), "Lookup authority mismatch")
    codes = {r["LSOA21CD"] for r in attributes}
    names = {r["LSOA21CD"]: r["LSOA21NM"] for r in attributes}
    require(
        all(r["LSOA21NM"] == names[r["LSOA21CD"]] for r in attributes), "Conflicting LSOA names"
    )
    require(
        all(len(c) == 9 and c.startswith("E01") and c[1:].isdigit() for c in codes),
        "Invalid LSOA code",
    )
    layer = read_json(RAW / "boundary-layer.json")
    require(layer["extent"]["spatialReference"]["wkid"] == 27700, "Boundary native CRS changed")
    geometries: dict[str, Any] = {}
    for name in sorted(pins["artifacts"]):
        if not name.startswith("boundaries-"):
            continue
        collection = read_json(RAW / name)
        require(collection["crs"]["properties"]["name"] == "EPSG:27700", "Unexpected boundary CRS")
        require(not collection.get("exceededTransferLimit"), "Boundary response truncated")
        for feature in collection["features"]:
            code = feature["properties"]["LSOA21CD"]
            require(code not in geometries, f"Duplicate geometry key: {code}")
            require(
                feature["properties"]["LSOA21NM"] == names.get(code),
                f"Boundary name mismatch: {code}",
            )
            geometry = shape(feature["geometry"])
            require(
                geometry.geom_type in {"Polygon", "MultiPolygon"}
                and geometry.is_valid
                and not geometry.is_empty
                and geometry.area > 0,
                f"Invalid geometry: {code}",
            )
            geometries[code] = geometry
    require(set(geometries) == codes, "Boundary code set mismatch")
    with zipfile.ZipFile(RAW / "sam.zip") as archive:
        matches = [n for n in archive.namelist() if n.endswith("/SAM_LSOA_DEC_2021_EW.csv")]
        require(len(matches) == 1, "SAM LSOA file ambiguous or missing")
        areas, fields = land_rows(archive.read(matches[0]), codes)
        sam_member = matches[0]
    population, workbook = populations(codes)
    union = unary_union(list(geometries.values()))
    require(union.is_valid and not union.is_empty, "Invalid city union")
    overlap = sum(g.area for g in geometries.values()) - union.area
    require(abs(overlap) < 0.01, f"Overlapping analytical boundaries: {overlap} m2")
    datadir.append_data_dir(str(RAW.resolve()))
    group = TransformerGroup(27700, 4326, always_xy=True)
    require(group.best_available, "OSTN15 grid unavailable; refuse less accurate fallback")
    transformer = group.transformers[0]
    wgs_features = []
    fixtures = []
    for code in sorted(codes):
        geometry = geometries[code]
        wgs = transform(geometry, transformer.transform, interleaved=False)
        require(wgs.is_valid and not wgs.is_empty, f"Invalid WGS84 geometry: {code}")
        require(
            -1 < wgs.bounds[0] < wgs.bounds[2] < 1 and 50 < wgs.bounds[1] < wgs.bounds[3] < 52,
            f"WGS84 axis/range failure: {code}",
        )
        point = geometry.representative_point()
        matches = sorted(c for c, g in geometries.items() if g.covers(point))
        require(matches == [code], f"Interior assignment failure: {code}")
        fixtures.append(
            {
                "kind": "interior",
                "expected_lsoa": code,
                "point_epsg27700": list(point.coords[0]),
                "point_wgs84": list(transformer.transform(point.x, point.y)),
            }
        )
        wgs_features.append(
            {
                "type": "Feature",
                "properties": {"LSOA21CD": code, "LSOA21NM": names[code]},
                "geometry": mapping(wgs),
            }
        )
    # Use a source vertex, avoiding interpolation/round-trip drift at an exact boundary.
    outer = union.exterior if union.geom_type == "Polygon" else union.geoms[0].exterior
    vertex = Point(outer.coords[0])
    require(
        union.covers(vertex) and not union.contains(vertex),
        "Inclusive outer boundary fixture failed",
    )
    covering = sorted(c for c, g in geometries.items() if g.covers(vertex))
    require(bool(covering), "Outer boundary has no area")
    fixtures.append(
        {
            "kind": "outer_boundary",
            "point_epsg27700": list(vertex.coords[0]),
            "expected_lsoa": covering[0],
            "expected_covered": True,
        }
    )
    ocean = Point(530000, 101000)
    require(not union.covers(ocean), "Expected outside coastal fixture changed")
    fixtures.append(
        {
            "kind": "outside_coast",
            "point_epsg27700": list(ocean.coords[0]),
            "point_wgs84": list(transformer.transform(ocean.x, ocean.y)),
            "expected_covered": False,
        }
    )
    # A boundary-centred circle crosses city coverage; never allocate radius demographics.
    require(not union.covers(vertex.buffer(500)), "City-edge coverage fixture failed")
    OUT.mkdir(parents=True, exist_ok=True)
    with (OUT / "target-areas.csv").open("w", encoding="utf-8", newline="") as stream:
        writer = csv.DictWriter(
            stream,
            fieldnames=[
                "LSOA21CD",
                "LSOA21NM",
                "LAD22CD",
                "land_hectares",
                "land_km2",
                "population",
                "age_0_15",
                "age_16_64",
                "age_65_plus",
            ],
        )
        writer.writeheader()
        for code in sorted(codes):
            writer.writerow(
                {
                    "LSOA21CD": code,
                    "LSOA21NM": names[code],
                    "LAD22CD": LAD,
                    "land_hectares": str(areas[code]),
                    "land_km2": str(areas[code] / 100),
                    **population[code],
                }
            )
    write_json(RAW / "areas-wgs84.geojson", {"type": "FeatureCollection", "features": wgs_features})
    write_json(
        RAW / "city-coverage-27700.geojson",
        {
            "type": "Feature",
            "geometry": mapping(union),
            "properties": {"authority": LAD, "srid": 27700},
        },
    )
    write_json(
        RAW / "city-coverage-wgs84.geojson",
        {
            "type": "Feature",
            "geometry": mapping(transform(union, transformer.transform, interleaved=False)),
            "properties": {"authority": LAD, "srid": 4326},
        },
    )
    write_json(
        OUT / "spatial-fixtures.json",
        {
            "assignment": "covers; lexicographically smallest LSOA code on ties",
            "fixtures": fixtures,
        },
    )
    summary = {
        "authority": LAD,
        "lsoa_count": len(codes),
        "oa_count": len(attributes),
        "land_hectares": str(sum(areas.values())),
        "land_km2": str(sum(areas.values()) / 100),
        "population_totals_unrounded_for_validation_only": {
            k: sum(row[k] for row in population.values())
            for k in ("population", "age_0_15", "age_16_64", "age_65_plus")
        },
        "boundary_native_srid": 27700,
        "storage_srid": 4326,
        "invalid_geometries": 0,
        "overlap_m2": overlap,
        "city_geometry_type": union.geom_type,
        "sam_member": sam_member,
        "sam_encoding": "cp1252",
        "sam_fields": fields,
        "sam_land_mapping": {
            "provider_field": SAM_LAND,
            "semantic_field": "AREALHECT",
            "unit": "hectares",
        },
        "workbook": workbook,
        "transform": transformer.description,
        "transform_reported_accuracy_metres": transformer.accuracy,
        "outputs": {
            name: sha((RAW / name).read_bytes())
            for name in (
                "areas-wgs84.geojson",
                "city-coverage-27700.geojson",
                "city-coverage-wgs84.geojson",
            )
        },
    }
    write_json(OUT / "inspection.json", summary)
    print(
        f"Validated {len(codes)} LSOAs from {len(attributes)} OAs; "
        f"official land {summary['land_km2']} km2."
    )
    print("All target geometry/land/population code sets match; all age bands reconcile.")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("task", choices=["fetch", "check"])
    args = parser.parse_args()
    if args.task == "fetch":
        acquire()
    else:
        validate()


if __name__ == "__main__":
    main()

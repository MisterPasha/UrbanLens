"""Public projections and deterministic, checksum-verified reuse archives."""

from __future__ import annotations

import csv
import hashlib
import io
import json
import re
import zipfile
from typing import Any


def encoded(value: Any) -> bytes:
    return (
        json.dumps(value, sort_keys=True, ensure_ascii=False, separators=(",", ":")) + "\n"
    ).encode()


def sha(body: bytes) -> str:
    return hashlib.sha256(body).hexdigest()


def venue(row: dict[str, Any]) -> dict[str, Any]:
    withheld = row["location_quality"] == "withheld"
    result = {
        k: v
        for k, v in row.items()
        if k
        not in {
            "location",
            "resolution_artifact_id",
            "sources",
            "provenance",
            "verified_operators",
        }
    }
    for key in ("address", "postcode", "area_code", "longitude", "latitude"):
        if withheld:
            result[key] = None
    result["sources"] = []
    for source in row["sources"]:
        provider, key = source["provider"], source["key"]
        url = (
            "https://ratings.food.gov.uk/business/" + key
            if provider == "fsa"
            else "https://www.openstreetmap.org/" + key
            if provider == "osm"
            else None
        )
        result["sources"].append(
            dict(
                provider=provider,
                source_key=key,
                generation=source.get("generation"),
                snapshot_id=source.get("snapshot_id"),
                record_hash=source.get("record_hash"),
                observed_at=source.get("observed_at"),
                state=source.get("state"),
                url=url,
            )
        )
    result["provenance"] = {}
    result["null_reasons"] = dict(
        name="source_name_unavailable" if row.get("name_unavailable") else None,
        location=row["location_quality"] if row.get("longitude") is None else None,
        address=("withheld" if withheld else "unavailable") if row.get("address") is None else None,
    )
    for key, value in row["provenance"].items():
        hidden = withheld and key in {"address", "postcode", "location", "area_code"}
        result["provenance"][key] = dict(
            rule_version=value.get("rule_version"),
            source_time=value.get("source_time"),
            reason="Source withheld" if hidden else value.get("reason"),
            reviewed=value.get("review_id") is not None,
            value=None if hidden else (value.get("detail") or {}).get("value"),
        )
    result["verified_companies"] = [
        dict(
            company_number=v["company_number"],
            relationship=v["relationship"],
            reviewed_at=v.get("reviewed_at"),
            reviewed_on=v.get("reviewed_on"),
            url=(
                "https://find-and-update.company-information.service.gov.uk/company/"
                + v["company_number"]
            ),
        )
        for v in row["verified_operators"]
    ]
    return result


def company(row: dict[str, Any]) -> dict[str, Any]:
    fields = (
        "release_id company_number legal_name legal_status company_type sic_codes "
        "incorporation_date "
        "cessation_date registered_office registered_office_postcode office_caveat "
        "first_observed_at last_observed_at profile_observed_at"
    ).split()
    result = {k: row[k] for k in fields}
    result["cohorts"] = sorted(v["cohort"] for v in row["cohorts"])
    result["url"] = (
        "https://find-and-update.company-information.service.gov.uk/company/"
        + row["company_number"]
    )
    return result


FORBIDDEN = {
    "officers",
    "psc",
    "pscs",
    "email",
    "phone",
    "telephone",
    "user",
    "uid",
    "username",
    "notes",
    "reviewer",
    "retained_evidence_uri",
    "source_artifact_id",
    "normalised_artifact",
    "evidence_uri",
    "raw_record_locator",
    "search_history",
    "password",
    "secret",
    "api_key",
}


def inspect(value: Any) -> None:
    if isinstance(value, dict):
        if FORBIDDEN & {k.lower() for k in value}:
            raise ValueError("Private or unreviewed nested fields in reuse data")
        for key, item in value.items():
            if key in {"postcode", "registered_office_postcode", "postal_code"} and isinstance(
                item, str
            ):
                if item.replace(" ", "").upper().startswith("BT"):
                    raise ValueError("Northern Ireland postcode outside publication policy")
            inspect(item)
    elif isinstance(value, list):
        for item in value:
            inspect(item)
    elif isinstance(value, str):
        if re.search(
            r"s3://|file://|postgres(?:ql)?://|(?:AKIA|ASIA)[A-Z0-9]{16}|gh[pousr]_[A-Za-z0-9]{36,}",
            value,
        ):
            raise ValueError("Private path or credential pattern in reuse data")


def archive(
    tables: dict[str, list[dict[str, Any]]],
    manifest: dict[str, Any],
    extras: dict[str, bytes],
) -> bytes:
    files = dict(extras)
    schema = {}
    for name, rows in sorted(tables.items()):
        inspect(rows)
        rows = sorted(rows, key=lambda r: encoded(r))
        columns = sorted({k for row in rows for k in row})
        if name == "mart_company_link" and not columns:
            columns = (
                "release_id link_id company_number establishment_id relationship valid_from "
                "review_id reviewed_at reviewed_on primary_spatial_eligible public_evidence_url"
            ).split()
        schema[name] = dict(
            columns=columns, rows=len(rows), nested_csv_values="JSON", null_csv="\\N"
        )
        types = {
            type(None): "null",
            bool: "boolean",
            int: "integer",
            float: "number",
            str: "string",
            dict: "object",
            list: "array",
        }
        schema[name]["json_schema"] = {
            "$schema": "https://json-schema.org/draft/2020-12/schema",
            "type": "object",
            "properties": {
                k: {"type": sorted({types[type(r.get(k))] for r in rows})} if rows else {}
                for k in columns
            },
            "required": columns,
            "additionalProperties": False,
        }
        files[name + ".jsonl"] = b"".join(encoded(row) for row in rows)
        stream = io.StringIO(newline="")
        writer = csv.writer(stream, lineterminator="\n")
        writer.writerow(columns)
        for row in rows:
            writer.writerow(
                [
                    "\\N"
                    if row.get(k) is None
                    else json.dumps(row[k], ensure_ascii=False, sort_keys=True)
                    if isinstance(row[k], (dict, list, bool))
                    else row[k]
                    for k in columns
                ]
            )
        files[name + ".csv"] = stream.getvalue().encode()
    files["schema.json"] = encoded(schema)
    files["manifest.json"] = encoded(manifest)
    files["SHA256SUMS"] = "".join(f"{sha(v)}  {k}\n" for k, v in sorted(files.items())).encode()
    output = io.BytesIO()
    with zipfile.ZipFile(output, "w", compression=zipfile.ZIP_DEFLATED, compresslevel=9) as zipped:
        for name, body in sorted(files.items()):
            info = zipfile.ZipInfo(name, date_time=(2026, 1, 1, 0, 0, 0))
            info.compress_type = zipfile.ZIP_DEFLATED
            info.external_attr = 0o100644 << 16
            zipped.writestr(info, body, compresslevel=9)
    verify(output.getvalue())
    return output.getvalue()


def verify(body: bytes) -> dict[str, Any]:
    with zipfile.ZipFile(io.BytesIO(body)) as zipped:
        names = zipped.namelist()
        if len(names) != len(set(names)) or any("/" in n or "\\" in n for n in names):
            raise ValueError("Unexpected archive paths")
        checked = set()
        for line in zipped.read("SHA256SUMS").decode().splitlines():
            expected, name = line.split("  ", 1)
            if sha(zipped.read(name)) != expected:
                raise ValueError("Reuse member checksum mismatch")
            checked.add(name)
        if checked != set(names) - {"SHA256SUMS"}:
            raise ValueError("Incomplete reuse checksum inventory")
        for name in names:
            if name.endswith(".jsonl"):
                for content_line in zipped.read(name).splitlines():
                    inspect(json.loads(content_line))
        manifest = dict(json.loads(zipped.read("manifest.json")))
        for name, record in json.loads(zipped.read("schema.json")).items():
            rows = [json.loads(line) for line in zipped.read(name + ".jsonl").splitlines()]
            csv_rows = list(csv.reader(io.StringIO(zipped.read(name + ".csv").decode())))
            if len(rows) != record["rows"] or len(csv_rows) != record["rows"] + 1:
                raise ValueError("CSV/JSONL row counts disagree")
            if csv_rows[0] != record["columns"]:
                raise ValueError("CSV headers differ from schema")
            for row, cells in zip(rows, csv_rows[1:], strict=True):
                if len(cells) != len(record["columns"]):
                    raise ValueError("CSV column count differs")
                for key, cell in zip(record["columns"], cells, strict=True):
                    value = row.get(key)
                    agrees = (
                        cell == "\\N"
                        if value is None
                        else json.loads(cell) == value
                        if isinstance(value, (dict, list, bool))
                        else cell == str(value)
                    )
                    if not agrees:
                        raise ValueError("CSV/JSONL contents disagree")
        inspect(manifest)
        geometry = json.loads(zipped.read("areas.geojson")) if "areas.geojson" in names else None
        if geometry:
            areas = [json.loads(line) for line in zipped.read("dim_area.jsonl").splitlines()]
            expected_features = [
                dict(
                    type="Feature",
                    geometry=r["geometry"],
                    properties={k: v for k, v in r.items() if k != "geometry"},
                )
                for r in areas
            ]
            if sorted(map(encoded, geometry["features"])) != sorted(
                map(encoded, expected_features)
            ):
                raise ValueError("Area geometry contents differ")
        return manifest

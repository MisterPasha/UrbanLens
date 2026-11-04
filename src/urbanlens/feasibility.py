"""Isolated step 08 measurements; these outputs are not accepted production models."""

from __future__ import annotations

import csv
import gzip
import hashlib
import io
import json
import re
import time
import zipfile
from pathlib import Path
from typing import Any

RULE = "registered-office-sic-v1"
SICS = {"56101", "56102", "56103", "56302", "10710", "47240"}


def require(condition: bool, message: str) -> None:
    if not condition:
        raise ValueError(message)


def file_sha(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        while chunk := stream.read(1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def peak_rss(*, children: bool = False) -> int | None:
    """Process lifetime high-water RSS, not an isolated stage allocation."""
    try:
        import importlib

        resource: Any = importlib.import_module("resource")
        scope = resource.RUSAGE_CHILDREN if children else resource.RUSAGE_SELF
        return int(resource.getrusage(scope).ru_maxrss * 1024)
    except ImportError:
        return None  # Linux container is the authoritative memory measurement.


def company_partition(source: Path, part: int) -> tuple[dict[str, Any], bytes]:
    require(part in range(1, 8), "Expected one of seven pinned parts")
    started = time.perf_counter()
    manifest = json.loads((source / "manifest.json").read_text(encoding="utf-8"))
    name = f"BasicCompanyData-2026-11-01-part{part}_7.zip"
    input_sha = file_sha(source / name)
    require(input_sha == manifest[name]["sha256"], "Partition source checksum mismatch")
    inspection = json.loads((source / "bulk-inspection.json").read_text(encoding="utf-8"))
    outcodes = set(inspection["official_local_outcodes"])
    selected = []
    count = matches = 0
    with zipfile.ZipFile(source / name) as archive:
        members = archive.infolist()
        require(len(members) == 1 and members[0].filename.endswith(".csv"), "Invalid ZIP")
        with (
            archive.open(members[0]) as binary,
            io.TextIOWrapper(binary, encoding="utf-8-sig", newline="") as stream,
        ):
            reader = csv.DictReader(stream)
            require(
                list(reader.fieldnames or []) == inspection["schema_original"],
                "Unexpected partition schema",
            )
            reader.fieldnames = [name.strip() for name in reader.fieldnames or []]
            for row in reader:
                require(
                    None not in row and all(v is not None for v in row.values()),
                    "Malformed company CSV row",
                )
                count += 1
                codes = []
                for index in range(1, 5):
                    match = re.match(r"^(\d{5})(?:\s|$)", row[f"SICCode.SicText_{index}"].strip())
                    if match and match[1] not in codes:
                        codes.append(match[1])
                if not set(codes) & SICS:
                    continue
                matches += 1
                pc = "".join(row["RegAddress.PostCode"].upper().split())
                if not pc or pc[:-3] not in outcodes:
                    continue
                number = row["CompanyNumber"].strip()
                require(bool(re.fullmatch(r"[A-Z0-9]{8}", number)), "Invalid company number")
                selected.append(
                    {
                        "company_number": number,
                        "company_name": row["CompanyName"].strip(),
                        "status": row["CompanyStatus"].strip(),
                        "postcode": pc,
                        "sic_codes": codes,
                        "source_part": part,
                    }
                )
    expected = inspection["parts"][part - 1]
    require(
        count == expected["rows"] and matches == expected["sic_matches"],
        "Partition counts differ from validated snapshot",
    )
    body = b"".join((json.dumps(row, sort_keys=True) + "\n").encode() for row in selected)
    artifact = gzip.compress(body, mtime=0)
    return {
        "part": part,
        "total_parts": 7,
        "edition": "2026-11-01",
        "rule_version": RULE,
        "input_sha256": input_sha,
        "rows": count,
        "sic_matches": matches,
        "candidate_rows": len(selected),
        "artifact_sha256": hashlib.sha256(artifact).hexdigest(),
        "artifact_bytes": len(artifact),
        "input_bytes": (source / name).stat().st_size,
        "scratch_bytes": 0,
        "seconds": round(time.perf_counter() - started, 4),
        "process_peak_rss_bytes": peak_rss(),
    }, artifact


def reconcile_partitions(records: list[dict[str, Any]]) -> dict[str, Any]:
    """Fail closed: the coordinator cannot accept partial, duplicate or mixed results."""
    require(
        len(records) == 7 and sorted(r["part"] for r in records) == list(range(1, 8)),
        "Incomplete or duplicate partition set",
    )
    require(
        all(
            r["edition"] == "2026-11-01" and r["rule_version"] == RULE and r["total_parts"] == 7
            for r in records
        ),
        "Mixed partition versions",
    )
    return {
        "complete": True,
        "parts": 7,
        "rows": sum(r["rows"] for r in records),
        "candidate_rows": sum(r["candidate_rows"] for r in records),
        "artifact_bytes": sum(r["artifact_bytes"] for r in records),
        "max_partition_seconds": max(r["seconds"] for r in records),
        "sum_partition_seconds": sum(r["seconds"] for r in records),
    }

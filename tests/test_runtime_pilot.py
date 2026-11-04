"""Meaningful partition-completeness, replay and Lambda-adapter contracts."""

import asyncio
import csv
import gzip
import io
import json
import zipfile
from pathlib import Path
from typing import Any

import pytest

from urbanlens.feasibility import RULE, company_partition, file_sha, reconcile_partitions
from urbanlens.lambda_handlers import api_handler, pipeline_handler


def records() -> list[dict[str, Any]]:
    return [
        {
            "part": i,
            "total_parts": 7,
            "edition": "2026-11-01",
            "rule_version": RULE,
            "rows": 10,
            "candidate_rows": 1,
            "artifact_bytes": 20,
            "seconds": 2,
        }
        for i in range(1, 8)
    ]


@pytest.mark.parametrize("mode", ["missing", "duplicate", "edition", "rule"])
def test_coordinator_rejects_incomplete_or_incompatible_partitions(mode: str) -> None:
    rows = records()
    if mode == "missing":
        rows.pop()
    elif mode == "duplicate":
        rows[-1]["part"] = 1
    elif mode == "edition":
        rows[-1]["edition"] = "2026-10-01"
    else:
        rows[-1]["rule_version"] = "other"
    with pytest.raises(ValueError):
        reconcile_partitions(rows)


def test_coordinator_requires_whole_collection() -> None:
    result = reconcile_partitions(records())
    assert result["complete"] and result["rows"] == 70 and result["candidate_rows"] == 7


def synthetic_partition(tmp_path: Path) -> Path:
    fields = [" CompanyNumber", "CompanyName", "CompanyStatus", "RegAddress.PostCode"]
    fields += [f"SICCode.SicText_{i}" for i in range(1, 5)]
    buffer = io.StringIO(newline="")
    writer = csv.writer(buffer)
    writer.writerow(fields)
    writer.writerow(["00000001", "Example", "Active", "BN1 1JE", "99999", "", "56101 - Cafe", ""])
    writer.writerow(["SC000002", "Outside", "Active", "SW1A 1AA", "56302 - Pub", "", "", ""])
    writer.writerow(["00000003", "Other", "Active", "BN1 1JE", "99999", "", "", ""])
    name = "BasicCompanyData-2026-11-01-part1_7.zip"
    path = tmp_path / name
    with zipfile.ZipFile(path, "w") as archive:
        archive.writestr("source.csv", buffer.getvalue())
    (tmp_path / "manifest.json").write_text(json.dumps({name: {"sha256": file_sha(path)}}))
    (tmp_path / "bulk-inspection.json").write_text(
        json.dumps(
            {
                "schema_original": fields,
                "official_local_outcodes": ["BN1"],
                "parts": [{"rows": 3, "sic_matches": 2}],
            }
        )
    )
    return path


def test_partition_replay_keeps_identity_all_sics_and_deterministic_artifact(
    tmp_path: Path,
) -> None:
    synthetic_partition(tmp_path)
    first, artifact = company_partition(tmp_path, 1)
    second, replay = company_partition(tmp_path, 1)
    row = json.loads(gzip.decompress(artifact))
    assert row["company_number"] == "00000001"
    assert row["sic_codes"] == ["99999", "56101"]
    assert first["rows"] == 3 and first["sic_matches"] == 2 and first["candidate_rows"] == 1
    assert artifact == replay and first["artifact_sha256"] == second["artifact_sha256"]


def test_partition_rejects_changed_source(tmp_path: Path) -> None:
    path = synthetic_partition(tmp_path)
    path.write_bytes(path.read_bytes() + b"changed")
    with pytest.raises(ValueError, match="checksum"):
        company_partition(tmp_path, 1)


@pytest.mark.parametrize("part", [0, 8, True, "1"])
def test_handler_rejects_invalid_partition_without_io(part: Any) -> None:
    with pytest.raises(ValueError):
        pipeline_handler({"task": "company-part", "part": part}, None)


def test_real_asgi_adapter_health_event() -> None:
    event = {
        "version": "2.0",
        "rawPath": "/health",
        "rawQueryString": "",
        "headers": {"host": "localhost"},
        "requestContext": {
            "http": {"method": "GET", "path": "/health", "sourceIp": "127.0.0.1"},
            "stage": "$default",
        },
        "isBase64Encoded": False,
    }
    context: Any = object()
    with asyncio.Runner() as runner:
        runner.get_loop()
        result = api_handler(event, context)
    assert result["statusCode"] == 200
    assert json.loads(result["body"])["status"] == "ok"

"""Lambda entry points: existing health API and a bounded discovery-only pipeline."""

from __future__ import annotations

import base64
import importlib
import json
import os
import tempfile
import time
import urllib.request
from pathlib import Path
from typing import Any

from mangum import Mangum

from urbanlens.api.app import app
from urbanlens.feasibility import company_partition, file_sha, require

api_handler = Mangum(app, lifespan="off")


def pipeline_handler(event: dict[str, Any], context: Any) -> dict[str, Any]:
    if event.get("task") == "database-neon":
        return cloud_database()
    if event.get("task") == "database-local":
        from urbanlens.feasibility_db import benchmark

        work = Path("/tmp/urbanlens-db")
        work.mkdir(exist_ok=True)
        return benchmark(Path("/inputs"), os.environ["URBANLENS_FEASIBILITY_DATABASE_URL"], work)
    require(
        event.get("task") in {"company-part", "company-s3-part"}, "Unsupported feasibility task"
    )
    part = event.get("part")
    require(isinstance(part, int) and not isinstance(part, bool), "Integer part required")
    assert isinstance(part, int)
    if event["task"] == "company-s3-part":
        return cloud_partition(part)
    root = Path(os.environ.get("URBANLENS_PILOT_INPUT", "/inputs"))
    metrics, artifact = company_partition(root / "pilots/companies/2026-11-02", part)
    # Small local-emulator transport only. Production partitions will persist private S3 artifacts.
    return {"metrics": metrics, "artifact_base64": base64.b64encode(artifact).decode()}


def cloud_partition(part: int) -> dict[str, Any]:
    """Pilot-only S3 transport: each invocation downloads and processes one pinned ZIP."""
    require(part in range(1, 8), "Expected a pinned part 1 to 7")
    bucket = os.environ["URBANLENS_PILOT_BUCKET"]
    s3: Any = importlib.import_module("boto3").client("s3")
    started = time.perf_counter()
    with tempfile.TemporaryDirectory(prefix="urbanlens-part-") as temporary:
        source = Path(temporary)
        for name in ("manifest.json", "bulk-inspection.json"):
            content = s3.get_object(Bucket=bucket, Key="bootstrap/" + name)["Body"].read(200_001)
            require(len(content) <= 200_000, "Oversized bootstrap metadata")
            (source / name).write_bytes(content)
        name = f"BasicCompanyData-2026-11-01-part{part}_7.zip"
        url = "https://download.companieshouse.gov.uk/" + name
        size = 0
        fetch_start = time.perf_counter()
        with (
            urllib.request.urlopen(url, timeout=45) as response,
            (source / name).open("wb") as file,
        ):
            while chunk := response.read(1024 * 1024):
                size += len(chunk)
                require(size <= 120_000_000, "Bulk download exceeded pilot byte budget")
                file.write(chunk)
        fetched_seconds = time.perf_counter() - fetch_start
        metrics, artifact = company_partition(source, part)
        # Same keys/content hashes make retried writes idempotent for this fixed pilot edition.
        s3.upload_file(str(source / name), bucket, "raw/" + name)
        key = f"filtered/{metrics['artifact_sha256']}.jsonl.gz"
        s3.put_object(Bucket=bucket, Key=key, Body=artifact, ContentType="application/gzip")
        metrics.update(
            download_seconds=fetched_seconds,
            end_to_end_seconds=time.perf_counter() - started,
            scratch_bytes=size,
            output_key=key,
        )
        s3.put_object(
            Bucket=bucket,
            Key=f"results/part-{part}.json",
            Body=json.dumps(metrics).encode(),
            ContentType="application/json",
        )
        return metrics


def cloud_database() -> dict[str, Any]:
    from psycopg.conninfo import conninfo_to_dict

    from urbanlens.feasibility_db import benchmark, dump_probe

    bucket = os.environ["URBANLENS_PILOT_BUCKET"]
    s3: Any = importlib.import_module("boto3").client("s3")
    allowed = {
        "pilots/venues/2026-11-02/fsa.xml",
        "pilots/venues/2026-11-02/osm.json",
        "reference/geography/areas-wgs84.geojson",
        "pilots/companies/2026-11-02/registered-office-cohort.csv",
    }
    content = s3.get_object(Bucket=bucket, Key="bootstrap/dataset-manifest.json")["Body"].read(
        20_001
    )
    require(len(content) <= 20_000, "Oversized dataset manifest")
    expected = json.loads(content)
    require(set(expected) == allowed, "Unexpected dataset bootstrap files")
    with tempfile.TemporaryDirectory(prefix="urbanlens-db-") as temporary:
        root = Path(temporary)
        inputs = root / "inputs"
        for name in sorted(allowed):
            path = inputs / name
            path.parent.mkdir(parents=True, exist_ok=True)
            data = s3.get_object(Bucket=bucket, Key="bootstrap/data/" + name)["Body"].read(
                10_000_001
            )
            require(len(data) <= 10_000_000, "Oversized dataset fixture")
            path.write_bytes(data)
            require(file_sha(path) == expected[name], "Dataset bootstrap checksum mismatch")
        dsn = os.environ["URBANLENS_FEASIBILITY_DATABASE_URL"]
        try:
            metrics = benchmark(inputs, dsn, root)
            backup, path = dump_probe(dsn, root)
            backup["sha256"] = file_sha(path)
            s3.upload_file(str(path), bucket, "results/codebuild-probe.dump")
            metrics["backup"] = backup
        except Exception:
            text = "\n".join(p.read_text(errors="replace") for p in root.rglob("run.txt"))
            password = str(conninfo_to_dict(dsn).get("password") or "")
            text = text.replace(dsn, "[redacted connection]")
            if password:
                text = text.replace(password, "[redacted]")
            if text:
                s3.put_object(
                    Bucket=bucket,
                    Key="results/dbt-failure.txt",
                    Body=text[-60_000:].encode(),
                    ContentType="text/plain",
                )
            raise
        metrics["dataset_bytes"] = sum(p.stat().st_size for p in inputs.rglob("*") if p.is_file())
        return metrics

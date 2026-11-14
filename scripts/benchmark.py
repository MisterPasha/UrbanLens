"""Small read-only benchmark of the deployed private API: 10 visitors, 80 requests."""

from __future__ import annotations

import base64
import json
import math
import re
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime
from typing import Any
from urllib.parse import urlencode

import boto3
from botocore.config import Config
from botocore.exceptions import BotoCoreError, ClientError

from scripts.dev import ROOT
from scripts.production import client

FUNCTION = "urbanlens-production-api"


def percentile(values: list[float], fraction: float = 0.95) -> float | None:
    return round(sorted(values)[math.ceil(len(values) * fraction) - 1], 2) if values else None


def main() -> None:
    started = time.monotonic()
    directory = ROOT / "data/benchmarks" / datetime.now(UTC).strftime("%Y-%m-%d-%H%M%S")
    directory.mkdir(parents=True)
    # Configure the existing CA bundle, then explicitly disable SDK invocation retries.
    configuration = client("lambda").get_function_configuration(FunctionName=FUNCTION)
    sdk = boto3.Session(profile_name="urbanlens_dev", region_name="eu-west-2").client(
        "lambda",
        config=Config(
            connect_timeout=5,
            read_timeout=15,
            max_pool_connections=10,
            retries={"total_max_attempts": 1},
        ),
    )
    records: list[dict[str, Any]] = []
    lock = threading.Lock()

    def invoke(
        name: str,
        path: str,
        parameters: dict[str, Any],
        reader: int,
        phase: str,
        release: str | None = None,
    ) -> dict[str, Any]:
        event = dict(
            version="2.0",
            routeKey="GET /{proxy+}",
            rawPath=path,
            rawQueryString=urlencode(parameters, doseq=True),
            headers={"host": "private.urbanlens"},
            requestContext=dict(
                http=dict(
                    method="GET", path=path, sourceIp=f"192.0.2.{reader + 1}", protocol="HTTP/1.1"
                ),
                stage="$default",
                requestId=f"benchmark-{reader}-{name}",
                domainName="private.urbanlens",
            ),
            isBase64Encoded=False,
        )
        began = time.monotonic()
        row: dict[str, Any] = dict(name=name, reader=reader, phase=phase)
        body: dict[str, Any] = {}
        try:
            response = sdk.invoke(
                FunctionName=FUNCTION,
                InvocationType="RequestResponse",
                LogType="Tail",
                Payload=json.dumps(event).encode(),
            )
            envelope = json.loads(response["Payload"].read())
            row["status"] = envelope.get("statusCode", 502)
            row["function_error"] = bool(response.get("FunctionError"))
            body = json.loads(envelope.get("body", "{}"))
            row["bytes"] = len(envelope.get("body", "").encode())
            row["release_matches"] = release is None or body.get("release_id") == release
            tail = base64.b64decode(response.get("LogResult", "")).decode(errors="replace")
            for field, pattern in (
                ("lambda_ms", r"REPORT[^\n]*?\bDuration: ([\d.]+) ms"),
                ("billed_ms", r"Billed Duration: ([\d.]+) ms"),
                ("init_ms", r"Init Duration: ([\d.]+) ms"),
                ("app_ms", r'"duration_ms":\s*([\d.]+)'),
            ):
                match = re.search(pattern, tail)
                row[field] = float(match[1]) if match else None
            row["cold"] = row["init_ms"] is not None
            row["logs_available"] = "REPORT" in tail
        except ClientError as error:
            code = error.response["Error"]["Code"]
            row.update(
                status=429 if code == "TooManyRequestsException" else 502,
                error_code=code,
                cold=None,
                logs_available=False,
                release_matches=False,
            )
        except BotoCoreError as transport_error:
            row.update(
                status=502,
                error_code=type(transport_error).__name__,
                cold=None,
                logs_available=False,
                release_matches=False,
            )
        row["client_ms"] = round((time.monotonic() - began) * 1000, 2)
        row["success"] = (
            row["status"] == 200 and row["release_matches"] and not row.get("function_error")
        )
        with lock:
            records.append(row)
            with (directory / "requests.jsonl").open("a", encoding="utf-8") as output:
                output.write(json.dumps(row) + "\n")
        return body

    # The first real read is retained separately; no database warm-up precedes it.
    metadata = invoke("first_metadata", "/api/v1/meta", {}, 0, "initial")
    if "release_id" not in metadata:
        time.sleep(1)
        metadata = invoke("metadata_retry", "/api/v1/meta", {}, 0, "setup")
    release = metadata["release_id"]
    pin = dict(release_id=release)
    areas = invoke("areas", "/api/v1/areas", pin, 0, "setup", release)["data"]
    codes = [r["area_code"] for r in areas[:2]]
    sample = invoke("sample", "/api/v1/establishments", {**pin, "limit": 20}, 0, "setup", release)[
        "data"
    ]
    venues = [r["establishment_id"] for r in sample["items"]]
    gate = threading.Barrier(10)

    def visitor(reader: int) -> None:
        gate.wait(timeout=10)
        time.sleep(reader * 0.5)  # Roughly two requests/second, matching the planned gateway rate.
        filters = {**pin, "limit": 20, "map_limit": 500}
        if reader % 3:
            filters["categories"] = "cafe" if reader % 3 == 1 else "restaurant"
        first: dict[str, Any] = {}
        for step, name in enumerate(
            ("metadata", "explore", "page", "radius", "compare", "detail", "companies", "activity")
        ):
            if time.monotonic() - started > 120:
                raise TimeoutError("Bounded benchmark exceeded two minutes")
            if name == "metadata":
                path, params = "/api/v1/meta", pin
            elif name in {"explore", "page"}:
                path, params = "/api/v1/establishments", dict(filters)
                if name == "page":
                    cursor = first.get("data", {}).get("next_cursor")
                    if not cursor:
                        raise ValueError(
                            "No valid pagination cursor; benchmark cannot fake page two"
                        )
                    params["cursor"] = cursor
            elif name == "radius":
                path, params = (
                    "/api/v1/establishments",
                    dict(
                        **pin,
                        mode="radius",
                        longitude=-0.14,
                        latitude=50.825,
                        radius=500 if reader % 2 else 1000,
                        limit=20,
                        map_limit=500,
                    ),
                )
            elif name == "compare":
                path, params = "/api/v1/compare", {**pin, "area_codes": codes}
            elif name == "detail":
                path, params = "/api/v1/establishments/" + venues[reader % len(venues)], pin
            elif name == "companies":
                path, params = (
                    "/api/v1/companies",
                    {**pin, "cohort": "registered_office", "limit": 20},
                )
            else:
                path, params = "/api/v1/activity", {**pin, "limit": 20}
            result = invoke(name, path, params, reader, "browsing", release)
            if name == "explore":
                first = result
            if step < 7:
                time.sleep(5)

    with ThreadPoolExecutor(max_workers=10) as executor:
        list(executor.map(visitor, range(10)))
    browsing = [r for r in records if r["phase"] == "browsing"]
    warm = [r for r in browsing if r["success"] and r["cold"] is False and r["logs_available"]]
    analytical = [r for r in warm if r["name"] != "metadata"]

    def metrics(rows: list[dict[str, Any]]) -> dict[str, Any]:
        return dict(
            requests=len(rows),
            successes=sum(r["success"] for r in rows),
            errors=sum(not r["success"] for r in rows),
            throttles=sum(r["status"] == 429 for r in rows),
            success_client_p95_ms=percentile([r["client_ms"] for r in rows if r["success"]]),
        )

    report = dict(
        checked_at=datetime.now(UTC).isoformat(),
        release_id=release,
        environment="Private IAM Lambda invocation, London, live Neon; gateway/Cloudflare excluded",
        deployed_configuration={
            k: configuration.get(k) for k in ("MemorySize", "Timeout", "LastModified", "CodeSha256")
        },
        readers=10,
        requests_per_reader=8,
        think_seconds=5,
        initial_stagger_seconds=0.5,
        retries="SDK disabled; at most one recorded metadata setup retry",
        seconds=round(time.monotonic() - started, 2),
        browsing=metrics(browsing),
        warm_analytical=metrics(analytical),
        endpoints={
            n: metrics([r for r in browsing if r["name"] == n])
            for n in sorted({r["name"] for r in browsing})
        },
        cold_observations=[r for r in records if r["cold"]],
        initial_observations=[r for r in records if r["phase"] == "initial"],
        billed_gb_seconds=round(
            sum(r.get("billed_ms") or 0 for r in records)
            / 1000
            * configuration["MemorySize"]
            / 1024,
            3,
        ),
        requests_total=len(records),
        limit_notes=[
            "Small sample, not a sustained traffic or capacity guarantee.",
            "API responses are no-store; no application cache-hit scenario exists for this mix.",
            "Neon wake is unverified without provider telemetry; first read may include it.",
            "No forced cold start, deployment, provider downloads or database writes.",
            "Public gateway rate/burst 2/4 and reserved concurrency 2 remain launch checks.",
            "GB-seconds exclude requests/logs/transfer; not a bill or Neon CU measurement.",
        ],
        configuration_changes=0,
        provider_requests=0,
    )
    p95 = report["warm_analytical"]["success_client_p95_ms"]
    report["warm_target_passed"] = (
        p95 is not None
        and p95 <= 2000
        and len(analytical) >= 50
        and len(browsing) == 80
        and all(r["success"] for r in browsing)
    )
    limits = sdk.get_account_settings()["AccountLimit"]
    concurrency = sdk.get_function_concurrency(FunctionName=FUNCTION)
    report["quota_check"] = dict(
        regional_concurrent_executions=limits["ConcurrentExecutions"],
        api_reserved_concurrency=concurrency.get("ReservedConcurrentExecutions"),
        public_activation_required_regional_limit=104,
        configuration_changes=0,
    )
    (directory / "report.json").write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(dict(report=str(directory / "report.json"), **report), indent=2))


if __name__ == "__main__":
    main()

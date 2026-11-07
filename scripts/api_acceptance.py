"""Check published API reconciliation and measure ten in-process HTTP readers."""

from __future__ import annotations

import argparse
import hashlib
import json
import logging
import math
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime
from typing import Any

import psycopg
from fastapi.testclient import TestClient
from psycopg import sql
from pydantic import SecretStr

from scripts.database import credential
from scripts.dev import ROOT
from urbanlens.api.app import create_app
from urbanlens.config import Settings
from urbanlens.database.migrations import connect


def verify(target: str) -> dict[str, Any]:
    logging.getLogger("urbanlens.api").setLevel(logging.WARNING)
    settings = Settings(
        environment="test",
        api_database_url=SecretStr(credential(target, "api")),
        api_requests_per_minute=1000,
    )
    # First fresh connection, before any other target reads. A recent postmaster
    # start proves a compute restart; latency alone cannot prove scale-to-zero.
    attempted = datetime.now(UTC)
    started = time.perf_counter()
    with psycopg.connect(credential(target, "api"), connect_timeout=15) as c:
        row = c.execute("SELECT pg_postmaster_start_time()").fetchone()
        assert row
        server_start = row[0]
    first_ms = (time.perf_counter() - started) * 1000
    with TestClient(create_app(settings)) as client:

        def get(path: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
            response = client.get("/api/v1/" + path, params=params)
            if response.status_code != 200:
                raise ValueError(f"API acceptance failed: {path}: {response.status_code}")
            return dict(response.json())

        meta = get("meta")
        pin = {"release_id": meta["release_id"]}
        areas = get("areas", pin)["data"]
        codes = [a["area_code"] for a in areas[:2]]
        exploration = get("establishments", {**pin, "limit": 100, "map_limit": 1000})["data"]
        total = exploration["total"]
        assert sum(exploration["category_mix"].values()) == total
        seen = {v["establishment_id"] for v in exploration["items"]}
        cursor = exploration["next_cursor"]
        while cursor:
            page = get("establishments", {**pin, "limit": 100, "cursor": cursor})["data"]
            identifiers = {v["establishment_id"] for v in page["items"]}
            assert not seen.intersection(identifiers)
            seen.update(identifiers)
            cursor = page["next_cursor"]
        assert len(seen) == total
        comparison = get("compare", {**pin, "area_codes": codes})["data"]
        city = comparison["city_benchmark"]
        assert city["establishment_count"] == total
        for area in comparison["areas"]:
            summary = get(f"areas/{area['area_code']}/summary", pin)["data"]
            listed = get("establishments", {**pin, "mode": "area", "area_code": area["area_code"]})[
                "data"
            ]
            assert summary["area"] == area
            assert summary["city_benchmark"] == city
            assert listed["total"] == area["establishment_count"]
            assert listed["category_mix"] == area["category_mix"]
        for category, count in exploration["category_mix"].items():
            filtered = get("compare", {**pin, "area_codes": codes, "categories": [category]})[
                "data"
            ]
            assert filtered["city_benchmark"]["establishment_count"] == count
        companies = get("companies", {**pin, "cohort": "registered_office"})["data"]
        verified_companies = get("companies", {**pin, "cohort": "verified_operator"})["data"][
            "total"
        ]
        if companies["items"]:
            number = companies["items"][0]["company_number"]
            assert get("companies/" + number, pin)["data"]["company_number"] == number
        activity = get("activity", pin)["data"]
        # Compare all 165 area counts and weighted totals with accepted Gold,
        # independently of the list/map pagination path.
        with connect(credential(target, "migration")) as c:
            row = c.execute(
                "SELECT model_schema::text FROM ul_control.data_release WHERE release_id=%s",
                (meta["release_id"],),
            ).fetchone()
            assert row
            schema = sql.Identifier(row[0])
            expected = c.execute(
                sql.SQL(
                    "SELECT sum(establishment_count),sum(official_land_km2),sum(population) "
                    "FROM {}.mart_area WHERE category='all'"
                ).format(schema)
            ).fetchone()
            assert expected
            assert total == expected[0]
            assert math.isclose(city["official_land_km2"], float(expected[1]))
            assert city["population"] == expected[2]
            for cohort, count in (
                ("registered_office", companies["total"]),
                ("verified_operator", verified_companies),
            ):
                expected_company = c.execute(
                    sql.SQL(
                        "SELECT count(*) FROM {}.mart_company WHERE cohorts @> %s::jsonb"
                    ).format(schema),
                    (json.dumps([{"cohort": cohort}]),),
                ).fetchone()
                assert expected_company and count == expected_company[0]
            plans = {}
            queries = {
                "area": (
                    "SELECT establishment_id FROM {}.mart_establishment WHERE area_code=%s",
                    (codes[0],),
                ),
                "radius": (
                    "SELECT establishment_id FROM {}.mart_establishment WHERE "
                    "public.ST_DWithin(location,"
                    "public.ST_SetSRID(public.ST_Point(-0.14,50.82),4326)::public.geography,500.000001)",
                    (),
                ),
            }
            for name, (query, args) in queries.items():
                measured = c.execute(
                    sql.SQL("EXPLAIN (ANALYZE,BUFFERS,FORMAT JSON) " + query).format(schema), args
                ).fetchone()
                assert measured
                plans[name] = measured[0]
        paths: list[tuple[str, dict[str, Any]]] = [
            ("establishments", {**pin, "mode": "area", "area_code": codes[0]}),
            (
                "establishments",
                {**pin, "mode": "radius", "longitude": -0.14, "latitude": 50.82, "radius": 500},
            ),
            ("compare", {**pin, "area_codes": codes}),
            ("companies", {**pin, "cohort": "registered_office"}),
            ("activity", pin),
        ]
        # Warm every route before timing. Ten readers each run five sequential calls.
        for path, params in paths:
            get(path, params)

        def reader(_: int) -> list[tuple[str, float]]:
            timings = []
            for path, params in paths:
                start = time.perf_counter()
                response = get(path, params)
                assert response["release_id"] == meta["release_id"]
                label = path + (":" + params["mode"] if "mode" in params else "")
                timings.append((label, (time.perf_counter() - start) * 1000))
            return timings

        started = time.perf_counter()
        with ThreadPoolExecutor(max_workers=10) as pool:
            samples = [sample for batch in pool.map(reader, range(10)) for sample in batch]
        timings = sorted(t for _, t in samples)
        by_route = {}
        for label in sorted({label for label, _ in samples}):
            group = sorted(t for name, t in samples if name == label)
            by_route[label] = dict(
                requests=len(group), p95_ms=round(group[math.ceil(len(group) * 0.95) - 1], 2)
            )
        elapsed = time.perf_counter() - started
    return dict(
        target=target,
        verified_at=datetime.now(UTC).isoformat(),
        release_id=meta["release_id"],
        counts=dict(
            venues=total, areas=len(areas), companies=companies["total"], activity=activity["total"]
        ),
        city=city,
        baselines=activity["baselines"],
        plans=plans,
        first_connection=dict(
            attempted_at=attempted.isoformat(),
            duration_ms=round(first_ms, 2),
            postmaster_started_at=server_start.isoformat(),
            restart_during_connection=attempted.timestamp() - 2
            <= server_start.timestamp()
            <= datetime.now(UTC).timestamp(),
        ),
        benchmark=dict(
            method="In-process FastAPI HTTP client with actual database; not deployed HTTP",
            readers=10,
            requests=len(timings),
            failures=0,
            p50_ms=round(timings[len(timings) // 2], 2),
            p95_ms=round(timings[math.ceil(len(timings) * 0.95) - 1], 2),
            max_ms=round(max(timings), 2),
            elapsed_seconds=round(elapsed, 2),
            by_route=by_route,
            frontend_gate="Preliminary; deployed RC-12 remains open",
        ),
    )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--target", choices=["local", "neon"], required=True)
    args = parser.parse_args()
    report = verify(args.target)
    report["openapi_sha256"] = hashlib.sha256(
        (ROOT / "reference/api/openapi.json").read_bytes()
    ).hexdigest()
    path = ROOT / f"data/gold/api-acceptance-{args.target}.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    print(
        json.dumps(
            {k: v for k, v in report.items() if k not in {"plans", "city", "baselines"}}, indent=2
        )
    )


if __name__ == "__main__":
    try:
        main()
    except psycopg.Error as error:
        raise SystemExit(
            f"API database check failed ({error.sqlstate or type(error).__name__})"
        ) from None

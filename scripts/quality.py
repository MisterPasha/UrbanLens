"""Step-53 checks on the rehearsal's disposable database, never on production."""

from __future__ import annotations

import itertools
import json
import logging
import math
from typing import Any
from uuid import UUID, uuid4

import psycopg
from fastapi.testclient import TestClient
from psycopg import sql
from pydantic import SecretStr

from scripts.database import credential
from scripts.dev import ROOT
from scripts.rehearsal import api_read, command
from urbanlens.api.app import create_app
from urbanlens.config import Settings
from urbanlens.database.migrations import connect
from urbanlens.database.releases import publish
from urbanlens.silver.audit import inspect
from urbanlens.silver.operations import coverage

CATEGORIES = ("bakery", "cafe", "food_service", "pub_bar", "restaurant", "takeaway")


def injections(source: str, active: str) -> list[dict[str, Any]]:
    command("release", "prepare", "--target", "local", "--source-schema", source)
    candidate = json.loads((ROOT / "data/gold/release-local-prepare.json").read_bytes())
    schema = candidate["schema"]
    manifest = json.loads(
        (ROOT / ".cache" / ("gold-dbt-local-" + source) / "manifest.json").read_bytes()
    )
    compiled = ROOT / ".cache" / ("gold-dbt-local-" + source) / "compiled"
    checks = {}
    for name, node in manifest["nodes"].items():
        if node["resource_type"] != "test":
            continue
        paths = list(compiled.rglob(node["path"]))
        if len(paths) != 1:
            raise ValueError("Expected one compiled dbt test: " + name)
        checks[name] = (
            paths[0].read_text(encoding="utf-8").replace('"' + source + '"', '"' + schema + '"')
        )
    assert len(checks) >= 16
    before = api_read("/api/v1/establishments", active)
    with connect(credential("local", "migration")) as c:
        number = c.execute(
            sql.SQL("SELECT company_number FROM {}.mart_company LIMIT 1").format(
                sql.Identifier(schema)
            )
        ).fetchone()
        venue = c.execute(
            sql.SQL("SELECT establishment_id::text FROM {}.mart_establishment LIMIT 1").format(
                sql.Identifier(schema)
            )
        ).fetchone()
        assert number and venue
        link = dict(
            release_id=candidate["release_id"],
            link_id=str(uuid4()),
            company_number=number[0],
            establishment_id=venue[0],
            relationship="operator",
            review_id=str(uuid4()),
            evidence_uri="s3://synthetic/evidence/unapproved",
        )
        event = dict(
            release_id=candidate["release_id"],
            event_id=str(uuid4()),
            entity_type="source",
            event_type="source_changed",
            retained_evidence_uri="s3://synthetic/evidence/event",
        )

        def insert_record(
            table: str, row: dict[str, Any], repeat: int = 1
        ) -> tuple[str, tuple[Any, ...]]:
            relation = sql.Identifier(schema, table)
            query = sql.SQL(
                "INSERT INTO {} SELECT r.* FROM jsonb_populate_record(NULL::{},%s::jsonb) r "
                "CROSS JOIN generate_series(1,%s)"
            ).format(relation, relation)
            return query.as_string(c), (json.dumps(row), repeat)

        faults = [
            (
                "duplicate_venue_key",
                sql.SQL(
                    "INSERT INTO {s}.mart_establishment "
                    "SELECT * FROM {s}.mart_establishment LIMIT 1"
                )
                .format(s=sql.Identifier(schema))
                .as_string(c),
                (),
            ),
            ("duplicate_event_key", *insert_record("mart_activity", event, 2)),
            (
                "missing_area",
                sql.SQL(
                    "DELETE FROM {}.dim_area "
                    "WHERE area_code=(SELECT min(area_code) FROM {}.dim_area)"
                )
                .format(sql.Identifier(schema), sql.Identifier(schema))
                .as_string(c),
                (),
            ),
            (
                "missing_population",
                sql.SQL("UPDATE {}.dim_area SET population=NULL")
                .format(sql.Identifier(schema))
                .as_string(c),
                (),
            ),
            (
                "incompatible_edition",
                sql.SQL("UPDATE {}.dim_area SET population_edition='synthetic-incompatible'")
                .format(sql.Identifier(schema))
                .as_string(c),
                (),
            ),
            (
                "invalid_company_relationship",
                *insert_record("mart_company_link", {**link, "company_number": "INVALID"}),
            ),
            ("unreviewed_company_link", *insert_record("mart_company_link", link)),
            (
                "anomalous_source_counts",
                sql.SQL(
                    "UPDATE {}.mart_source_health SET accepted_count=raw_count+1 "
                    "WHERE provider='fsa'"
                )
                .format(sql.Identifier(schema))
                .as_string(c),
                (),
            ),
            (
                "invalid_venue_area",
                sql.SQL("UPDATE {}.mart_establishment SET area_code='INVALID'")
                .format(sql.Identifier(schema))
                .as_string(c),
                (),
            ),
        ]
        result = []
        for name, statement, parameters in faults:
            failed: list[str] = []
            constraint = None
            with c.transaction(force_rollback=True):
                try:
                    with c.transaction():
                        c.execute(statement, parameters)
                except psycopg.IntegrityError as error:
                    if (
                        name not in {"duplicate_venue_key", "duplicate_event_key"}
                        or error.sqlstate != "23505"
                    ):
                        raise
                    constraint = error.diag.constraint_name
                else:
                    failed = [test for test, query in checks.items() if c.execute(query).fetchone()]
                    assert failed, "A critical defect escaped dbt checks: " + name
                    try:
                        publish(c, UUID(candidate["release_id"]))
                    except ValueError as error:
                        assert "Validated release contents changed" in str(error)
                    else:
                        raise ValueError("A corrupt candidate was published: " + name)
                pointer = c.execute(
                    "SELECT release_id FROM ul_control.release_slot WHERE slot='active'"
                ).fetchone()
                assert pointer and str(pointer[0]) == active
            assert api_read("/api/v1/establishments", active) == before
            result.append(
                dict(
                    scenario=name,
                    failing_dbt_checks=failed,
                    rejected_by_unique_constraint=constraint,
                    publication_blocked=True,
                    active_api_unchanged=True,
                )
            )
    return result


def reconciliation(schema: str, release: str) -> dict[str, Any]:
    logging.getLogger("urbanlens.api").setLevel(logging.WARNING)
    settings = Settings(
        environment="test",
        api_database_url=SecretStr(credential("local", "api")),
        api_requests_per_minute=1000,
    )
    with connect(credential("local", "migration")) as c, TestClient(create_app(settings)) as http:

        def get(path: str, params: dict[str, Any]) -> dict[str, Any]:
            response = http.get("/api/v1/" + path, params={"release_id": release, **params})
            assert response.status_code == 200, (path, response.status_code)
            body = response.json()
            assert body["release_id"] == release
            return dict(body["data"])

        prefix = sql.Identifier(schema)
        rows = c.execute(
            sql.SQL(
                "SELECT establishment_id::text,primary_category,area_code "
                "FROM {}.mart_establishment"
            ).format(prefix)
        ).fetchall()
        areas = c.execute(
            sql.SQL("SELECT area_code FROM {}.dim_area ORDER BY area_code").format(prefix)
        ).fetchall()
        area_codes = [r[0] for r in areas]
        denominators = dict(
            (code, (float(land), population))
            for code, land, population in c.execute(
                sql.SQL("SELECT area_code,official_land_km2,population FROM {}.dim_area").format(
                    prefix
                )
            )
        )

        def rates(area: dict[str, Any]) -> None:
            basis = (
                [denominators[area["area_code"]]]
                if area["area_code"]
                else list(denominators.values())
            )
            land, population = sum(r[0] for r in basis), sum(r[1] for r in basis)
            assert math.isclose(area["official_land_km2"], land)
            assert area["population"] == population
            count = area["establishment_count"]
            for field, divisor, scale in (
                ("establishments_per_land_km2", land, 1),
                ("establishments_per_1000_residents", population, 1000),
            ):
                if divisor:
                    assert math.isclose(area[field], count / divisor * scale, rel_tol=1e-8)
                else:
                    assert area[field] is None

        subset_checks = 0
        for size in range(7):
            for subset in itertools.combinations(CATEGORIES, size):
                expected = {r[0] for r in rows if r[1] in subset}
                measured = c.execute(
                    sql.SQL("SELECT establishment_id::text FROM {}.selected_venues(%s)").format(
                        prefix
                    ),
                    (list(subset),),
                ).fetchall()
                assert {r[0] for r in measured} == expected
                summaries = c.execute(
                    sql.SQL(
                        "SELECT a.area_code,s.establishment_count FROM {}.dim_area a "
                        "CROSS JOIN LATERAL {}.area_summary(ARRAY[a.area_code],%s) s"
                    ).format(prefix, prefix),
                    (list(subset),),
                ).fetchall()
                assert len(summaries) == len(areas)
                for code, total in summaries:
                    assert total == sum(r[1] in subset and r[2] == code for r in rows)
                subset_checks += 1
        scopes = [
            dict(mode="city"),
            *[dict(mode="area", area_code=code) for code in area_codes[:2]],
            *[
                dict(mode="radius", longitude=-0.14, latitude=50.82, radius=radius)
                for radius in (250, 500, 1000, 2000)
            ],
        ]
        filters = [None, [], *[[category] for category in CATEGORIES], ["cafe", "bakery"]]
        checked = []
        for categories in filters:
            for scope in scopes:
                params = {**scope, "limit": 100, "map_limit": 1000}
                if categories is not None:
                    params["categories"] = categories
                if categories == []:
                    params["empty_categories"] = True
                expected_rows = c.execute(
                    sql.SQL(
                        "SELECT establishment_id::text FROM {}.mart_establishment "
                        "WHERE (%s::text[] IS NULL OR primary_category=ANY(%s)) "
                        "AND (%s::text IS NULL OR area_code=%s) "
                        "AND (%s::double precision IS NULL OR public.ST_DWithin(location,"
                        "public.ST_SetSRID(public.ST_Point(%s,%s),4326)::public.geography,%s))"
                    ).format(prefix),
                    (
                        categories,
                        categories,
                        scope.get("area_code"),
                        scope.get("area_code"),
                        scope.get("longitude"),
                        scope.get("longitude", -0.14),
                        scope.get("latitude", 50.82),
                        scope.get("radius", 500) + 0.000001,
                    ),
                ).fetchall()
                ids = {r[0] for r in expected_rows}
                data = get("establishments", params)
                assert data["total"] == len(ids) == sum(data["category_mix"].values())
                points = {r["establishment_id"] for r in data["map_points"]}
                assert points <= ids and len(points) == min(len(ids), 1000)
                assert data["map_truncated"] == (len(ids) > 1000)
                seen = {r["establishment_id"] for r in data["items"]}
                cursor = data["next_cursor"]
                while cursor:
                    page = get("establishments", {**params, "cursor": cursor})
                    current = {r["establishment_id"] for r in page["items"]}
                    assert not seen.intersection(current)
                    seen.update(current)
                    cursor = page["next_cursor"]
                assert seen == ids
                checked.append(
                    dict(
                        categories=categories,
                        scope=scope,
                        count=len(ids),
                        map_truncated=data["map_truncated"],
                    )
                )
            compare = get(
                "compare",
                {
                    "area_codes": area_codes[:2],
                    **({"categories": categories} if categories is not None else {}),
                    **({"empty_categories": True} if categories == [] else {}),
                },
            )
            assert compare["city_benchmark"]["establishment_count"] == sum(
                categories is None or r[1] in categories for r in rows
            )
            rates(compare["city_benchmark"])
            for area in compare["areas"]:
                assert area["establishment_count"] == sum(
                    (categories is None or r[1] in categories) and r[2] == area["area_code"]
                    for r in rows
                )
                rates(area)
        return dict(
            category_subsets=subset_checks,
            areas_per_subset=len(areas),
            api_cases=checked,
            independent_expected_sets=True,
            zero_category_filter_verified=True,
        )


def verify(gold: dict[str, Any], release: dict[str, Any]) -> dict[str, Any]:
    faults = injections(gold["schema"], release["release_id"])
    (ROOT / "data/quality-injections.json").write_text(json.dumps(faults, indent=2))
    with connect(credential("local", "migration")) as c:
        audit = inspect(c)
        measured = coverage(c)
        population = c.execute(
            "SELECT count(*) FROM ul_silver.population p JOIN ul_control.artifact a "
            "ON a.artifact_id=p.source_artifact_id WHERE a.uri LIKE '%%/population.xlsx'"
        ).fetchone()
        assert audit["passed"], "Real-record audit found contradictions"
        assert population and population[0] == 165
    return dict(
        fault_injections=faults,
        reconciliation=reconciliation(release["schema"], release["release_id"]),
        audit=audit,
        coverage=measured,
        population_workbook_provenance_rows=population[0],
    )

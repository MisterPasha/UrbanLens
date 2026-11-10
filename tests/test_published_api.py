"""Release failure/concurrency and public query/privacy contracts on disposable PostGIS."""

from __future__ import annotations

import hashlib
from collections.abc import Iterator
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from uuid import UUID, uuid4

import httpx
import pytest
from fastapi.testclient import TestClient
from psycopg import sql
from psycopg.conninfo import make_conninfo
from psycopg.types.json import Jsonb
from pydantic import SecretStr

from scripts.database import administrator, credential, scratch_database
from scripts.gold import dbt_run
from urbanlens.api.app import create_app
from urbanlens.api.postcodes import Postcodes
from urbanlens.config import Settings
from urbanlens.database.migrations import ROLES, connect, migrate
from urbanlens.database.releases import cleanup, publish, schema_for

pytestmark = pytest.mark.database

SEED = """
CREATE TABLE __SCHEMA__.dim_area AS SELECT 'E01000001'::text area_code,'Synthetic area'::text 
 area_name,
 'LSOA-2021'::text geography_version,1::numeric official_land_km2,100::bigint population,
 public.ST_MakeEnvelope(-0.2,50.75,-0.05,50.9,4326) geom;
CREATE TABLE __SCHEMA__.mart_source_health AS SELECT 'fsa'::text provider,'synthetic'::text 
 scope_key,
 'healthy'::text status,'2026-11-04 12:00Z'::timestamptz last_success_at,now()+interval 
 '1 month' next_due_at;
CREATE TABLE __SCHEMA__.mart_coverage AS SELECT 'current canonical roster'::text universe,
 'coordinate_quality'::text dimension,'withheld'::text bucket,1::bigint n,4::bigint denominator,
 25::numeric percent,true reasons_overlap;
CREATE TABLE __SCHEMA__.mart_establishment AS
 SELECT md5(g::text)::uuid establishment_id,'Synthetic cafe '||g display_name,false 
 name_unavailable,
 'Published address'::text address,'BN1 1AA'::text postcode,
 CASE WHEN g=3 THEN 'food_service' ELSE 'cafe' END primary_category,'{}'::text[] 
 secondary_categories,
 'eligible'::text eligibility,'resolved'::text duplicate_status,'observed'::text 
 observation_state,
 true primary_spatial_eligible,'source_point'::text location_quality,
 public.ST_Project(public.ST_SetSRID(public.ST_Point(-0.14,50.82),4326)::public.geography,
 CASE WHEN g=2 THEN 500 WHEN g=3 THEN 501 ELSE 0 END,0) location,'E01000001'::text area_code,
 now() first_observed_at,now() last_observed_at,'fsa_only'::text source_mix,'{}'::text[] 
 exclusion_reasons,
 jsonb_build_array(jsonb_build_object('provider','fsa','key','123','state','observed',
 'observed_at',now(),
   'tags',jsonb_build_object('addr:street','SECRET ADDRESS'))) sources,
 jsonb_build_array(jsonb_build_object('rating',jsonb_build_object('state',
 'awaiting_inspection','score',null))) hygiene,
 jsonb_build_object('address',jsonb_build_object('rule_version','fixture','source_time',null,
   'reason','SECRET ADDRESS','detail',jsonb_build_object('value','SECRET ADDRESS'))) provenance,
 '[]'::jsonb verified_operators
 FROM generate_series(1,3) g;
CREATE TABLE __SCHEMA__.mart_venue_context (LIKE __SCHEMA__.mart_establishment INCLUDING ALL);
INSERT INTO __SCHEMA__.mart_venue_context SELECT * FROM __SCHEMA__.mart_establishment LIMIT 1;
UPDATE __SCHEMA__.mart_venue_context SET establishment_id=md5('4')::uuid,
 display_name='Withheld venue',
 address=null,postcode=null,location=null,area_code=null,primary_spatial_eligible=false,
 eligibility='unresolved',location_quality='withheld',
 exclusion_reasons=ARRAY['not_primary_spatial'];
INSERT INTO __SCHEMA__.mart_venue_context SELECT * FROM __SCHEMA__.mart_venue_context LIMIT 1;
UPDATE __SCHEMA__.mart_venue_context SET establishment_id=md5('5')::uuid,
 duplicate_status='unresolved'
 WHERE ctid=(SELECT ctid FROM __SCHEMA__.mart_venue_context ORDER BY ctid DESC LIMIT 1);
INSERT INTO __SCHEMA__.mart_venue_context SELECT * FROM __SCHEMA__.mart_venue_context
 WHERE establishment_id=md5('5')::uuid;
UPDATE __SCHEMA__.mart_venue_context SET establishment_id=md5('6')::uuid,eligibility='excluded'
 WHERE ctid=(SELECT ctid FROM __SCHEMA__.mart_venue_context ORDER BY ctid DESC LIMIT 1);
ALTER TABLE __SCHEMA__.dim_area ADD COLUMN population_edition text DEFAULT 'fixture',
 ADD COLUMN geography_artifact_id uuid,
 ADD COLUMN population_reference_date date DEFAULT '2024-06-30',
 ADD COLUMN age_0_15 bigint DEFAULT 20, ADD COLUMN age_16_64 bigint DEFAULT 60,
 ADD COLUMN age_65_plus bigint DEFAULT 20,
 ADD COLUMN age_0_15_percent numeric DEFAULT 20, ADD COLUMN age_16_64_percent numeric DEFAULT 60,
 ADD COLUMN age_65_plus_percent numeric DEFAULT 20;
INSERT INTO __SCHEMA__.dim_area(area_code,area_name,geography_version,official_land_km2,
 population,geom) VALUES ('E01000002','Second area','LSOA-2021',3,900,
 public.ST_MakeEnvelope(-0.05,50.75,0,50.9,4326));
UPDATE __SCHEMA__.dim_area SET age_0_15=180,age_16_64=540,age_65_plus=180
 WHERE area_code='E01000002';
CREATE TABLE __SCHEMA__.mart_city AS SELECT 'all'::text category,
 'LSOA-2021'::text geography_version,'fixture'::text population_edition,
 '2024-06-30'::date population_reference_date,
 sum(age_0_15) age_0_15,sum(age_16_64) age_16_64,sum(age_65_plus) age_65_plus,
 20::numeric age_0_15_percent,60::numeric age_16_64_percent,20::numeric age_65_plus_percent
 FROM __SCHEMA__.dim_area;
CREATE TABLE __SCHEMA__.mart_company AS SELECT c.*,'[]'::jsonb cohorts,
 'Registered office is not evidence of a trading premises'::text office_caveat
 FROM ul_silver.company c;
INSERT INTO __SCHEMA__.mart_company(company_number,legal_name,legal_status,company_type,
 sic_codes,incorporation_date,first_observed_at,last_observed_at,cohorts,office_caveat)
 SELECT '0000000'||g,'Synthetic Company '||g,'active','ltd',ARRAY['56101'],'2020-01-01',
 now(),now(),CASE WHEN g<3 THEN '[{"cohort":"registered_office"}]'::jsonb ELSE '[]'::jsonb END,
 'Registered office is not evidence of a trading premises' FROM generate_series(1,3) g;
UPDATE __SCHEMA__.mart_company SET cohorts=cohorts||'[{"cohort":"verified_operator"}]'::jsonb
 WHERE company_number='00000002';
CREATE TABLE __SCHEMA__.mart_company_link AS SELECT l.*,null::timestamptz reviewed_at,
 null::date reviewed_on,true primary_spatial_eligible FROM ul_silver.company_establishment_link l;
INSERT INTO __SCHEMA__.mart_company_link(company_number,establishment_id,relationship,
 evidence_uri,reviewed_at,primary_spatial_eligible) VALUES ('00000002',md5('1')::uuid,
 'operator','https://example.org/public-evidence',now(),true);
CREATE TABLE __SCHEMA__.mart_activity AS SELECT e.*,null::timestamptz baseline_established_at,
 'Observed changes are not verified premises openings or closures'::text interpretation
 FROM ul_silver.change_event e;
INSERT INTO __SCHEMA__.mart_activity(event_id,event_key,entity_type,establishment_id,event_type,
 observed_at,before_value,after_value,evidence_fields,retained_evidence_uri,rule_version,
 baseline_established_at,interpretation)
 SELECT md5(('event'||g)::text)::uuid,repeat('a',64),'establishment',md5('1')::uuid,
 CASE WHEN g=3 THEN 'processing_correction' ELSE 'source_changed' END,
 '2026-11-06 12:00Z'::timestamptz,
 '{"display_name":"Old name","address":"SECRET ADDRESS"}'::jsonb,
 '{"display_name":"New name","location":{"secret":"PRIVATE LOCATION"}}'::jsonb,
 '{"provider":"fsa","manifest_uri":"s3://private/manifest"}'::jsonb,
 's3://private/evidence/event','fixture','2026-11-01 12:00Z',
 'Observed changes are not verified premises openings or closures' FROM generate_series(1,3) g;
CREATE TABLE __SCHEMA__.mart_activity_status AS SELECT 'establishment'::text entity_type,
 '2026-11-01 12:00Z'::timestamptz established_at,3::bigint retained_event_count,
 'observed_changes'::text state,'2025-11-07'::timestamptz retention_boundary,
 2::bigint baseline_source_scopes UNION ALL SELECT 'company',null,null,'baseline_unavailable',
 '2025-11-07'::timestamptz,0;
GRANT SELECT ON ALL TABLES IN SCHEMA __SCHEMA__ TO urbanlens_dbt;"""


@dataclass(repr=False)
class Lab:
    database: str
    roles: dict[str, str]
    base: UUID
    settings: Settings

    def candidate(self, source: UUID | None = None, *, drop_venue: bool = False) -> UUID:
        identifier = uuid4()
        schema = schema_for(identifier)
        with connect(make_conninfo(administrator("local"), dbname=self.database)) as admin:
            admin.execute(
                sql.SQL("CREATE SCHEMA {} AUTHORIZATION urbanlens_migrator").format(
                    sql.Identifier(schema)
                )
            )
            admin.execute(
                sql.SQL("GRANT USAGE,CREATE ON SCHEMA {} TO urbanlens_dbt").format(
                    sql.Identifier(schema)
                )
            )
        with connect(self.roles["migration"]) as c:
            if source:
                old = schema_for(source)
                table_names = [
                    r[0]
                    for r in c.execute(
                        "SELECT tablename FROM pg_tables WHERE schemaname=%s", (old,)
                    )
                ]
                for table in table_names:
                    c.execute(
                        sql.SQL("CREATE TABLE {}.{} AS SELECT * FROM {}.{}").format(
                            sql.Identifier(schema),
                            sql.Identifier(table),
                            sql.Identifier(old),
                            sql.Identifier(table),
                        )
                    )
                definitions = c.execute(
                    (
                        "SELECT pg_get_functiondef(p.oid) FROM pg_proc p JOIN"
                        " pg_namespace n ON n.oid=p.pronamespace WHERE n.nspname=%s ORDER"
                        " BY p.proname DESC"
                    ),
                    (old,),
                ).fetchall()
                # Reuse actual dbt-generated query definitions; no duplicate fixture implementation.
                for (definition,) in definitions:
                    c.execute(definition.replace(old, schema))
            else:
                c.execute(SEED.replace("__SCHEMA__", sql.Identifier(schema).as_string(c)))
        if not source:
            dbt_run(
                "local", schema, {}, ["run-operation", "install_queries"], database=self.database
            )
        with connect(self.roles["migration"]) as c:
            if drop_venue:
                c.execute(
                    sql.SQL(
                        "DELETE FROM {}.mart_establishment WHERE primary_category='food_service'"
                    ).format(sql.Identifier(schema))
                )
            tables = {}
            for (table,) in c.execute(
                "SELECT tablename FROM pg_tables WHERE schemaname=%s", (schema,)
            ).fetchall():
                measured = c.execute(
                    sql.SQL(
                        "SELECT count(*),md5(coalesce(string_agg(h,'' ORDER BY h),''))"
                        " FROM (SELECT md5(to_jsonb(t)::text) h FROM {}.{} t) v"
                    ).format(sql.Identifier(schema), sql.Identifier(table))
                ).fetchone()
                assert measured is not None
                count, md5 = measured
                tables[table] = dict(rows=count, md5=md5)
            c.execute(
                (
                    "INSERT INTO"
                    " ul_control.data_release(release_id,status,quality_passed,geograp"
                    "hy_version,population_edition,population_reference_date,taxonomy_"
                    "version,pipeline_version,manifest_uri,manifest_sha256,model_schem"
                    "a,validated_contents) VALUES"
                    " (%s,'validated',true,'LSOA-2021','fixture','2024-06-30','fixture"
                    "','fixture','s3://synthetic/release',%s,%s,%s)"
                ),
                (identifier, "a" * 64, schema, Jsonb(dict(tables=tables, omitted_columns=[]))),
            )
        return identifier


@pytest.fixture
def lab(tmp_path: Path) -> Iterator[Lab]:
    with scratch_database("local") as name:
        roles = {p: credential("local", p, database=name) for p in ROLES}
        with connect(roles["migration"]) as c:
            migrate(c)
        settings = Settings(
            environment="test",
            api_database_url=SecretStr(roles["api"]),
            postcode_cache_path=tmp_path / "postcodes.sqlite",
        )
        value = Lab(name, roles, uuid4(), settings)
        value.base = value.candidate()
        with connect(roles["migration"]) as c:
            publish(c, value.base)
        yield value


def test_atomic_failure_concurrent_publication_rollback_and_reader_safe_expiry(lab: Lab) -> None:
    second, third = lab.candidate(lab.base), lab.candidate(lab.base)
    with connect(lab.roles["migration"]) as c:
        with pytest.raises(ValueError, match="Injected"):
            publish(c, second, fail_after_pointer=True)
        assert c.execute(
            "SELECT release_id FROM ul_control.release_slot WHERE slot='active'"
        ).fetchone() == (lab.base,)
        c.execute(
            "UPDATE ul_control.data_release SET quality_passed=false WHERE release_id=%s", (third,)
        )
        with pytest.raises(ValueError, match="validated"):
            publish(c, third)
        c.execute(
            "UPDATE ul_control.data_release SET quality_passed=true WHERE release_id=%s", (third,)
        )

    def worker(identifier: UUID) -> dict[str, Any]:
        with connect(lab.roles["migration"]) as c:
            return publish(c, identifier)

    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(pool.map(worker, [second, third]))
    assert {r["active"] for r in results} == {str(second), str(third)}
    with connect(lab.roles["migration"]) as c:
        slots = dict(c.execute("SELECT slot,release_id FROM ul_control.release_slot"))
        before = slots["active"]
        result = publish(c, slots["previous"], rollback=True)
        assert result["previous"] == str(before)
    # An already-running request protects its old schema even after two publications.
    with connect(lab.roles["api"]) as reader:
        reader.execute("SELECT pg_advisory_xact_lock_shared(192033,hashtext(%s))", (str(lab.base),))
        with connect(lab.roles["migration"]) as c:
            report = cleanup(c, apply=True)
            assert str(lab.base) in report["readers_protected"]
    with connect(lab.roles["migration"]) as c:
        assert str(lab.base) in cleanup(c, apply=True)["removable"]
    with TestClient(create_app(lab.settings)) as client:
        response = client.get("/api/v1/meta", params={"release_id": str(lab.base)})
        assert response.status_code == 410 and response.json()["error"]["refresh_required"]
    with connect(lab.roles["api"]) as c:
        assert c.execute(
            "SELECT has_schema_privilege(current_user,'ul_control','USAGE')"
        ).fetchone() == (False,)


def test_pinned_exploration_counts_pagination_radius_detail_and_privacy(lab: Lab) -> None:
    with TestClient(create_app(lab.settings)) as client:
        meta = client.get("/api/v1/meta")
        assert meta.status_code == 200, meta.text
        assert meta.json()["release_id"] == str(lab.base)
        assert len(client.get("/api/v1/areas").json()["data"]) == 2
        page = client.get("/api/v1/establishments", params={"limit": 1, "map_limit": 1})
        assert page.status_code == 200, page.text
        data = page.json()["data"]
        assert data["total"] == 3 and len(data["items"]) == 1 and data["map_truncated"]
        assert sum(data["category_mix"].values()) == 3
        cursor = data["next_cursor"]
        later = lab.candidate(lab.base)
        with connect(lab.roles["migration"]) as c:
            publish(c, later)
        next_page = client.get(
            "/api/v1/establishments", params={"cursor": cursor, "limit": 1, "map_limit": 1}
        )
        assert next_page.status_code == 200, next_page.text
        assert next_page.json()["release_id"] == str(lab.base)
        assert (
            next_page.json()["data"]["items"][0]["establishment_id"]
            != data["items"][0]["establishment_id"]
        )
        assert (
            client.get(
                "/api/v1/establishments", params={"cursor": cursor, "categories": "cafe"}
            ).status_code
            == 422
        )
        radius = client.get(
            "/api/v1/establishments",
            params={"mode": "radius", "longitude": -0.14, "latitude": 50.82, "radius": 500},
        )
        assert radius.status_code == 200, radius.text
        assert radius.json()["data"]["total"] == 2  # Inclusive exact 500 m, excludes 501 m.
        box = client.get(
            "/api/v1/establishments",
            params=[("mode", "bbox")] + [("bbox", v) for v in [-0.16, 50.80, -0.12, 50.85]],
        )
        assert box.status_code == 200, box.text
        assert box.json()["data"]["total"] == 3
        hidden = str(UUID(hashlib.md5(b"4").hexdigest()))
        detail = client.get("/api/v1/establishments/" + hidden)
        assert detail.status_code == 200, detail.text
        assert "SECRET ADDRESS" not in detail.text and "s3://" not in detail.text
        assert detail.json()["data"]["null_reasons"]["location"] == "withheld"
        for params in (
            {"radius": 500},
            {"mode": "radius", "longitude": -0.14, "latitude": 50.82, "radius": 333},
            {"limit": 101},
            {"mode": "radius", "longitude": "nan", "latitude": 50.82},
            {"categories": "school"},
            {"mode": "area", "area_code": "E01999999"},
            {"mode": "radius", "longitude": 0, "latitude": 0},
            {"sql": "SELECT secrets"},
            {"mode": "bbox", "bbox": [-0.2, 50.7, 0.1, 50.9]},
        ):
            assert client.get("/api/v1/establishments", params=params).status_code == 422


def test_context_reconciliation_pinned_browsing_and_cohort_privacy(lab: Lab) -> None:
    with TestClient(create_app(lab.settings)) as client:
        pin = {"release_id": str(lab.base)}
        summary = client.get("/api/v1/areas/E01000001/summary", params=pin)
        assert summary.status_code == 200, summary.text
        coverage = summary.json()["coverage"]
        matching = [row for row in coverage if row["dimension"] == "duplicates"]
        assert matching == [
            {
                "universe": "non-excluded venue records",
                "dimension": "duplicates",
                "bucket": "resolved",
                "n": 4,
                "denominator": 5,
                "percent": 80,
                "reasons_overlap": False,
            },
            {
                "universe": "non-excluded venue records",
                "dimension": "duplicates",
                "bucket": "unresolved",
                "n": 1,
                "denominator": 5,
                "percent": 20,
                "reasons_overlap": False,
            },
        ]
        data = summary.json()["data"]
        assert data["area"]["establishment_count"] == 3
        assert data["city_benchmark"]["establishments_per_land_km2"] == 0.75
        assert data["city_benchmark"]["establishments_per_1000_residents"] == 3
        assert sum(data["area"]["category_mix"].values()) == 3
        companies = client.get(
            "/api/v1/companies", params={**pin, "cohort": "registered_office", "limit": 1}
        )
        assert companies.status_code == 200, companies.text
        assert companies.json()["data"]["total"] == 2
        assert companies.json()["data"]["items"][0]["company_number"] == "00000001"
        activity = client.get("/api/v1/activity", params={**pin, "limit": 1})
        assert activity.status_code == 200, activity.text
        assert activity.json()["data"]["total"] == 3
        assert "SECRET ADDRESS" not in activity.text and "s3://" not in activity.text
        assert "PRIVATE LOCATION" not in activity.text
        later = lab.candidate(lab.base, drop_venue=True)
        with connect(lab.roles["migration"]) as c:
            publish(c, later)
        assert client.get("/api/v1/establishments").json()["data"]["total"] == 2
        compare = client.get(
            "/api/v1/compare", params={**pin, "area_codes": ["E01000001", "E01000002"]}
        )
        assert compare.status_code == 200, compare.text
        assert compare.json()["release_id"] == str(lab.base)
        assert compare.json()["data"]["city_benchmark"] == data["city_benchmark"]
        assert sum(a["establishment_count"] for a in compare.json()["data"]["areas"]) == 3
        assert client.get("/api/v1/companies/00000001", params=pin).status_code == 200
        assert client.get("/api/v1/companies/00000003", params=pin).status_code == 404
        assert client.get("/api/v1/companies").status_code == 422
        assert (
            client.get("/api/v1/companies", params={"cohort": "verified_operator"}).json()["data"][
                "total"
            ]
            == 1
        )
        next_company = client.get(
            "/api/v1/companies",
            params={
                "cohort": "registered_office",
                "cursor": companies.json()["data"]["next_cursor"],
                "limit": 1,
            },
        )
        assert next_company.status_code == 200, next_company.text
        assert next_company.json()["release_id"] == str(lab.base)
        assert not next_company.json()["data"]["has_more"]
        seen = {activity.json()["data"]["items"][0]["event_id"]}
        cursor = activity.json()["data"]["next_cursor"]
        while cursor:
            response = client.get("/api/v1/activity", params={"cursor": cursor, "limit": 1})
            assert response.status_code == 200, response.text
            assert response.json()["release_id"] == str(lab.base)
            item = response.json()["data"]["items"][0]
            assert item["event_id"] not in seen
            seen.add(item["event_id"])
            cursor = response.json()["data"]["next_cursor"]
        assert len(seen) == 3
        assert (
            client.get(
                "/api/v1/activity",
                params={
                    "cursor": activity.json()["data"]["next_cursor"],
                    "event_type": "reappeared",
                },
            ).status_code
            == 422
        )
        for codes in (["E01000001", "E01000001"], ["E01000001", "E01999999"], ["E01000001"]):
            assert client.get("/api/v1/compare", params={"area_codes": codes}).status_code == 422
        empty = client.get("/api/v1/areas/E01000001/summary", params={"empty_categories": True})
        assert empty.json()["data"]["area"]["establishment_count"] == 0


def test_database_wait_is_bounded_and_private_gateway_is_not_callable(lab: Lab) -> None:
    with connect(lab.roles["api"]) as c:
        assert c.execute(
            "SELECT has_function_privilege(current_user,"
            "'ul_api.read_foundation(uuid,text,jsonb)','EXECUTE')"
        ).fetchone() == (False,)
    with connect(lab.roles["migration"]) as blocker:
        blocker.execute("SELECT pg_advisory_xact_lock(192033,hashtext(%s))", (str(lab.base),))
        with TestClient(create_app(lab.settings)) as client:
            response = client.get("/api/v1/activity", params={"release_id": str(lab.base)})
            assert response.status_code == 503, response.text
            assert response.json()["error"]["code"] == "data_busy"
            assert response.headers["Retry-After"] == "1"
    # Inject expensive work only into this disposable database, retaining the real
    # request transaction, gateway and driver error mapping.
    with connect(lab.roles["migration"]) as c:
        c.execute("""
            CREATE OR REPLACE FUNCTION ul_api.activity_context(v jsonb) RETURNS jsonb
            LANGUAGE plpgsql AS $$ BEGIN PERFORM pg_sleep(6); RETURN null; END $$
        """)
    with TestClient(create_app(lab.settings)) as client:
        response = client.get("/api/v1/activity")
        assert response.status_code == 503, response.text
        assert response.json()["error"]["code"] == "query_timeout"


def test_postcode_cache_scope_quality_failures_and_request_budget(lab: Lab, tmp_path: Path) -> None:
    calls = []

    def provider(request: httpx.Request) -> httpx.Response:
        key = request.url.path.rsplit("/", 1)[-1]
        calls.append(key)
        if key == "BN12AA":
            return httpx.Response(404, json={"status": 404})
        if key == "BN13AA":
            raise httpx.ConnectError("private provider detail")
        if key == "BN14AA":
            return httpx.Response(429)
        return httpx.Response(
            200,
            json={
                "status": 200,
                "result": dict(
                    postcode=key,
                    quality=1,
                    longitude=-0.14,
                    latitude=50.82,
                    codes={"admin_district": "E06000043", "lsoa21": "E01000001"},
                ),
            },
        )

    service = Postcodes(tmp_path / "interactive.sqlite", httpx.MockTransport(provider))
    with TestClient(create_app(lab.settings, postcode_service=service)) as client:
        first = client.get("/api/v1/postcodes/BN1%201AA")
        assert first.status_code == 200, first.text
        assert first.json()["data"]["can_define_search"] and first.json()["data"]["approximate"]
        assert client.get("/api/v1/postcodes/BN11AA").json()["data"]["cache_hit"]
        assert calls == ["BN11AA"]
        for code, state in [
            ("BN12AA", "unmatched"),
            ("BN13AA", "provider_failure"),
            ("BN14AA", "throttled"),
        ]:
            response = client.get("/api/v1/postcodes/" + code)
            assert response.status_code == 200, response.text
            assert response.json()["data"]["state"] == state
        assert client.get("/api/v1/postcodes/not-a-postcode").status_code == 422
    limited = lab.settings.model_copy(update={"api_requests_per_minute": 1})
    with TestClient(create_app(limited)) as client:
        assert client.get("/api/v1/meta").status_code == 200
        response = client.get("/api/v1/meta")
        assert response.status_code == 429 and 1 <= int(response.headers["Retry-After"]) <= 60

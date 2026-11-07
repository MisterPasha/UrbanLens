"""Synthetic contracts exercised only inside a newly created disposable database."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any
from uuid import uuid4

import psycopg

from urbanlens.database.migrations import connect


def expect_error(
    connection: psycopg.Connection[Any], statement: str, parameters: tuple[Any, ...], state: str
) -> None:
    try:
        with connection.transaction():
            connection.execute(statement, parameters)
    except psycopg.Error as error:
        if error.sqlstate != state:
            raise
    else:
        raise AssertionError("A database invariant was not enforced")


def exercise(dsns: dict[str, str]) -> dict[str, Any]:
    """Small synthetic dataset: no downloaded source data and no real publication."""
    now = datetime.now(UTC)
    scope, cycle, run, snapshot, artifact, identity, observation, venue, release, review = (
        uuid4() for _ in range(10)
    )
    with connect(dsns["migration"]) as owner:
        owner.execute(
            (
                "INSERT INTO ul_control.source_scope(scope_id,provider,scope_key) VALUES "
                "(%s,'fsa','synthetic')"
            ),
            (scope,),
        )
        owner.execute(
            (
                "INSERT INTO ul_control.collection_cycle VALUES (%s,%s,%s,%s+interval '1 "
                "month','fixture',%s,NULL)"
            ),
            (cycle, scope, now, now, "a" * 64),
        )
        owner.execute(
            (
                "INSERT INTO "
                "ul_control.ingestion_run(run_id,cycle_id,scope_id,mode,pipeline_version,status,started_at,ended_at)"
                " VALUES (%s,%s,%s,'collect','fixture','accepted',%s,%s)"
            ),
            (run, cycle, scope, now, now),
        )
        owner.execute(
            (
                "INSERT INTO "
                "ul_control.source_snapshot(snapshot_id,cycle_id,scope_id,provider,run_id,manifest_uri,manifest_sha256,fetched_at,validation_version,validation_passed,expected_parts,complete_parts,raw_count,parsed_count,accepted_count,rejected_count)"
                " VALUES "
                "(%s,%s,%s,'fsa',%s,'s3://synthetic/manifests/input.json',%s,%s,'fixture',true,1,1,1,1,1,0)"
            ),
            (snapshot, cycle, scope, run, "a" * 64, now),
        )
        owner.execute(
            (
                "INSERT INTO "
                "ul_control.artifact(artifact_id,run_id,uri,sha256,byte_count,compression,media_type,layer)"
                " VALUES "
                "(%s,%s,'s3://synthetic/bronze/input.xml',%s,1,'none','application/xml','bronze')"
            ),
            (artifact, run, "a" * 64),
        )
        owner.execute(
            (
                "INSERT INTO ul_silver.source_identity(identity_id,provider,source_key) "
                "VALUES (%s,'fsa','1234')"
            ),
            (identity,),
        )
        owner.execute(
            (
                "INSERT INTO "
                "ul_silver.observation_version(observation_id,identity_id,provider,record_hash,normalisation_version,first_snapshot_id,last_snapshot_id,first_observed_at,last_observed_at,source_artifact_id,raw_record_locator,address_state,location_quality)"
                " VALUES "
                "(%s,%s,'fsa',%s,'fixture',%s,%s,%s,%s,%s,'row/1','missing','missing')"
            ),
            (observation, identity, "a" * 64, snapshot, snapshot, now, now, artifact),
        )
        expect_error(
            owner,
            "INSERT INTO ul_silver.source_identity(provider,source_key) VALUES ('osm','1234')",
            (),
            "23514",
        )
        expect_error(
            owner,
            (
                "INSERT INTO ul_silver.source_identity(provider,source_key,generation) "
                "VALUES ('fsa','1234',2)"
            ),
            (),
            "23505",
        )
        # Closing the current row still cannot manufacture a new unchanged monthly version.
        try:
            with owner.transaction():
                owner.execute(
                    (
                        "UPDATE ul_silver.observation_version SET valid_to=%s+interval '1 day' "
                        "WHERE observation_id=%s"
                    ),
                    (now, observation),
                )
                owner.execute(
                    (
                        "INSERT INTO "
                        "ul_silver.observation_version(observation_id,identity_id,provider,record_hash,normalisation_version,first_snapshot_id,last_snapshot_id,first_observed_at,last_observed_at,source_artifact_id,raw_record_locator,address_state,location_quality)"
                        " SELECT "
                        "%s,identity_id,provider,record_hash,normalisation_version,first_snapshot_id,last_snapshot_id,first_observed_at,last_observed_at,source_artifact_id,raw_record_locator,address_state,location_quality"
                        " FROM ul_silver.observation_version WHERE observation_id=%s"
                    ),
                    (uuid4(), observation),
                )
        except psycopg.errors.CheckViolation:
            pass
        else:
            raise AssertionError("Unchanged monthly copies must not create new versions")
        owner.execute(
            "UPDATE ul_silver.observation_version SET valid_to=clock_timestamp() "
            "WHERE observation_id=%s",
            (observation,),
        )
        owner.execute(
            "INSERT INTO ul_silver.observation_version(observation_id,identity_id,provider,"
            "record_hash,normalisation_version,first_snapshot_id,last_snapshot_id,"
            "first_observed_at,last_observed_at,source_artifact_id,raw_record_locator,"
            "address_state,location_quality) SELECT %s,identity_id,provider,%s,"
            "'fixture-corrected',first_snapshot_id,last_snapshot_id,first_observed_at,"
            "last_observed_at,source_artifact_id,raw_record_locator,address_state,location_quality "
            "FROM ul_silver.observation_version WHERE observation_id=%s",
            (uuid4(), "c" * 64, observation),
        )
        corrected = owner.execute(
            "SELECT revision,first_observed_at,last_observed_at "
            "FROM ul_silver.observation_version WHERE identity_id=%s ORDER BY revision",
            (identity,),
        ).fetchall()
        assert corrected == [(1, now, now), (2, now, now)]
        expect_error(
            owner,
            "UPDATE ul_control.ingestion_run SET status='failed',failure_code='synthetic' "
            "WHERE run_id=%s",
            (run,),
            "23503",
        )
        owner.execute(
            (
                "INSERT INTO "
                "ul_silver.establishment(establishment_id,display_name,eligibility,primary_category,taxonomy_version,identity_rule_version,location_quality,observation_state,first_observed_at,last_observed_at)"
                " VALUES (%s,'Synthetic "
                "cafe','eligible','cafe','fixture','fixture','missing','observed',%s,%s)"
            ),
            (venue, now, now),
        )
        expect_error(
            owner,
            (
                "UPDATE ul_silver.establishment SET "
                "location=public.ST_GeogFromText('POINT(-0.14 "
                "50.82)'),location_quality='postcode_fallback',primary_spatial_eligible=true"
                " WHERE establishment_id=%s"
            ),
            (venue,),
            "23514",
        )
        expect_error(
            owner,
            "UPDATE ul_silver.establishment SET location_quality='withheld',"
            "address='private synthetic address' WHERE establishment_id=%s",
            (venue,),
            "23514",
        )
        owner.execute(
            (
                "INSERT INTO "
                "ul_silver.company(company_number,legal_name,legal_status,company_type,sic_codes,first_observed_at,last_observed_at)"
                " VALUES ('SC000001','Synthetic "
                "company','active','ltd',ARRAY['56101'],%s,%s)"
            ),
            (now, now),
        )
        owner.execute(
            (
                "INSERT INTO "
                "ul_control.review(review_id,review_type,establishment_id,company_number,reason,evidence_uri,rule_version)"
                " VALUES (%s,'company_link',%s,'SC000001','Synthetic "
                "review','https://example.invalid/proof','fixture')"
            ),
            (review, venue),
        )
        link_sql = (
            "INSERT INTO "
            "ul_silver.company_establishment_link(company_number,establishment_id,review_id,relationship,evidence_uri,valid_from)"
            " VALUES ('SC000001',%s,%s,'operator','https://example.invalid/proof',%s)"
        )
        expect_error(owner, link_sql, (venue, review, now), "23503")
        owner.execute(
            (
                "UPDATE ul_control.review SET decision='approved',reviewer='synthetic "
                "inspector',reviewed_at=%s WHERE review_id=%s"
            ),
            (now, review),
        )
        owner.execute(link_sql, (venue, review, now))
        event = uuid4()
        owner.execute(
            (
                "INSERT INTO "
                "ul_silver.change_event(event_id,event_key,entity_type,identity_id,event_type,snapshot_id,observed_at,evidence_fields,rule_version)"
                " VALUES (%s,%s,'source',%s,'source_changed',%s,%s,'{}','fixture')"
            ),
            (event, "b" * 64, identity, snapshot, now),
        )
        owner.execute(
            "INSERT INTO ul_silver.event_observation_evidence VALUES (%s,%s)", (event, observation)
        )
        expect_error(
            owner,
            "DELETE FROM ul_silver.observation_version WHERE observation_id=%s",
            (observation,),
            "23503",
        )
        expect_error(
            owner,
            (
                "UPDATE ul_silver.change_event SET "
                "retained_evidence_uri='s3://synthetic/bronze/expired.xml' WHERE "
                "event_id=%s"
            ),
            (event,),
            "23514",
        )
        owner.execute(
            (
                "INSERT INTO "
                "ul_control.data_release(release_id,status,geography_version,population_edition,population_reference_date,taxonomy_version,pipeline_version,manifest_uri,manifest_sha256)"
                " VALUES "
                "(%s,'building','fixture','fixture','2024-06-30','fixture','fixture','s3://synthetic/manifests/release.json',%s)"
            ),
            (release, "a" * 64),
        )
        owner.execute("INSERT INTO ul_control.release_slot VALUES ('building',%s)", (release,))
        owner.execute(
            (
                "INSERT INTO "
                "ul_gold.establishment(release_id,establishment_id,display_name,primary_category,location,location_quality,primary_spatial_eligible,observation_state)"
                " VALUES (%s,%s,'Synthetic cafe','cafe',public.ST_GeogFromText('POINT(-0.14"
                " 50.82)'),'source_point',true,'observed')"
            ),
            (release, venue),
        )
        expect_error(
            owner,
            "UPDATE ul_gold.establishment SET location=NULL,location_quality='withheld',"
            "primary_spatial_eligible=false,address='private synthetic address' "
            "WHERE establishment_id=%s",
            (venue,),
            "23514",
        )
        expect_error(
            owner,
            "UPDATE ul_gold.establishment SET "
            "location=public.ST_GeogFromText('SRID=4269;POINT(-0.14 50.82)') "
            "WHERE establishment_id=%s",
            (venue,),
            "22023",
        )
        expect_error(
            owner,
            "INSERT INTO ul_control.release_slot VALUES ('unbounded',%s)",
            (release,),
            "23514",
        )
        expect_error(
            owner,
            (
                "UPDATE ul_control.data_release SET status='published',published_at=%s "
                "WHERE release_id=%s"
            ),
            (now, release),
            "23514",
        )
        owner.commit()
        with connect(dsns["api"]) as api:
            assert api.execute(
                "SELECT count(*) FROM ul_api.establishment WHERE release_id=%s", (release,)
            ).fetchone() == (0,)
            assert api.execute(
                "SELECT count(*) FROM ul_api.establishments_within(%s,-0.14,50.82,500)", (release,)
            ).fetchone() == (0,)
        owner.execute(
            (
                "UPDATE ul_control.data_release SET "
                "status='published',quality_passed=true,published_at=%s WHERE release_id=%s"
            ),
            (now, release),
        )
        owner.execute(
            "UPDATE ul_control.release_slot SET slot='active' WHERE release_id=%s", (release,)
        )
        # Three additional synthetic points: exact edge, outside, approximate fallback.
        for distance, quality, eligible in (
            (500.0, "source_point", True),
            (500.1, "source_point", True),
            (0.0, "postcode_fallback", False),
        ):
            other = uuid4()
            owner.execute(
                (
                    "INSERT INTO "
                    "ul_silver.establishment(establishment_id,display_name,eligibility,primary_category,taxonomy_version,identity_rule_version,location_quality,observation_state,first_observed_at,last_observed_at)"
                    " VALUES (%s,'Synthetic spatial "
                    "probe','eligible','cafe','fixture','fixture','missing','observed',%s,%s)"
                ),
                (other, now, now),
            )
            owner.execute(
                (
                    "INSERT INTO "
                    "ul_gold.establishment(release_id,establishment_id,display_name,primary_category,location,location_quality,primary_spatial_eligible,observation_state)"
                    " VALUES (%s,%s,'Synthetic spatial "
                    "probe','cafe',public.ST_Project(public.ST_GeogFromText('POINT(-0.14 "
                    "50.82)'),%s,0),%s,%s,'observed')"
                ),
                (release, other, distance, quality, eligible),
            )
        # Selectivity makes the native geography index useful, not merely present.
        owner.execute(
            (
                "WITH ids AS (SELECT gen_random_uuid() AS id,i FROM generate_series(1,1000)"
                " i), entities AS (INSERT INTO "
                "ul_silver.establishment(establishment_id,display_name,eligibility,primary_category,taxonomy_version,identity_rule_version,location_quality,observation_state,first_observed_at,last_observed_at)"
                " SELECT id,'Synthetic index "
                "probe','eligible','cafe','fixture','fixture','missing','observed',now(),now()"
                " FROM ids RETURNING establishment_id) INSERT INTO "
                "ul_gold.establishment(release_id,establishment_id,display_name,primary_category,location,location_quality,primary_spatial_eligible,observation_state)"
                " SELECT %s,ids.id,'Synthetic index "
                "probe','cafe',public.ST_SetSRID(public.ST_MakePoint(-100+ids.i*0.01,0),4326)::public.geography,'source_point',true,'observed'"
                " FROM ids JOIN entities ON entities.establishment_id=ids.id"
            ),
            (release,),
        )
        owner.execute("ANALYZE ul_gold.establishment")
        owner.commit()
        with connect(dsns["api"]) as api:
            for radius, expected in ((250, 1), (500, 2), (1000, 3), (2000, 3)):
                assert api.execute(
                    "SELECT count(*) FROM ul_api.establishments_within(%s,-0.14,50.82,%s)",
                    (release, radius),
                ).fetchone() == (expected,)
            expect_error(
                api,
                "SELECT * FROM ul_api.establishments_within(%s,-0.14,50.82,999999)",
                (release,),
                "22023",
            )
        owner.execute("SET LOCAL enable_seqscan=off")
        plan = owner.execute(
            (
                "EXPLAIN SELECT g.establishment_id FROM ul_gold.establishment g JOIN "
                "ul_control.release_slot s USING(release_id) JOIN ul_control.data_release r"
                " USING(release_id) WHERE g.release_id=%s AND s.slot IN "
                "('active','previous') AND r.status='published' AND r.quality_passed AND "
                "g.primary_spatial_eligible AND g.observation_state IN "
                "('observed','pending_absence') AND "
                "public.ST_DWithin(g.location,public.ST_GeogFromText('POINT(-0.14 "
                "50.82)'),500.000001)"
            ),
            (release,),
        ).fetchall()
        assert any("gold_establishment_location_gist" in row[0] for row in plan)
        owner.rollback()
        # Future objects do not accidentally become readable by the application.
        owner.execute("CREATE TABLE ul_silver.future_private_probe(id integer)")
        owner.execute(
            "CREATE FUNCTION ul_api.future_private_probe() RETURNS bigint "
            "LANGUAGE sql SECURITY DEFINER SET search_path=pg_catalog "
            "AS 'SELECT count(*) FROM ul_silver.observation_version'"
        )
        owner.commit()
        with connect(dsns["api"]) as api:
            expect_error(api, "SELECT * FROM ul_silver.future_private_probe", (), "42501")
            expect_error(api, "SELECT ul_api.future_private_probe()", (), "42501")
        with connect(dsns["pipeline"]) as loader:
            loader.execute("INSERT INTO ul_silver.future_private_probe VALUES (1)")
        with connect(dsns["dbt"]) as dbt:
            assert dbt.execute(
                "SELECT count(*) FROM ul_silver.future_private_probe"
            ).fetchone() == (1,)
            dbt.execute("CREATE TABLE ul_gold.future_model_probe(id integer)")
        with connect(dsns["api"]) as api:
            expect_error(api, "SELECT * FROM ul_gold.future_model_probe", (), "42501")
    return {
        "synthetic_only": True,
        "source_generations_checked": True,
        "unchanged_versions_rejected": True,
        "processing_corrections_keep_observation_dates": True,
        "withheld_addresses_and_wrong_srids_rejected": True,
        "unreviewed_company_links_denied": True,
        "event_evidence_protected": True,
        "unpublished_data_hidden": True,
        "published_radius_edges_and_fallbacks_checked": True,
        "radius_query_uses_gist": True,
        "future_default_privileges_checked": True,
    }

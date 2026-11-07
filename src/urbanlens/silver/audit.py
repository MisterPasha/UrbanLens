"""M2 evidence-consistency audit; never a claim of a complete or error-free census."""

from __future__ import annotations

import json
from collections.abc import Callable
from typing import Any

import psycopg
from psycopg.rows import dict_row

from urbanlens.silver.eligibility import digest
from urbanlens.silver.history import required


def semantic_state(c: psycopg.Connection[Any]) -> dict[str, Any]:
    queries = {
        "canonical": "SELECT "
        "establishment_id::text,display_name,address,postcode,eligibility,primary_category,"
        "secondary_categories,duplicate_status,location_quality,primary_spatial_eligible,"
        "geography_version,area_code,observation_state,taxonomy_version,identity_rule_version,"
        "public.ST_AsEWKB(location::public.geometry)::text FROM "
        "ul_silver.current_establishment ORDER BY establishment_id",
        "source": "SELECT "
        "o.observation_id::text,i.provider,i.source_key,i.generation,s.scope_key,o.record_hash,"
        "o.last_snapshot_id::text,o.first_observed_at::text,o.last_observed_at::text FROM "
        "ul_silver.observation_version o JOIN ul_silver.source_identity i USING(identity_id) "
        "JOIN ul_control.source_scope s USING(scope_id) ORDER BY o.observation_id",
        "links": "SELECT identity_id::text,establishment_id::text,method,rule_version FROM "
        "ul_silver.establishment_source_link WHERE valid_to IS NULL AND "
        "decision='accepted' ORDER BY identity_id",
        "classifications": "SELECT "
        "observation_id::text,record_hash,decision_hash,taxonomy_sha256,reviews_sha256 FROM "
        "ul_silver.observation_classification ORDER BY observation_id",
        "locations": "SELECT observation_id::text,record_hash,decision_hash FROM "
        "ul_silver.observation_location ORDER BY observation_id",
        "geography": "SELECT "
        "geography_version,area_code,official_land_km2::text,md5(public.ST_AsEWKB(geom)) FROM "
        "ul_silver.area ORDER BY geography_version,area_code",
        "population": "SELECT to_jsonb(p) FROM ul_silver.population p ORDER BY "
        "geography_version,area_code,population_edition",
        "events": "SELECT event_key FROM ul_silver.change_event ORDER BY event_key",
        "company": "SELECT company_number,context FROM ul_silver.company ORDER BY company_number",
        "company_links": "SELECT "
        "company_number,establishment_id::text,relationship,review_id::text "
        "FROM ul_silver.company_establishment_link WHERE valid_to IS NULL "
        "ORDER BY company_number,establishment_id",
        "coverage": "SELECT universe,dimension,bucket,n,denominator,percent::text FROM "
        "ul_silver.establishment_coverage ORDER BY universe,dimension,bucket",
        "fields": "SELECT establishment_id::text,field_name,value_sha256,rule_version,detail "
        "FROM ul_silver.field_provenance WHERE valid_to IS NULL ORDER BY "
        "establishment_id,field_name",
        "history": "SELECT entity_type,entity_key,value,processing_sha256,support_sha256 FROM "
        "ul_silver.history_projection ORDER BY entity_type,entity_key",
    }
    parts = {}
    for name, query in queries.items():
        rows = c.execute(query).fetchall()
        rows.sort(key=lambda row: json.dumps(row, sort_keys=True, ensure_ascii=False))
        parts[name] = dict(rows=len(rows), sha256=digest(rows))
    return dict(
        parts=parts,
        sha256=digest(parts),
        omitted="Database-local processing timestamps, sizes and runtime logs",
    )


def inspect(c: psycopg.Connection[Any]) -> dict[str, Any]:
    with c.cursor(row_factory=dict_row) as cursor:
        cursor.execute(
            "SELECT e.*,public.ST_X(e.location::public.geometry) longitude,"
            "public.ST_Y(e.location::public.geometry) latitude,"
            "coalesce((SELECT string_agg(DISTINCT i.provider,'+' ORDER BY i.provider) "
            "FROM ul_silver.establishment_source_link l JOIN ul_silver.source_identity i "
            "USING(identity_id) "
            "WHERE l.establishment_id=e.establishment_id AND l.valid_to IS NULL AND "
            "l.decision='accepted'),'owner') source_mix "
            "FROM ul_silver.current_establishment e ORDER BY e.establishment_id"
        )
        rows = cursor.fetchall()
        cursor.execute(
            "SELECT establishment_id,field_name,detail FROM ul_silver.field_provenance WHERE "
            "valid_to IS NULL"
        )
        fields: dict[str, dict[str, Any]] = {}
        for item in cursor.fetchall():
            fields.setdefault(str(item["establishment_id"]), {})[item["field_name"]] = item[
                "detail"
            ]
    samples: dict[str, dict[str, Any]] = {}
    # Deterministic strata plus explicit owner corrections, rather than the first 20 rows.
    predicates: list[tuple[str, Callable[..., bool]]] = [
        (
            f"category/{cat}",
            lambda r, cat=cat: r["primary_category"] == cat and r["eligibility"] == "eligible",
        )
        for cat in ("cafe", "restaurant", "takeaway", "bakery", "pub_bar", "food_service")
    ]
    predicates += [
        (f"source/{source}", lambda r, source=source: r["source_mix"] == source)
        for source in ("fsa", "osm", "fsa+osm", "owner")
    ]
    predicates += [
        (f"coordinate/{quality}", lambda r, quality=quality: r["location_quality"] == quality)
        for quality in (
            "source_point",
            "point_on_surface",
            "postcode_fallback",
            "missing",
            "withheld",
            "unresolved",
        )
    ]
    predicates += [
        ("uncertain eligibility", lambda r: r["eligibility"] == "unresolved"),
        ("uncertain identity", lambda r: r["duplicate_status"] == "unresolved"),
        ("excluded nonhospitality", lambda r: r["eligibility"] == "excluded"),
    ]
    for label, eligible in predicates:
        for row in [r for r in rows if eligible(r)][:2]:
            key = str(row["establishment_id"])
            samples.setdefault(key, dict(row=row, strata=[]))["strata"].append(label)
    for row in rows:
        if row["display_name"] in {
            "Barbino",
            "Voya",
            "Nine Dragons",
            "Nom Pasta",
            "Okka Coffee",
            "Eat Central",
            "Pelicano",
            "Fluffy Fluffy Dessert Cafe",
            "Brighton Bites",
            "The Trellis",
        }:
            samples.setdefault(str(row["establishment_id"]), dict(row=row, strata=[]))[
                "strata"
            ].append("owner correction")
    sample_rows = []
    for key, item in samples.items():
        row = item["row"]
        values = {
            k: row[k]
            for k in (
                "display_name",
                "address",
                "postcode",
                "primary_category",
                "eligibility",
                "duplicate_status",
                "observation_state",
                "area_code",
            )
        }
        values["location"] = dict(
            longitude=row["longitude"], latitude=row["latitude"], quality=row["location_quality"]
        )
        provenance = fields.get(key, {})
        mismatches = [
            field
            for field, value in values.items()
            if field not in provenance or provenance[field]["value"] != value
        ]
        sample_rows.append(
            dict(
                establishment_id=key,
                strata=item["strata"],
                values=values,
                source_mix=row["source_mix"],
                field_provenance_matches=not mismatches,
                mismatches=mismatches,
                selection_reasons={k: v["selection_reason"] for k, v in provenance.items()},
                result="consistent with retained source/owner evidence"
                if not mismatches
                else "requires correction",
            )
        )
    spatial = c.execute(
        "SELECT count(*) FROM ul_silver.current_establishment e WHERE primary_spatial_eligible AND "
        "(eligibility<>'eligible' OR duplicate_status<>'resolved' OR "
        "observation_state='no_longer_observed' "
        "OR location_quality NOT IN ('source_point','point_on_surface') OR location IS NULL OR "
        "area_code IS DISTINCT FROM "
        "(ul_silver.covering_areas(public.ST_X(location::public.geometry),"
        "public.ST_Y(location::public.geometry)))[1])"
    ).fetchone()
    company = c.execute(
        "SELECT count(*) FROM ul_silver.company_establishment_link l LEFT JOIN "
        "ul_control.review r USING(review_id) "
        "WHERE l.valid_to IS NULL AND (r.decision IS DISTINCT FROM 'approved' OR "
        "r.review_type<>'company_link')"
    ).fetchone()
    assert spatial is not None and company is not None
    # All owner identity decisions, including distinct kitchens/pubs and close nonmatches.
    from urbanlens.silver.identity import Ref

    decisions = c.execute(
        "SELECT DISTINCT action FROM ul_silver.review_mapping WHERE action ? 'left' AND action "
        "? 'right'"
    ).fetchall()
    accepted = 0
    rejected = 0
    inconsistent = []
    nonmatches = []

    def endpoint(ref: Ref) -> Any:
        return c.execute(
            "SELECT l.establishment_id::text,o.name_normalised,o.address_normalised,i.source_key "
            "FROM ul_silver.source_identity i JOIN ul_silver.observation_version o "
            "USING(identity_id) "
            "JOIN ul_control.source_scope s USING(scope_id) JOIN "
            "ul_silver.establishment_source_link l USING(identity_id) "
            "WHERE i.provider=%s AND i.source_key=%s AND i.generation=%s AND s.scope_key=%s "
            "AND o.valid_to IS NULL AND l.valid_to IS NULL AND l.decision='accepted'",
            (ref.provider, ref.source_key, ref.generation, ref.scope),
        ).fetchone()

    for (action,) in decisions:
        if action["decision"] == "unsure":
            continue
        left, right = Ref.model_validate(action["left"]), Ref.model_validate(action["right"])
        a, b = endpoint(left), endpoint(right)
        same = action["decision"] in {"same", "same_place", "same place"}
        if a is None or b is None or ((a[0] == b[0]) != same):
            inconsistent.append(action.get("review_id"))
        elif same:
            accepted += 1
        else:
            rejected += 1
            nonmatches.append(
                dict(
                    review_id=action.get("review_id"),
                    left=dict(
                        zip(("establishment_id", "name", "address", "source_key"), a, strict=True)
                    ),
                    right=dict(
                        zip(("establishment_id", "name", "address", "source_key"), b, strict=True)
                    ),
                    reason=action.get("notes", action.get("reason")),
                    result="distinct canonical IDs preserved",
                )
            )
    with c.cursor(row_factory=dict_row) as cursor:
        cursor.execute(
            "SELECT a.source_key left_source,b.source_key right_source,p.method,"
            "la.establishment_id::text,lb.establishment_id::text right_establishment_id "
            "FROM ul_silver.identity_candidate p JOIN ul_silver.source_identity a "
            "ON a.identity_id=p.left_identity_id "
            "JOIN ul_silver.source_identity b ON b.identity_id=p.right_identity_id "
            "JOIN ul_silver.establishment_source_link la ON la.identity_id=a.identity_id "
            "JOIN ul_silver.establishment_source_link lb ON lb.identity_id=b.identity_id "
            "WHERE p.decision='accepted' AND a.provider='osm' AND b.provider='osm' "
            "AND split_part(a.source_key,'/',1)<>split_part(b.source_key,'/',1) "
            "AND la.valid_to IS NULL AND lb.valid_to IS NULL ORDER BY a.source_key LIMIT 10"
        )
        node_area = cursor.fetchall()
        cursor.execute(
            "SELECT establishment_id::text,display_name,address "
            "FROM ul_silver.current_establishment "
            "WHERE display_name='Pelicano' ORDER BY address"
        )
        branches = cursor.fetchall()
    passed = (
        len(sample_rows) >= min(20, len(rows))
        and all(s["field_provenance_matches"] for s in sample_rows)
        and not inconsistent
        and spatial[0] == 0
        and company[0] == 0
    )
    passed = passed and all(p["establishment_id"] == p["right_establishment_id"] for p in node_area)
    return dict(
        version="m2-audit-v1",
        passed=passed,
        review_basis="Evidence/provenance consistency and prior owner decisions; not "
        "independent field verification",
        sample_count=len(sample_rows),
        samples=sample_rows,
        all_identity_reviews=dict(
            accepted=accepted, rejected=rejected, contradictions=inconsistent
        ),
        plausible_nonmatches=nonmatches,
        accepted_node_area_examples=node_area,
        branch_examples=branches,
        live_reused_identifier_generations=required(
            c.execute(
                "SELECT count(*) FROM ul_silver.source_identity WHERE generation>1"
            ).fetchone()
        )[0],
        spatial_gate_violations=spatial[0],
        unreviewed_current_company_links=company[0],
        all_initially_published_company_links=required(
            c.execute(
                "SELECT count(*) FROM ul_gold.company_establishment l "
                "JOIN ul_control.release_slot r "
                "ON l.release_id=r.release_id WHERE r.slot='active'"
            ).fetchone()
        )[0],
        limits="A stratified sample is not an accuracy guarantee or a complete hospitality census",
    )

"""Assess current venue observations against pinned PostGIS boundaries and dated cache."""

from __future__ import annotations

from collections import Counter
from datetime import datetime
from typing import Any

import psycopg
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb

from urbanlens.ingestion.contracts import Artifact
from urbanlens.ingestion.interfaces import ArtifactStore
from urbanlens.silver.classification import save_json
from urbanlens.silver.eligibility import digest
from urbanlens.silver.load import artifact_id
from urbanlens.silver.locations import Candidate, LocationReview, select

ENGINE = "location-engine-v1"


def assess(
    connection: psycopg.Connection[Any], as_of: datetime, reviews: list[dict[str, Any]]
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    if as_of.tzinfo is None:
        raise ValueError("Assessment time must have a timezone")
    reviews = [LocationReview.model_validate(r).model_dump(mode="json") for r in reviews]
    if any(r["reviewed_on"] > as_of.date().isoformat() for r in reviews):
        raise ValueError("Location review is later than the assessment date")
    geography = connection.execute(
        "SELECT area_code,a.sha256 FROM ul_silver.area g JOIN ul_control.artifact a "
        "ON a.artifact_id=g.source_artifact_id WHERE geography_version='LSOA-2021' "
        "AND authority_code='E06000043' ORDER BY area_code"
    ).fetchall()
    if len(geography) != 165:
        raise ValueError("Expected all 165 pinned full-resolution LSOAs")
    by_key = {(r["provider"], r["scope"], r["source_key"]): r for r in reviews}
    if len(by_key) != len(reviews):
        raise ValueError("Duplicate location review identity")
    used = set()
    with connection.cursor(row_factory=dict_row) as cursor:
        cursor.execute(
            "SELECT o.observation_id,o.record_hash,o.provider,i.source_key,s.scope_key,"
            "o.location_quality,o.address_state,o.primary_spatial_eligible,"
            "public.ST_X(o.location::public.geometry) lon,"
            "public.ST_Y(o.location::public.geometry) lat,"
            "ul_silver.covering_areas(public.ST_X(o.location::public.geometry),"
            "public.ST_Y(o.location::public.geometry)) source_codes,"
            "ul_silver.covering_areas(p.longitude,p.latitude) postcode_codes,"
            "a.uri source_uri,a.sha256 source_sha256,o.raw_record_locator,"
            "o.source_fields,c.eligibility,c.decision_hash classification_hash,"
            "p.longitude postcode_lon,p.latitude postcode_lat,p.state postcode_state,"
            "p.observed_at postcode_observed_at,p.expires_at,p.authority_code,p.area_code,"
            "p.record_hash postcode_hash,p.dataset_version,p.edition_note,"
            "pa.uri postcode_uri,pa.sha256 postcode_sha256 "
            "FROM ul_silver.observation_version o JOIN ul_silver.source_identity i "
            "USING(identity_id) JOIN ul_control.source_scope s USING(scope_id) "
            "JOIN ul_control.artifact a ON a.artifact_id=o.source_artifact_id "
            "LEFT JOIN ul_silver.observation_classification c ON "
            "c.observation_id=o.observation_id AND c.record_hash=o.record_hash "
            "LEFT JOIN ul_silver.postcode_cache p ON p.postcode="
            "replace(upper(o.postcode_normalised),' ','') "
            "LEFT JOIN ul_control.artifact pa ON pa.artifact_id=p.source_artifact_id "
            "WHERE o.valid_to IS NULL AND o.provider IN ('fsa','osm') "
            "ORDER BY o.provider,s.scope_key,i.source_key"
        )
        inputs = cursor.fetchall()
    results = []
    counts: Counter[str] = Counter()
    for row in inputs:
        if row["eligibility"] is None:
            raise ValueError("Location assessment requires current step 21 classification")
        candidates = []
        flags = []
        if row["lon"] is not None and row["primary_spatial_eligible"]:
            candidates.append(
                Candidate(
                    longitude=row["lon"],
                    latitude=row["lat"],
                    evidence=row["source_uri"] + "#" + row["raw_record_locator"],
                )
            )
        cache_current = (
            row["expires_at"] is not None
            and row["postcode_observed_at"] <= as_of < row["expires_at"]
        )
        postcode_evidence = None
        if row["postcode_hash"]:
            postcode_evidence = dict(
                record_hash=row["postcode_hash"],
                uri=row["postcode_uri"],
                sha256=row["postcode_sha256"],
                state=row["postcode_state"],
                observed_at=row["postcode_observed_at"].isoformat(),
                expires_at=row["expires_at"].isoformat(),
                current=cache_current,
                dataset_version=row["dataset_version"],
                edition_note=row["edition_note"],
            )
        if (
            cache_current
            and row["postcode_state"] == "resolved"
            and row["postcode_lon"] is not None
        ):
            candidates.append(
                Candidate(
                    longitude=row["postcode_lon"],
                    latitude=row["postcode_lat"],
                    kind="approximate",
                    evidence=row["postcode_uri"],
                )
            )
        key = (row["provider"], row["scope_key"], row["source_key"])
        review = by_key.get(key)
        reviewed_index = None
        if review is not None:
            used.add(key)
            if review["expected_record_hash"] != row["record_hash"]:
                raise ValueError("Stale location review: source meaning changed")
            if (
                not review.get("evidence")
                or not review.get("reason")
                or not review.get("reviewed_on")
            ):
                raise ValueError("Location review requires dated evidence and a reason")
            if row["address_state"] == "withheld":
                raise ValueError("Cannot reconstruct withheld premises")
            candidates.append(
                Candidate(
                    longitude=review["longitude"],
                    latitude=review["latitude"],
                    kind="reviewed",
                    evidence=review["evidence"],
                )
            )
            reviewed_index = len(candidates) - 1
        selection = select(
            tuple(candidates),
            withheld=row["address_state"] == "withheld",
            reviewed_index=reviewed_index,
        )
        quality = selection.quality
        if quality == "missing" and row["location_quality"] == "invalid":
            quality = "invalid"
        selected = selection.selected
        codes: list[str] = []
        if selected is not None:
            if quality == "reviewed":
                found = connection.execute(
                    "SELECT ul_silver.covering_areas(%s,%s)",
                    (selected.longitude, selected.latitude),
                ).fetchone()
                assert found is not None
                codes = found[0]
            else:
                codes = (row["source_codes"] if selection.precise else row["postcode_codes"]) or []
        if len(codes) > 1:
            flags.append("shared_boundary_lowest_code_selected")
        if selected is not None and not codes:
            flags.append("outside_supported_coverage")
        contradiction = False
        if selection.precise and cache_current:
            if row["authority_code"] and row["authority_code"] != "E06000043":
                contradiction = True
                flags.append("published_postcode_authority_contradicts_supported_scope")
            elif codes and row["area_code"] and row["area_code"] not in codes:
                # A centroid across an LSOA boundary does not disprove the premises point.
                flags.append("postcode_centroid_lsoa_differs_not_a_premises_verification")
        if review is not None:
            contradiction = False  # An explicit, source-bound review selects the point.
        usable = selection.precise and bool(codes) and not contradiction
        result = dict(
            provider=key[0],
            scope=key[1],
            source_key=key[2],
            record_hash=row["record_hash"],
            engine_version=ENGINE,
            geography_sha256=digest(geography),
            classification_hash=row["classification_hash"],
            eligibility=row["eligibility"],
            source=dict(
                uri=row["source_uri"],
                sha256=row["source_sha256"],
                locator=row["raw_record_locator"],
                location_quality=row["location_quality"],
                field_sources=row["source_fields"].get("field_sources", {}),
            ),
            postcode_evidence=postcode_evidence,
            review=review,
            selection=selection.model_dump(mode="json"),
            quality=quality,
            longitude=selected.longitude if selected else None,
            latitude=selected.latitude if selected else None,
            covering_area_codes=codes if selection.precise else [],
            area_code=codes[0] if usable else None,
            geography_version="LSOA-2021" if usable else None,
            spatial_usable=usable,
            eligible_spatial_candidate=usable and row["eligibility"] == "eligible",
            primary_count_ready=False,
            review_required=contradiction or quality in {"invalid", "unresolved"},
            flags=flags,
            exclusion_scope=(
                "located"
                if usable
                else "outside_supported_coverage"
                if selected and not codes and selection.precise
                else "city_wide_unknown_membership"
            ),
        )
        result["decision_hash"] = digest(result)
        results.append(result)
        counts[quality] += 1
        counts["spatial_usable"] += usable
        counts["eligible_spatial_candidate"] += result["eligible_spatial_candidate"]
        counts["review_required"] += result["review_required"]
        counts["shared_boundary"] += len(codes) > 1
        counts["postcode_lsoa_difference"] += any("lsoa_differs" in f for f in flags)
    if used != set(by_key):
        raise ValueError("Location review does not identify a current venue observation")
    return results, dict(
        engine_version=ENGINE,
        assessed_as_of=as_of.isoformat(),
        venue_observations=len(results),
        counts=dict(counts),
        geography_sha256=digest(geography),
        primary_counts_published=False,
        definition="Source observations, not distinct establishments. Approximate locations "
        "have no primary LSOA or radius membership. Unknown exclusions are city-wide.",
    )


def run(
    connection: psycopg.Connection[Any],
    store: ArtifactStore,
    as_of: datetime,
    reviews: list[dict[str, Any]],
) -> dict[str, Any]:
    with connection.transaction():
        connection.execute("SELECT pg_advisory_xact_lock(192020,1)")
        rows, report = assess(connection, as_of, reviews)
        artifact: Artifact = save_json(
            store,
            "silver/locations/" + ENGINE,
            dict(
                engine_version=ENGINE,
                assessed_as_of=as_of.isoformat(),
                geography_sha256=report["geography_sha256"],
                records=rows,
            ),
        )
        aid = artifact_id(connection, artifact)
        current_ids = {
            (r[0], r[1], r[2], r[3]): r[4]
            for r in connection.execute(
                "SELECT o.provider,i.source_key,s.scope_key,o.record_hash,o.observation_id "
                "FROM ul_silver.observation_version o JOIN ul_silver.source_identity i "
                "USING(identity_id) JOIN ul_control.source_scope s USING(scope_id) "
                "WHERE o.valid_to IS NULL AND o.provider IN ('fsa','osm')"
            )
        }
        inserts = []
        for row in rows:
            current = current_ids.get(
                (row["provider"], row["source_key"], row["scope"], row["record_hash"])
            )
            if current is None:
                raise ValueError("Source observation changed during location assessment")
            values = (
                current,
                row["record_hash"],
                ENGINE,
                row["decision_hash"],
                row["quality"],
                row["longitude"],
                row["latitude"],
                row["geography_version"],
                row["area_code"],
                row["covering_area_codes"],
                row["spatial_usable"],
                row["eligible_spatial_candidate"],
                row["review_required"],
                row["exclusion_scope"],
                Jsonb(row),
                aid,
            )
            inserts.append(values[:5] + (row["longitude"],) + values[5:])
        with connection.cursor() as cursor:
            cursor.executemany(
                "INSERT INTO ul_silver.observation_location(observation_id,record_hash,"
                "engine_version,decision_hash,quality,location,geography_version,area_code,"
                "covering_area_codes,spatial_usable,eligible_spatial_candidate,review_required,"
                "exclusion_scope,decision,artifact_id) VALUES (%s,%s,%s,%s,%s,"
                "CASE WHEN %s::double precision IS NULL THEN NULL ELSE "
                "public.ST_SetSRID(public.ST_Point(%s,%s),4326)::public.geography END,"
                "%s,%s,%s,%s,%s,%s,%s,%s,%s) "
                "ON CONFLICT(observation_id) DO UPDATE SET "
                "engine_version=excluded.engine_version,decision_hash=excluded.decision_hash,"
                "quality=excluded.quality,location=excluded.location,"
                "geography_version=excluded.geography_version,area_code=excluded.area_code,"
                "covering_area_codes=excluded.covering_area_codes,"
                "spatial_usable=excluded.spatial_usable,"
                "eligible_spatial_candidate=excluded.eligible_spatial_candidate,"
                "review_required=excluded.review_required,exclusion_scope=excluded.exclusion_scope,"
                "decision=excluded.decision,artifact_id=excluded.artifact_id,"
                "processed_at=clock_timestamp() WHERE "
                "ul_silver.observation_location.decision_hash<>excluded.decision_hash",
                inserts,
            )
            changed = cursor.rowcount
    return {**report, "inserted_or_changed": changed, "artifact": artifact.model_dump(mode="json")}

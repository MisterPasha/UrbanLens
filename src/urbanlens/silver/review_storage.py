"""Dated approvals and processing provenance, independent from real-world event history."""

from __future__ import annotations

from typing import Any
from uuid import uuid4

import psycopg
from psycopg.types.json import Jsonb

from urbanlens.silver.eligibility import digest
from urbanlens.silver.load import artifact_id, identifier
from urbanlens.silver.review_mappings import Application


def register(c: psycopg.Connection[Any], app: Application, rows: dict[str, dict[str, Any]]) -> None:
    assert app.artifact is not None
    aid = artifact_id(c, app.artifact)
    c.execute(
        "INSERT INTO ul_control.review_definition VALUES (%s,%s,%s) ON CONFLICT DO NOTHING",
        (app.version, app.sha256, aid),
    )
    if c.execute(
        "SELECT sha256 FROM ul_control.review_definition WHERE version=%s", (app.version,)
    ).fetchone() != (app.sha256,):
        raise ValueError("Changed review definition requires a new version")
    actions = []
    for original_review in app.original.reviews:
        actions.append(
            (
                "pair/" + original_review.review_id,
                original_review.model_dump(mode="json"),
                original_review.reviewed_on,
                original_review.reviewer,
                original_review.reason,
            )
        )
    for mapping in app.decisions.reviews:
        actions.append(
            (
                "pair/" + mapping.review_id,
                mapping.model_dump(mode="json"),
                mapping.reviewed_on or mapping.confirmed_on,
                app.decisions.reviewer,
                mapping.notes or "Owner confirmed " + mapping.decision,
            )
        )
    for correction in app.corrections.corrections:
        actions.append(
            (
                "correction/" + correction.review_id,
                correction.model_dump(mode="json"),
                app.corrections.received_on,
                app.corrections.reviewer,
                correction.owner_statement,
            )
        )
    actions.append(
        (
            "policy",
            dict(
                scope=app.decisions.hospitality_scope_policy,
                name=app.decisions.name_selection_policy,
            ),
            app.decisions.confirmed_on,
            app.decisions.reviewer,
            "Owner hospitality scope and current OSM name preference",
        )
    )
    review_rows = []
    mapping_rows = []
    for label, action, day, reviewer, reason in actions:
        key = digest([app.sha256, label])
        rid = identifier("mapping-review/" + key)
        app.review_ids[label] = rid
        review_rows.append(
            (
                rid,
                reason,
                app.artifact.uri + "#" + label,
                app.artifact.sha256,
                app.version,
                reviewer,
                day,
            )
        )
        mapping_rows.append((key, rid, app.version, Jsonb(action)))
    with c.cursor() as cursor:
        cursor.executemany(
            "INSERT INTO ul_control.review(review_id,review_type,decision,reason,evidence_uri,"
            "evidence_sha256,rule_version,reviewer,reviewed_on) VALUES "
            "(%s,'identity','approved',%s,%s,%s,%s,%s,%s) ON CONFLICT(review_id) DO NOTHING",
            review_rows,
        )
        cursor.executemany(
            "INSERT INTO ul_silver.review_mapping VALUES (%s,%s,%s,%s,DEFAULT) "
            "ON CONFLICT(mapping_key) DO NOTHING",
            mapping_rows,
        )


def replacement_bodies(c: psycopg.Connection[Any], app: Application) -> list[dict[str, Any]]:
    result = []
    for correction in app.corrections.corrections:
        r = correction.field_overrides.replacement_candidate
        if r is None:
            continue
        key = correction.review_id
        existing = c.execute(
            "SELECT establishment_id,recorded_at FROM ul_silver.reviewed_replacement "
            "WHERE replacement_key=%s",
            (key,),
        ).fetchone()
        seed = next((e for e in app.registry_replacements if e["replacement_key"] == key), None)
        eid = str(existing[0]) if existing else seed["establishment_id"] if seed else str(uuid4())
        now_row = c.execute("SELECT clock_timestamp()").fetchone()
        assert now_row is not None
        stamp = (
            existing[1].isoformat()
            if existing
            else seed["first_observed_at"]
            if seed
            else now_row[0].isoformat()
        )
        body = dict(
            replacement_key=key,
            establishment_id=eid,
            display_name=r.name,
            address=r.address_from_reviewed_records,
            postcode=r.postcode_from_reviewed_records,
            eligibility="eligible",
            primary_category=r.primary_category,
            service_style=r.service_style,
            duplicate_status="unresolved",
            location_quality="missing",
            longitude=None,
            latitude=None,
            area_code=None,
            geography_version=None,
            primary_spatial_eligible=False,
            observation_state="observed",
            first_observed_at=stamp,
            last_observed_at=stamp,
            reported_on=app.corrections.received_on.isoformat(),
            timestamp_meaning=(
                "Owner report first recorded by the pipeline; "
                "not an opening date or provider acquisition"
            ),
            members=[],
            field_sources={},
            field_reviews={
                f: "correction/" + key
                for f in [
                    "display_name",
                    "address",
                    "postcode",
                    "primary_category",
                    "location",
                    "eligibility",
                    "duplicate_status",
                    "observation_state",
                    "area_code",
                ]
            },
            review_application_sha256=app.sha256,
            identity_continuity="unestablished replacement; not merged into prior businesses",
        )
        body["resolution_hash"] = digest(body)
        result.append(body)
    return result


def transition_links(
    c: psycopg.Connection[Any],
    app: Application,
    rows: dict[str, dict[str, Any]],
    assignments: dict[str, str],
    old: dict[Any, Any],
    aid: Any,
) -> None:
    moves = []
    for key, eid in assignments.items():
        identity = rows[key]["identity_id"]
        previous = old.get(identity)
        if previous is not None and str(previous) != eid:
            moves.append((identity, previous, eid))
    with c.cursor() as cursor:
        cursor.executemany(
            "UPDATE ul_silver.establishment_source_link SET valid_to=clock_timestamp() "
            "WHERE identity_id=%s AND valid_to IS NULL AND decision='accepted'",
            [(identity,) for identity, _, _ in moves],
        )
        cursor.executemany(
            "INSERT INTO ul_silver.identity_correction VALUES (%s,%s,%s,%s,%s,%s,DEFAULT) "
            "ON CONFLICT DO NOTHING",
            [
                (digest([app.sha256, str(i), str(a), b]), i, a, b, app.version, aid)
                for i, a, b in moves
            ],
        )
        retired = {str(a) for _, a, _ in moves} - set(assignments.values())
        cursor.executemany(
            "UPDATE ul_silver.establishment SET primary_spatial_eligible=false,"
            "duplicate_status='unresolved',identity_decision=identity_decision || "
            "jsonb_build_object('processing_superseded_by_review',%s::text) "
            "WHERE establishment_id=%s",
            [(app.sha256, eid) for eid in retired],
        )
        cursor.executemany(
            "UPDATE ul_silver.field_provenance SET valid_to=clock_timestamp() "
            "WHERE establishment_id=%s AND valid_to IS NULL",
            [(eid,) for eid in retired],
        )


def persist_fields(
    c: psycopg.Connection[Any],
    app: Application,
    entities: list[dict[str, Any]],
    rows: dict[str, dict[str, Any]],
    replacements: list[dict[str, Any]],
    aid: Any,
) -> dict[str, Any]:
    for e in replacements:
        c.execute(
            "INSERT INTO ul_silver.establishment(establishment_id,display_name,address,postcode,"
            "eligibility,primary_category,taxonomy_version,identity_rule_version,location_quality,"
            "primary_spatial_eligible,observation_state,first_observed_at,last_observed_at,duplicate_status,"
            "identity_decision,resolution_hash,resolution_artifact_id) "
            "VALUES "
            "(%s,%s,%s,%s,'eligible',%s,'owner-reviewed',%s,'missing',false,'observed',%s,%s,"
            "'unresolved',%s,%s,%s) ON CONFLICT(establishment_id) DO UPDATE SET "
            "display_name=excluded.display_name,address=excluded.address,postcode=excluded.postcode,"
            "primary_category=excluded.primary_category,identity_decision=excluded.identity_decision,"
            "resolution_hash=excluded.resolution_hash,"
            "resolution_artifact_id=excluded.resolution_artifact_id "
            "WHERE ul_silver.establishment.resolution_hash<>excluded.resolution_hash",
            (
                e["establishment_id"],
                e["display_name"],
                e["address"],
                e["postcode"],
                e["primary_category"],
                app.version,
                e["first_observed_at"],
                e["last_observed_at"],
                Jsonb(e),
                e["resolution_hash"],
                aid,
            ),
        )
        c.execute(
            "INSERT INTO ul_silver.reviewed_replacement VALUES (%s,%s,%s,%s,%s,%s) "
            "ON CONFLICT(replacement_key) DO NOTHING",
            (
                e["replacement_key"],
                e["establishment_id"],
                app.review_ids["correction/" + e["replacement_key"]],
                e["reported_on"],
                e["first_observed_at"],
                Jsonb(e),
            ),
        )
    existing = {
        (str(r[0]), r[1]): (r[2], r[3])
        for r in c.execute(
            "SELECT establishment_id,field_name,provenance_id,value_sha256 FROM "
            "ul_silver.field_provenance "
            "WHERE valid_to IS NULL"
        )
    }
    closes = []
    inserts = []
    for e in entities + replacements:
        for field in (
            "display_name",
            "address",
            "postcode",
            "primary_category",
            "location",
            "eligibility",
            "duplicate_status",
            "observation_state",
            "area_code",
        ):
            source = e["field_sources"].get(
                {"display_name": "name", "primary_category": "category"}.get(field, field)
            )
            sources = source if isinstance(source, list) else [source] if source else []
            sources = [k for k in sources if k in rows and k not in app.inactive]
            chosen = sources[0] if sources else None
            if field == "primary_category":
                chosen = next(
                    (
                        k
                        for k in sources
                        if rows[k]["classification"]["selection"]["primary_category"]
                        == e["primary_category"]
                    ),
                    chosen,
                )
            review_label = e.get("field_reviews", {}).get(field, "policy")
            review_id = app.review_ids[review_label]
            value = (
                e.get(field)
                if field != "location"
                else dict(
                    longitude=e["longitude"], latitude=e["latitude"], quality=e["location_quality"]
                )
            )
            detail = dict(
                value=value,
                source_keys=sources,
                review_label=review_label,
                review_definition_sha256=app.sha256,
                selection_reason="Explicit owner correction"
                if review_label != "policy"
                else "Current OSM name preference"
                if field == "display_name"
                else "Accepted source evidence with separate scope, duplicate and location gates",
            )
            sha = digest(detail)
            prior = existing.get((e["establishment_id"], field))
            if prior and prior[1] == sha:
                continue
            if prior:
                closes.append((prior[0],))
            inserts.append(
                (
                    e["establishment_id"],
                    field,
                    rows[chosen]["observation_id"] if chosen else None,
                    review_id,
                    app.version,
                    detail["selection_reason"],
                    rows[chosen]["last_observed_at"] if chosen else None,
                    sha,
                    Jsonb(detail),
                )
            )
    with c.cursor() as cursor:
        cursor.executemany(
            "UPDATE ul_silver.field_provenance SET valid_to=clock_timestamp() "
            "WHERE provenance_id=%s",
            closes,
        )
        cursor.executemany(
            "INSERT INTO ul_silver.field_provenance(establishment_id,field_name,observation_id,"
            "review_id,rule_version,selection_reason,source_time,value_sha256,detail,valid_from) "
            "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,clock_timestamp())",
            inserts,
        )
    return dict(
        review_definition=app.version,
        owner_review_mappings=len(app.review_ids),
        new_field_provenance=len(inserts),
        replacement_candidates=len(replacements),
        review_application_sha256=app.sha256,
    )

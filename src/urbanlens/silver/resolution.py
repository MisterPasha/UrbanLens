"""Persist source-level identity evidence and stable, private canonical establishments."""

from __future__ import annotations

from collections import Counter
from datetime import UTC, datetime
from typing import Any
from uuid import UUID, uuid4

import psycopg
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb

from urbanlens.ingestion.contracts import Artifact
from urbanlens.ingestion.interfaces import ArtifactStore
from urbanlens.silver.classification import save_json
from urbanlens.silver.eligibility import Decision, digest, resolve_linked, selected
from urbanlens.silver.identity import Entry, Ref, Reviews, Rules, distance, resolve
from urbanlens.silver.load import artifact_id, identifier
from urbanlens.silver.review_mappings import Application

ENGINE = "identity-engine-v1"


def inputs(connection: psycopg.Connection[Any]) -> tuple[list[Entry], dict[str, dict[str, Any]]]:
    with connection.cursor(row_factory=dict_row) as c:
        c.execute(
            "SELECT o.*,i.source_key,i.generation,s.scope_key,"
            "c.decision classification,l.decision location_decision,"
            "l.decision_hash location_hash,l.spatial_usable,"
            "st.observation_state source_state "
            "FROM ul_silver.observation_version o JOIN ul_silver.source_identity i "
            "USING(identity_id) JOIN ul_control.source_scope s USING(scope_id) "
            "LEFT JOIN ul_silver.observation_classification c ON "
            "c.observation_id=o.observation_id AND c.record_hash=o.record_hash "
            "LEFT JOIN ul_silver.observation_location l ON "
            "l.observation_id=o.observation_id AND l.record_hash=o.record_hash "
            "LEFT JOIN ul_silver.identity_scope_state st ON "
            "st.identity_id=o.identity_id AND st.scope_id=o.scope_id "
            "WHERE o.provider IN ('fsa','osm') AND o.valid_to IS NULL "
            "AND i.retired_at IS NULL ORDER BY o.provider,s.scope_key,i.source_key"
        )
        rows = c.fetchall()
    entries = []
    by_key = {}
    for row in rows:
        classification, location = row["classification"], row["location_decision"]
        if (
            classification is None
            or location is None
            or (location["classification_hash"] != classification["decision_hash"])
        ):
            raise ValueError("Resolver requires exact current step 21 and 22 assessments")
        selected = location["selection"]["selected"]
        precise = location["quality"] in {"source", "reviewed"} and selected is not None
        entry = Entry(
            provider=row["provider"],
            scope=row["scope_key"],
            source_key=row["source_key"],
            generation=row["generation"],
            record_hash=row["record_hash"],
            name=row["name_normalised"],
            address=row["address_normalised"],
            postcode=row["postcode_normalised"],
            longitude=selected["longitude"] if precise else None,
            latitude=selected["latitude"] if precise else None,
            tags=row["source_fields"].get("tags", {}),
            primary_category=classification["selection"]["primary_category"],
            location_hash=row["location_hash"],
            classification_hash=classification["decision_hash"],
            withheld=row["address_state"] == "withheld",
        )
        if entry.key in by_key:
            raise ValueError("Multiple current venue scopes need explicit identity resolution")
        entries.append(entry)
        by_key[entry.key] = row
    if not entries:
        raise ValueError("No venue observations loaded")
    return entries, by_key


def canonical(
    members: list[str],
    entries: dict[str, Entry],
    rows: dict[str, dict[str, Any]],
    flags: dict[str, list[str]],
    rules: Rules,
    reviews: Reviews,
    application: Application | None = None,
) -> dict[str, Any]:
    sources = sorted(members, key=lambda k: (entries[k].provider != "fsa", k))
    current = [k for k in sources if application is None or k not in application.inactive]
    selection = (
        resolve_linked(tuple(Decision.model_validate(rows[k]["classification"]) for k in current))
        if current
        else selected(
            "excluded", "owner-review-scope", "All source records excluded or no longer current"
        )
    )
    reasons = sorted({v for k in sources for v in flags.get(k, [])})
    precise = [k for k in current if entries[k].longitude is not None]
    location_conflict = any(
        (distance(entries[a], entries[b]) or 0) > rules.location_agreement_metres
        for i, a in enumerate(precise)
        for b in precise[i + 1 :]
    )
    if location_conflict:
        reasons.append("accepted_identity_has_conflicting_locations")
    withheld = any(entries[k].withheld for k in sources)
    chosen = next((k for k in current if rows[k]["spatial_usable"]), None)
    if chosen is None:
        chosen = next(
            (k for k in current if rows[k]["location_decision"]["quality"] == "approximate"), None
        )
    if withheld or location_conflict:
        chosen = None
    loc = rows[chosen]["location_decision"] if chosen else None
    name_source = next(
        (k for k in current if k in reviews.preferred_name_keys),
        next(
            (
                k
                for k in sorted(current, key=lambda k: (entries[k].provider != "osm", k))
                if entries[k].name
            ),
            next((k for k in sources if entries[k].name), sources[0]),
        )
        if application
        else next((k for k in sources if entries[k].name), sources[0]),
    )
    address_source = None if withheld else next((k for k in current if entries[k].address), None)
    postcode_source = None if withheld else next((k for k in current if entries[k].postcode), None)
    states = [rows[k]["source_state"] or "observed" for k in sources]
    state = (
        "observed"
        if "observed" in states
        else "pending_absence"
        if "pending_absence" in states
        else "no_longer_observed"
    )
    duplicate_status = "unresolved" if any(flags.get(k) for k in sources) else "resolved"
    ready = (
        selection.eligibility == "eligible"
        and duplicate_status == "resolved"
        and state != "no_longer_observed"
        and loc is not None
        and loc["spatial_usable"]
    )
    body = dict(
        engine_version=ENGINE,
        rules_version=rules.version,
        rules_sha256=digest(rules.model_dump(mode="json")),
        reviews_version=reviews.version,
        reviews_sha256=digest(reviews.model_dump(mode="json")),
        members=sources,
        display_name=entries[name_source].name,
        address=entries[address_source].address if address_source else None,
        postcode=entries[postcode_source].postcode if postcode_source else None,
        eligibility=selection.eligibility,
        primary_category=selection.primary_category,
        category_selection=selection.model_dump(mode="json"),
        duplicate_status=duplicate_status,
        review_reasons=reasons,
        location_quality=(
            "withheld"
            if withheld
            else "unresolved"
            if location_conflict
            else "postcode_fallback"
            if loc and loc["quality"] == "approximate"
            else "source_point"
            if loc and loc["quality"] == "reviewed"
            else rows[chosen]["location_quality"]
            if chosen
            else "missing"
        ),
        coordinate_quality=loc["quality"]
        if loc
        else "withheld"
        if withheld
        else "unresolved"
        if location_conflict
        else "missing",
        longitude=loc["longitude"] if loc else None,
        latitude=loc["latitude"] if loc else None,
        area_code=loc["area_code"] if loc else None,
        geography_version=loc["geography_version"] if loc else None,
        primary_spatial_eligible=bool(ready),
        observation_state=state,
        first_observed_at=min(rows[k]["first_observed_at"] for k in sources).isoformat(),
        last_observed_at=max(rows[k]["last_observed_at"] for k in sources).isoformat(),
        field_sources=dict(
            name=name_source,
            address=address_source,
            postcode=postcode_source,
            location=chosen,
            category=sources,
        ),
        source_urls=[
            (
                "https://ratings.food.gov.uk/business/"
                if entries[k].provider == "fsa"
                else "https://www.openstreetmap.org/"
            )
            + entries[k].source_key
            for k in sources
        ],
    )
    if application:
        application.apply_fields(body, members)
    return body


def run(
    connection: psycopg.Connection[Any],
    store: ArtifactStore,
    rules: Rules,
    reviews: Reviews,
    registry: dict[str, str] | None = None,
    registry_inputs: list[Entry] | None = None,
    application: Application | None = None,
) -> dict[str, Any]:
    with connection.transaction():
        connection.execute("SELECT pg_advisory_xact_lock(192020,1)")
        entries, rows = inputs(connection)
        by_key = {e.key: e for e in entries}
        if application:
            from urbanlens.silver.review_storage import register

            definition = save_json(store, "evidence/review-definitions", application.definition)
            reviews = application.bind(entries, rows, definition)
            register(connection, application, rows)
        if registry is not None and (
            registry_inputs is None
            or {e.key: e for e in registry_inputs} != by_key
            or set(registry) != set(by_key)
        ):
            raise ValueError("Shared canonical registry requires identical assessed input records")
        previous = {
            Entry.model_validate(r[0]).key: Entry.model_validate(r[0])
            for r in connection.execute("SELECT entry FROM ul_silver.resolution_input")
        }
        pairs, groups, flags = resolve(entries, rules, reviews, previous)
        if application:
            application.adjust_flags(pairs, groups, flags)
        current_links = dict(
            connection.execute(
                "SELECT identity_id,establishment_id FROM ul_silver.establishment_source_link "
                "WHERE decision='accepted' AND valid_to IS NULL"
            ).fetchall()
        )
        entities = []
        seen_ids = set()
        assignments = {}
        preferred_groups: dict[str, int] = {}
        group_options: dict[str, dict[int, list[str]]] = {}
        if application:
            membership = {k: i for i, m in enumerate(groups) for k in m}
            for key, row in rows.items():
                old = current_links.get(row["identity_id"])
                if old:
                    group_options.setdefault(str(old), {}).setdefault(membership[key], []).append(
                        key
                    )
            preferred_groups = {
                old: min(
                    options,
                    key=lambda i: (
                        -sum(k not in application.inactive for k in options[i]),
                        -len(options[i]),
                        groups[i][0],
                    ),
                )
                for old, options in group_options.items()
            }
        for group_index, members in enumerate(groups):
            existing = {
                str(current_links[rows[k]["identity_id"]])
                for k in members
                if rows[k]["identity_id"] in current_links
            }
            seeds = {(registry or {})[k] for k in members if k in (registry or {})}
            if not application and len(existing | seeds) > 1:
                raise ValueError("Canonical merge requires an explicit reviewed correction")
            owned = (
                {old for old in existing if preferred_groups.get(old) == group_index}
                if application
                else existing
            )
            if len(seeds) > 1:
                raise ValueError("Shared registry disagrees with reviewed identity graph")
            eid = next(iter(seeds), min(owned) if owned else str(uuid4()))
            UUID(eid)
            if eid in seen_ids:
                raise ValueError("Canonical split requires an explicit reviewed correction")
            seen_ids.add(eid)
            assignments.update({k: eid for k in members})
            body = canonical(members, by_key, rows, flags, rules, reviews, application)
            body["establishment_id"] = eid
            body["resolution_hash"] = digest(body)
            entities.append(body)
        replacements = []
        if application:
            from urbanlens.silver.review_storage import replacement_bodies

            replacements = replacement_bodies(connection, application)
        artifact = save_json(
            store,
            "silver/identity/" + ENGINE,
            dict(
                engine_version=ENGINE,
                rules=rules.model_dump(mode="json"),
                reviews=reviews.model_dump(mode="json"),
                inputs=[e.model_dump(mode="json") for e in entries],
                candidates=[p.model_dump(mode="json") for p in pairs],
                establishments=entities,
                **(
                    {"review_application": application.definition, "replacements": replacements}
                    if application
                    else {}
                ),
            ),
        )
        aid = artifact_id(connection, artifact)
        changed = 0
        with connection.cursor() as cursor:
            cursor.executemany(
                "INSERT INTO ul_silver.establishment(establishment_id,display_name,address,"
                "postcode,eligibility,primary_category,taxonomy_version,identity_rule_version,"
                "location,location_quality,primary_spatial_eligible,geography_version,area_code,"
                "observation_state,first_observed_at,last_observed_at,duplicate_status,"
                "identity_decision,resolution_hash,resolution_artifact_id) "
                "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,CASE WHEN %s::double precision IS NULL "
                "THEN NULL ELSE public.ST_SetSRID(public.ST_Point(%s,%s),4326)::public.geography "
                "END,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT(establishment_id) DO UPDATE SET "
                "display_name=excluded.display_name,address=excluded.address,postcode=excluded.postcode,"
                "eligibility=excluded.eligibility,primary_category=excluded.primary_category,"
                "taxonomy_version=excluded.taxonomy_version,identity_rule_version=excluded.identity_rule_version,"
                "location=excluded.location,location_quality=excluded.location_quality,"
                "primary_spatial_eligible=excluded.primary_spatial_eligible,"
                "geography_version=excluded.geography_version,area_code=excluded.area_code,"
                "observation_state=excluded.observation_state,first_observed_at=excluded.first_observed_at,"
                "last_observed_at=excluded.last_observed_at,duplicate_status=excluded.duplicate_status,"
                "identity_decision=excluded.identity_decision,resolution_hash=excluded.resolution_hash,"
                "resolution_artifact_id=excluded.resolution_artifact_id "
                "WHERE ul_silver.establishment.resolution_hash IS DISTINCT FROM "
                "excluded.resolution_hash",
                [
                    (
                        e["establishment_id"],
                        e["display_name"],
                        e["address"],
                        e["postcode"],
                        e["eligibility"],
                        e["primary_category"],
                        rows[e["members"][0]]["classification"]["taxonomy_version"],
                        rules.version,
                        e["longitude"],
                        e["longitude"],
                        e["latitude"],
                        e["location_quality"],
                        e["primary_spatial_eligible"],
                        e["geography_version"],
                        e["area_code"],
                        e["observation_state"],
                        e["first_observed_at"],
                        e["last_observed_at"],
                        e["duplicate_status"],
                        Jsonb(e),
                        e["resolution_hash"],
                        aid,
                    )
                    for e in entities
                ],
            )
            changed = cursor.rowcount
            if application:
                from urbanlens.silver.review_storage import transition_links

                transition_links(connection, application, rows, assignments, current_links, aid)
            cursor.executemany(
                "INSERT INTO "
                "ul_silver.establishment_source_link(establishment_id,identity_id,method,"
                "evidence_uri,rule_version,decision,valid_from) VALUES "
                "(%s,%s,'identity_resolution',"
                "%s,%s,'accepted',clock_timestamp()) ON CONFLICT(identity_id) "
                "WHERE valid_to IS NULL AND decision='accepted' DO NOTHING",
                [
                    (assignments[e.key], rows[e.key]["identity_id"], artifact.uri, rules.version)
                    for e in entries
                ],
            )
            # Bound current working state; full historical candidate sets remain in S3.
            cursor.execute(
                "DELETE FROM ul_silver.identity_candidate WHERE NOT (pair_id=ANY(%s::text[]))",
                ([p.pair_id for p in pairs],),
            )
            cursor.executemany(
                "INSERT INTO "
                "ul_silver.identity_candidate(pair_id,left_identity_id,right_identity_id,"
                "decision,method,detail,artifact_id) VALUES (%s,%s,%s,%s,%s,%s,%s) "
                "ON CONFLICT(pair_id) DO UPDATE SET decision=excluded.decision,"
                "method=excluded.method,detail=excluded.detail,artifact_id=excluded.artifact_id,"
                "assessed_at=clock_timestamp() WHERE "
                "ul_silver.identity_candidate.detail<>excluded.detail OR "
                "ul_silver.identity_candidate.artifact_id<>excluded.artifact_id",
                [
                    (
                        p.pair_id,
                        rows[p.left]["identity_id"],
                        rows[p.right]["identity_id"],
                        p.decision,
                        p.method,
                        Jsonb(p.model_dump(mode="json")),
                        aid,
                    )
                    for p in pairs
                ],
            )
            changed_candidates = cursor.rowcount
            cursor.executemany(
                "INSERT INTO "
                "ul_silver.resolution_input(identity_id,entry,artifact_id,rule_sha256,flags) "
                "VALUES (%s,%s,%s,%s,%s) ON CONFLICT(identity_id) DO UPDATE SET "
                "entry=CASE WHEN %s THEN ul_silver.resolution_input.entry ELSE excluded.entry END,"
                "artifact_id=excluded.artifact_id,rule_sha256=excluded.rule_sha256,flags=excluded.flags,"
                "processed_at=clock_timestamp() WHERE "
                "ul_silver.resolution_input.artifact_id<>excluded.artifact_id OR "
                "ul_silver.resolution_input.rule_sha256<>excluded.rule_sha256 OR "
                "ul_silver.resolution_input.flags<>excluded.flags",
                [
                    (
                        rows[e.key]["identity_id"],
                        Jsonb(e.model_dump(mode="json")),
                        aid,
                        digest(rules.model_dump(mode="json")),
                        flags.get(e.key, []),
                        "possible_source_id_reuse_new_generation_review_required"
                        in flags.get(e.key, []),
                    )
                    for e in entries
                ],
            )
            changed_inputs = cursor.rowcount
        review_report = {}
        if application:
            from urbanlens.silver.review_storage import persist_fields

            review_report = persist_fields(
                connection, application, entities, rows, replacements, aid
            )
        from urbanlens.silver.history import project

        history_events = project(connection, store, "establishment")
    return dict(
        engine_version=ENGINE,
        source_observations=len(entries),
        establishments=len(entities),
        accepted_source_links=len(entries),
        inserted_or_changed_establishments=changed,
        inserted_or_changed_candidates=changed_candidates,
        inserted_or_changed_inputs=changed_inputs,
        pair_decisions=dict(Counter(p.decision for p in pairs)),
        candidate_methods=dict(Counter(p.method for p in pairs)),
        eligibility=dict(Counter(e["eligibility"] for e in entities)),
        duplicate_status=dict(Counter(e["duplicate_status"] for e in entities)),
        primary_spatial_candidates=sum(e["primary_spatial_eligible"] for e in entities),
        flags=dict(Counter(f for reasons in flags.values() for f in set(reasons))),
        artifact=artifact.model_dump(mode="json"),
        gold_published=False,
        history_events=history_events,
        **review_report,
    )


def rotate_generation(
    connection: psycopg.Connection[Any],
    reference: dict[str, Any],
    reviewer: str,
    reason: str,
    evidence: Artifact,
) -> int:
    """Explicit reviewed retirement; replay source/21/22 before resolving the new generation."""
    entry = Ref.model_validate(reference)
    if not reviewer.strip() or not reason.strip():
        raise ValueError("Generation rotation needs a named reviewer and reason")
    with connection.transaction():
        connection.execute("SELECT pg_advisory_xact_lock(192020,1)")
        row = connection.execute(
            "SELECT i.identity_id FROM ul_silver.source_identity i "
            "JOIN ul_silver.observation_version o USING(identity_id) "
            "JOIN ul_control.source_scope s USING(scope_id) WHERE i.provider=%s AND "
            "i.source_key=%s AND i.generation=%s AND i.retired_at IS NULL AND "
            "o.record_hash=%s AND s.scope_key=%s AND o.valid_to IS NULL",
            (entry.provider, entry.source_key, entry.generation, entry.record_hash, entry.scope),
        ).fetchone()
        if row is None:
            raise ValueError("Generation review is stale or identifies no current source")
        old = row[0]
        artifact_id(connection, evidence)
        connection.execute(
            "INSERT INTO ul_control.review(review_type,source_identity_id,decision,reason,"
            "evidence_uri,evidence_sha256,rule_version,reviewer,reviewed_at) VALUES "
            "('identity',%s,'approved',%s,%s,%s,%s,%s,%s)",
            (old, reason, evidence.uri, evidence.sha256, ENGINE, reviewer, datetime.now(UTC)),
        )
        connection.execute(
            "UPDATE ul_silver.source_identity SET retired_at=clock_timestamp() WHERE "
            "identity_id=%s",
            (old,),
        )
        connection.execute(
            "UPDATE ul_silver.observation_version SET valid_to=clock_timestamp() "
            "WHERE identity_id=%s AND valid_to IS NULL",
            (old,),
        )
        connection.execute(
            "UPDATE ul_silver.establishment SET "
            "duplicate_status='unresolved',primary_spatial_eligible=false "
            "WHERE establishment_id IN (SELECT establishment_id FROM "
            "ul_silver.establishment_source_link "
            "WHERE identity_id=%s AND valid_to IS NULL)",
            (old,),
        )
        connection.execute(
            "UPDATE ul_silver.establishment_source_link SET "
            "valid_to=clock_timestamp() WHERE identity_id=%s AND valid_to IS NULL",
            (old,),
        )
        next_generation = entry.generation + 1
        connection.execute(
            "INSERT INTO "
            "ul_silver.source_identity(identity_id,provider,source_key,generation) "
            "VALUES (%s,%s,%s,%s)",
            (
                identifier(f"identity/{entry.provider}/{entry.source_key}/{next_generation}"),
                entry.provider,
                entry.source_key,
                next_generation,
            ),
        )
    return next_generation

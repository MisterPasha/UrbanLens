"""Monthly, evidence-bearing observation history, never asserted openings/closures."""

from __future__ import annotations

import json
from collections import Counter
from datetime import datetime
from typing import Any, Literal
from uuid import UUID

import psycopg
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb

from urbanlens.ingestion.contracts import Artifact, Snapshot, Stage
from urbanlens.ingestion.interfaces import ArtifactStore
from urbanlens.silver.classification import save_json
from urbanlens.silver.eligibility import digest
from urbanlens.silver.load import artifact_id, identifier

ENGINE = "observed-history-v1"
State = Literal["observed", "pending_absence", "no_longer_observed"]


def required(row: Any) -> Any:
    if row is None:
        raise ValueError("Required history evidence row is absent")
    return row


def transition(
    previous: State, present: bool, interrupted: bool = False
) -> tuple[State, str | None]:
    if present:
        return "observed", "reappeared" if previous == "no_longer_observed" else None
    if previous == "no_longer_observed":
        return previous, None
    if previous == "pending_absence" and not interrupted:
        return "no_longer_observed", "no_longer_observed"
    return "pending_absence", None


def emit(
    c: psycopg.Connection[Any],
    store: ArtifactStore,
    kind: str,
    key: str,
    event: str,
    before: Any,
    after: Any,
    evidence: dict[str, Any],
    observed_at: datetime,
    snapshot_id: UUID | None = None,
    source_date: str | None = None,
) -> None:
    body = dict(
        engine=ENGINE,
        entity_type=kind,
        entity_key=key,
        event_type=event,
        before=before,
        after=after,
        evidence=evidence,
        observed_at=observed_at.isoformat(),
        source_event_date=source_date,
        meaning="Observed source/processing change; not a verified opening or closure",
    )
    event_key = digest(body)
    artifact = save_json(store, "evidence/history", body)
    artifact_id(c, artifact)
    ids = [key if kind == domain else None for domain in ("establishment", "source", "company")]
    c.execute(
        "INSERT INTO ul_silver.change_event(event_id,event_key,entity_type,establishment_id,"
        "identity_id,company_number,event_type,snapshot_id,observed_at,source_event_date,"
        "before_value,after_value,evidence_fields,retained_evidence_uri,rule_version) "
        "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING",
        (
            identifier("event/" + event_key),
            event_key,
            kind,
            *ids,
            event,
            snapshot_id,
            observed_at,
            source_date,
            Jsonb(before),
            Jsonb(after),
            Jsonb(evidence),
            artifact.uri,
            ENGINE,
        ),
    )
    observations = sorted(
        set(evidence.get("observation_ids", []))
        | set((evidence.get("prior") or {}).get("observation_ids", []))
    )
    with c.cursor() as cursor:
        cursor.executemany(
            "INSERT INTO ul_silver.event_observation_evidence VALUES (%s,%s) ON CONFLICT DO "
            "NOTHING",
            [(identifier("event/" + event_key), oid) for oid in observations],
        )


def interrupt(
    c: psycopg.Connection[Any], store: ArtifactStore, snapshot: Snapshot, artifact: Artifact
) -> None:
    if snapshot.run.mode != "collect" or (
        snapshot.run.status == Stage.ACCEPTED and snapshot.completeness == "complete"
    ):
        raise ValueError("Only unsuccessful collection evidence can interrupt history")
    if Snapshot.model_validate_json(b"".join(store.read_verified(artifact))) != snapshot:
        raise ValueError("Failed-attempt artifact mismatch")
    scope = c.execute(
        "SELECT scope_id FROM ul_control.source_scope WHERE provider=%s AND scope_key=%s",
        (snapshot.cycle.provider.value, snapshot.cycle.scope),
    ).fetchone()
    if scope is None:
        return  # A future first successful collection establishes its own baseline.
    inserted = c.execute(
        "INSERT INTO ul_silver.history_attempt VALUES (%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING "
        "RETURNING run_id",
        (
            snapshot.run.run_id,
            scope[0],
            snapshot.cycle.cycle_id,
            snapshot.cycle.window_start,
            artifact_id(c, artifact),
        ),
    ).fetchone()
    if inserted:
        c.execute(
            "UPDATE ul_silver.identity_scope_state st SET absence_interrupted=true "
            "FROM ul_silver.history_scope h WHERE st.scope_id=h.scope_id AND h.scope_id=%s "
            "AND h.cycle_id<>%s AND h.window_start<=%s AND st.observation_state='pending_absence'",
            (scope[0], snapshot.cycle.cycle_id, snapshot.cycle.window_start),
        )


def source_value(row: dict[str, Any]) -> dict[str, Any]:
    fields = row["source_fields"]
    return dict(
        name=row["name_normalised"],
        address=row["address_normalised"],
        postcode=row["postcode_normalised"],
        address_state=row["address_state"],
        location=json.loads(row["point"]) if row["point"] else None,
        company=fields.get("company"),
        tags=fields.get("tags"),
        rating=fields.get("rating"),
        generation=row["generation"],
        record_hash=row["record_hash"],
    )


def assess(
    c: psycopg.Connection[Any],
    store: ArtifactStore,
    snapshot: Snapshot,
    present_keys: set[str],
    processing_version: str,
    compatible_queries: frozenset[tuple[str, str]] = frozenset(),
) -> None:
    """Called under the shared load transaction, after every expected part loaded."""
    if snapshot.cycle.provider.value not in {"fsa", "osm", "companies-house"}:
        return
    scope = c.execute(
        "SELECT scope_id FROM ul_control.source_scope WHERE provider=%s AND scope_key=%s",
        (snapshot.cycle.provider.value, snapshot.cycle.scope),
    ).fetchone()
    assert scope is not None
    scope_id = scope[0]
    with c.cursor(row_factory=dict_row) as cursor:
        cursor.execute(
            "SELECT h.*,cc.query_version FROM ul_silver.history_scope h "
            "JOIN ul_control.collection_cycle cc USING(cycle_id) WHERE h.scope_id=%s",
            (scope_id,),
        )
        head = cursor.fetchone()
        cursor.execute(
            "SELECT * FROM ul_silver.identity_scope_state WHERE scope_id=%s", (scope_id,)
        )
        previous = {str(r["identity_id"]): r for r in cursor.fetchall()}
        cursor.execute(
            "SELECT "
            "o.*,i.source_key,i.generation,public.ST_AsGeoJSON(o.location::public.geometry) "
            "point "
            "FROM ul_silver.observation_version o JOIN ul_silver.source_identity i "
            "USING(identity_id) "
            "WHERE o.scope_id=%s AND o.valid_to IS NULL AND i.retired_at IS NULL ORDER BY "
            "i.source_key",
            (scope_id,),
        )
        rows = cursor.fetchall()
    cycle = snapshot.cycle
    same = bool(head and head["cycle_id"] == cycle.cycle_id)
    if head and head["window_start"] > cycle.window_start:
        raise ValueError("History cannot assess an older collection")
    if head and head["query_sha256"] != cycle.query_sha256:
        permitted = (head["query_sha256"], cycle.query_sha256) in compatible_queries
        if (
            cycle.provider.value == "companies-house"
            and head["query_version"] == cycle.query_version
        ):
            # Digests include the changing edition/listing checksum, not just scope.
            permitted = True
            if cycle.scope.endswith("-profiles"):
                prior = required(
                    c.execute(
                        "SELECT a.uri,a.sha256,a.byte_count,a.media_type,a.compression "
                        "FROM ul_control.source_snapshot s JOIN ul_control.artifact a "
                        "ON a.uri=s.manifest_uri "
                        "WHERE s.snapshot_id=%s",
                        (head["snapshot_id"],),
                    ).fetchone()
                )
                artifact = Artifact(
                    uri=prior[0],
                    sha256=prior[1],
                    byte_count=prior[2],
                    media_type=prior[3],
                    compression=prior[4],
                )
                earlier = Snapshot.model_validate_json(b"".join(store.read_verified(artifact)))
                permitted = set(earlier.cycle.expected_partitions) == set(cycle.expected_partitions)
        if not permitted:
            raise ValueError("Changed source query needs a separate reviewed history scope")
    if head and not same and head["window_end"] > cycle.window_start:
        raise ValueError("Overlapping collection windows cannot count separate absences")
    if (
        head
        and not same
        and (head["window_start"].year, head["window_start"].month)
        == (cycle.window_start.year, cycle.window_start.month)
    ):
        raise ValueError("Monthly history cannot advance twice within one calendar month")
    # On later cycles, every partition must prove a fresh acquisition, not copied raw bytes.
    if head and not same:
        for part in snapshot.partitions:
            if (
                part.fetched_at is None
                or not cycle.window_start <= part.fetched_at < cycle.window_end
            ):
                raise ValueError("New history cycle requires acquisition within its own window")
            if not part.artifacts or any(
                f"/{snapshot.run.run_id}/" not in a.uri for a in part.artifacts
            ):
                raise ValueError("New cycle cannot reuse an old run's raw artifact")
    present = {r["source_key"]: r for r in rows if r["source_key"] in present_keys}
    if set(present) != present_keys or any(
        r["last_snapshot_id"] != snapshot.snapshot_id for r in present.values()
    ):
        raise ValueError("History membership must match the loaded accepted snapshot")
    observed_at = max(p.fetched_at for p in snapshot.partitions if p.fetched_at is not None)
    gap = bool(head and cycle.window_start > head["window_end"])
    updates = []
    for row in rows:
        identity = str(row["identity_id"])
        old = previous.get(identity)
        is_present = row["source_key"] in present_keys
        if old is None and not is_present:
            continue
        value = source_value(row) if is_present else required(old)["value"]
        before = old["value"] if old else None
        if same and old and not is_present:
            continue
        state: State = old["observation_state"] if old else "observed"
        event = None
        if not same:
            if old:
                state, event = transition(state, is_present, old["absence_interrupted"] or gap)
            elif head:
                event = "processing_correction" if row["generation"] > 1 else "newly_observed"
        if head and old is None and (same or row["generation"] > 1):
            event = "processing_correction"
        if old and is_present and before != value and event is None:
            event = (
                "processing_correction"
                if same or old["processing_version"] != processing_version
                else "source_changed"
            )
        if event:
            evidence = dict(
                provider=snapshot.cycle.provider.value,
                scope=snapshot.cycle.scope,
                cycle_id=str(cycle.cycle_id),
                snapshot_id=str(snapshot.snapshot_id),
                manifest_uri=required(
                    c.execute(
                        "SELECT manifest_uri FROM ul_control.source_snapshot WHERE snapshot_id=%s",
                        (snapshot.snapshot_id,),
                    ).fetchone()
                )[0],
                prior_snapshot_id=str(old["last_seen_snapshot_id"]) if old else None,
                observation_ids=sorted(
                    {
                        str(row["observation_id"]),
                        str(old["current_observation_id"]) if old else str(row["observation_id"]),
                    }
                ),
                processing_version=processing_version,
            )
            emit(
                c,
                store,
                "source",
                identity,
                event,
                dict(value=before, state=old["observation_state"]) if old else None,
                dict(value=value, state=state),
                evidence,
                observed_at,
                snapshot.snapshot_id,
            )
        updates.append(
            (
                scope_id,
                row["identity_id"],
                row["observation_id"] if is_present else required(old)["current_observation_id"],
                snapshot.snapshot_id if is_present else required(old)["last_seen_snapshot_id"],
                snapshot.snapshot_id,
                {"observed": 0, "pending_absence": 1, "no_longer_observed": 2}[state],
                state,
                Jsonb(value),
                processing_version,
                old["absence_interrupted"] if same and old else False,
            )
        )
    with c.cursor() as cursor:
        cursor.executemany(
            "INSERT INTO "
            "ul_silver.identity_scope_state(scope_id,identity_id,current_observation_id,"
            "last_seen_snapshot_id,last_assessed_snapshot_id,consecutive_complete_absences,obse"
            "rvation_state,"
            "value,processing_version,absence_interrupted) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) "
            "ON CONFLICT(scope_id,identity_id) DO UPDATE SET "
            "current_observation_id=excluded.current_observation_id,"
            "last_seen_snapshot_id=excluded.last_seen_snapshot_id,last_assessed_snapshot_id=exc"
            "luded.last_assessed_snapshot_id,"
            "consecutive_complete_absences=excluded.consecutive_complete_absences,observation_s"
            "tate=excluded.observation_state,"
            "value=excluded.value,processing_version=excluded.processing_version,absence_interr"
            "upted=excluded.absence_interrupted "
            "WHERE "
            "(ul_silver.identity_scope_state.current_observation_id,ul_silver.identity_scope_st"
            "ate.last_assessed_snapshot_id,"
            "ul_silver.identity_scope_state.value,ul_silver.identity_scope_state.absence_interr"
            "upted) IS DISTINCT FROM "
            "(excluded.current_observation_id,excluded.last_assessed_snapshot_id,excluded.value"
            ",excluded.absence_interrupted)",
            updates,
        )
    c.execute(
        "UPDATE ul_control.source_scope SET "
        "baseline_snapshot_id=coalesce(baseline_snapshot_id,%s) WHERE scope_id=%s",
        (snapshot.snapshot_id, scope_id),
    )
    c.execute(
        "INSERT INTO ul_silver.history_scope VALUES (%s,%s,%s,%s,%s,%s) ON CONFLICT(scope_id) "
        "DO UPDATE SET "
        "snapshot_id=excluded.snapshot_id,cycle_id=excluded.cycle_id,"
        "query_sha256=excluded.query_sha256,window_start=excluded.window_start,"
        "window_end=excluded.window_end WHERE "
        "ul_silver.history_scope.snapshot_id<>excluded.snapshot_id",
        (
            scope_id,
            snapshot.snapshot_id,
            cycle.cycle_id,
            cycle.window_start,
            cycle.window_end,
            cycle.query_sha256,
        ),
    )


def project(
    c: psycopg.Connection[Any], store: ArtifactStore, kind: Literal["establishment", "company"]
) -> dict[str, int]:
    """Capture post-resolution values; source support remains independent of legal status."""
    with c.transaction():
        c.execute("SELECT pg_advisory_xact_lock(192020,1)")
        baseline = (
            c.execute(
                "SELECT 1 FROM ul_silver.history_baseline WHERE entity_type=%s", (kind,)
            ).fetchone()
            is None
        )
        with c.cursor(row_factory=dict_row) as cursor:
            cursor.execute(
                "SELECT * FROM ul_silver.history_projection WHERE entity_type=%s", (kind,)
            )
            old = {r["entity_key"]: r for r in cursor.fetchall()}
            if kind == "establishment":
                cursor.execute(
                    "SELECT e.*,public.ST_AsGeoJSON(e.location::public.geometry) point FROM "
                    "ul_silver.establishment e "
                    "WHERE EXISTS (SELECT 1 FROM ul_silver.establishment_source_link l WHERE "
                    "l.establishment_id=e.establishment_id "
                    "AND l.valid_to IS NULL AND l.decision='accepted') OR EXISTS (SELECT 1 "
                    "FROM ul_silver.reviewed_replacement r "
                    "WHERE r.establishment_id=e.establishment_id) ORDER BY e.establishment_id"
                )
            else:
                cursor.execute(
                    "SELECT * FROM ul_silver.company WHERE context_sha256 IS NOT NULL ORDER BY "
                    "company_number"
                )
            rows = cursor.fetchall()
            cursor.execute(
                "SELECT l.establishment_id,st.*,i.generation FROM "
                "ul_silver.establishment_source_link l "
                "JOIN ul_silver.identity_scope_state st USING(identity_id) "
                "JOIN ul_silver.source_identity i USING(identity_id) WHERE l.valid_to IS NULL "
                "AND l.decision='accepted' "
                "AND i.retired_at IS NULL ORDER BY l.establishment_id,st.identity_id,st.scope_id"
            )
            supports: dict[str, list[dict[str, Any]]] = {}
            for item in cursor.fetchall():
                supports.setdefault(str(item["establishment_id"]), []).append(item)
        counts: Counter[str] = Counter()
        updates = []
        seen = set()
        known_identities = {
            identity
            for previous in old.values()
            for identity in previous["evidence"].get("identities", [])
        }
        for row in rows:
            key = str(row["establishment_id"] if kind == "establishment" else row["company_number"])
            seen.add(key)
            prev = old.get(key)
            if kind == "establishment":
                support = supports.get(key, [])
                if support:
                    states = {s["observation_state"] for s in support}
                    expected = (
                        "observed"
                        if "observed" in states
                        else "pending_absence"
                        if "pending_absence" in states
                        else "no_longer_observed"
                    )
                    if row["observation_state"] != expected:
                        raise ValueError(
                            "Resolve current source states before projecting venue history"
                        )
                value = {
                    k: row[k]
                    for k in (
                        "display_name",
                        "address",
                        "postcode",
                        "eligibility",
                        "primary_category",
                        "duplicate_status",
                        "location_quality",
                        "area_code",
                        "observation_state",
                    )
                }
                value["location"] = json.loads(row["point"]) if row["point"] else None
                processing = digest(
                    dict(
                        taxonomy=row["taxonomy_version"],
                        identity=row["identity_rule_version"],
                        reviews=row["identity_decision"].get("review_application_sha256"),
                    )
                )
                evidence: dict[str, Any] = dict(
                    observation_ids=sorted({str(s["current_observation_id"]) for s in support}),
                    snapshots=sorted({str(s["last_assessed_snapshot_id"]) for s in support}),
                    identities=sorted({str(s["identity_id"]) for s in support}),
                    generations=sorted({s["generation"] for s in support}),
                    artifact_id=str(row["resolution_artifact_id"])
                    if row["resolution_artifact_id"]
                    else None,
                )
                observed_at = row["last_observed_at"]
                publishable = (
                    value["eligibility"] == "eligible" and value["duplicate_status"] == "resolved"
                )
            else:
                ctx = row["context"]
                value = {
                    k: ctx[k]
                    for k in (
                        "legal_name",
                        "legal_status",
                        "company_type",
                        "sic_codes",
                        "incorporation",
                        "cessation",
                        "registered_office",
                    )
                }
                processing = digest(ENGINE + "/company-context-v1")
                evidence = dict(
                    observation_ids=[ctx["source_observation_id"]],
                    sources=[
                        dict(
                            observation_id=s["observation_id"],
                            record_hash=s["record_hash"],
                            observed_at=s["observed_at"],
                        )
                        for s in ctx["sources"]
                    ],
                )
                observed_at = row["last_observed_at"]
                publishable = True
            support_hash = digest(evidence)
            event = None
            before = prev["value"] if prev else None
            quiet_pending_change = bool(
                kind == "establishment"
                and before is not None
                and before.get("observation_state") in {"observed", "pending_absence"}
                and value.get("observation_state") in {"observed", "pending_absence"}
                and {k: v for k, v in before.items() if k != "observation_state"}
                == {k: v for k, v in value.items() if k != "observation_state"}
            )
            if not baseline:
                if prev is None:
                    if kind == "establishment":
                        source_new = bool(
                            evidence["observation_ids"]
                            and c.execute(
                                "SELECT 1 FROM ul_silver.change_event ev JOIN "
                                "ul_silver.event_observation_evidence ee USING(event_id) "
                                "WHERE ev.entity_type='source' AND "
                                "ev.event_type='newly_observed' AND "
                                "ee.observation_id=ANY(%s::uuid[]) LIMIT 1",
                                (evidence["observation_ids"],),
                            ).fetchone()
                        )
                        reassigned = bool(known_identities.intersection(evidence["identities"]))
                        event = (
                            "processing_correction"
                            if reassigned
                            else "newly_observed"
                            if source_new and publishable
                            else "processing_correction"
                            if any(g > 1 for g in evidence["generations"])
                            else None
                        )
                    # Company observation dates are distinct from incorporation dates.
                    else:
                        event = (
                            "newly_observed"
                            if c.execute(
                                "SELECT 1 FROM ul_silver.change_event ev JOIN "
                                "ul_silver.event_observation_evidence ee USING(event_id) "
                                "WHERE ev.entity_type='source' AND ev.event_type='newly_observed' "
                                "AND ee.observation_id=ANY(%s::uuid[]) LIMIT 1",
                                (evidence["observation_ids"],),
                            ).fetchone()
                            else None
                        )
                elif before != value and not quiet_pending_change:
                    source_change = True
                    if kind == "establishment":
                        changed_snapshots = sorted(
                            set(evidence["snapshots"]) - set(prev["evidence"].get("snapshots", []))
                        )
                        source_change = bool(
                            c.execute(
                                "SELECT 1 FROM ul_silver.change_event WHERE entity_type='source' "
                                "AND event_type<>'processing_correction' "
                                "AND identity_id=ANY(%s::uuid[]) "
                                "AND snapshot_id=ANY(%s::uuid[]) LIMIT 1",
                                (evidence["identities"], changed_snapshots),
                            ).fetchone()
                        )
                    correction = (
                        processing != prev["processing_sha256"]
                        or support_hash == prev["support_sha256"]
                        or bool(required(before).get("retired"))
                        or not source_change
                    )
                    if correction:
                        event = "processing_correction"
                    elif kind == "company":
                        event = (
                            "company_dissolved"
                            if str(value["legal_status"]).lower() == "dissolved"
                            and str(required(before)["legal_status"]).lower() != "dissolved"
                            else "source_changed"
                        )
                    elif publishable:
                        a, b = required(before)["observation_state"], value["observation_state"]
                        event = (
                            "no_longer_observed"
                            if b == "no_longer_observed" and a != b
                            else "reappeared"
                            if a == "no_longer_observed" and b == "observed"
                            else None
                        )
                        before_fields = {
                            k: x for k, x in required(before).items() if k != "observation_state"
                        }
                        after_fields = {k: x for k, x in value.items() if k != "observation_state"}
                        if event is None and before_fields != after_fields:
                            event = "source_changed"
            if event:
                full_evidence = dict(
                    evidence,
                    prior=prev["evidence"] if prev else None,
                    processing_sha256=processing,
                    support_sha256=support_hash,
                )
                # Date disappearance at its confirming collection, not the last sighting.
                snapshots = evidence.get("snapshots", [])
                if snapshots:
                    observed_at = required(
                        c.execute(
                            "SELECT max(fetched_at) FROM ul_control.source_snapshot WHERE "
                            "snapshot_id=ANY(%s::uuid[])",
                            (snapshots,),
                        ).fetchone()
                    )[0]
                source_date = value["cessation"]["value"] if event == "company_dissolved" else None
                emit(
                    c,
                    store,
                    kind,
                    key,
                    event,
                    before,
                    value,
                    full_evidence,
                    observed_at,
                    source_date=source_date,
                )
                counts[event] += 1
            updates.append((kind, key, Jsonb(value), processing, support_hash, Jsonb(evidence)))
        # Retired merged/split identities are processing corrections, never vanished competitors.
        for key, prev in old.items():
            if key not in seen and prev["value"].get("retired") is not True:
                value = dict(retired=True, reason="No current accepted canonical membership")
                if kind == "establishment":
                    timestamp = required(
                        c.execute(
                            "SELECT last_observed_at FROM ul_silver.establishment WHERE "
                            "establishment_id=%s",
                            (key,),
                        ).fetchone()
                    )[0]
                    emit(
                        c,
                        store,
                        kind,
                        key,
                        "processing_correction",
                        prev["value"],
                        value,
                        prev["evidence"],
                        timestamp,
                    )
                    counts["processing_correction"] += 1
                    updates.append(
                        (
                            kind,
                            key,
                            Jsonb(value),
                            prev["processing_sha256"],
                            prev["support_sha256"],
                            Jsonb(prev["evidence"]),
                        )
                    )
        with c.cursor() as cursor:
            cursor.executemany(
                "INSERT INTO ul_silver.history_projection VALUES (%s,%s,%s,%s,%s,%s) ON "
                "CONFLICT(entity_type,entity_key) DO UPDATE SET "
                "value=excluded.value,processing_sha256=excluded.processing_sha256,support_sha2"
                "56=excluded.support_sha256,evidence=excluded.evidence "
                "WHERE "
                "(ul_silver.history_projection.value,ul_silver.history_projection.processing_sh"
                "a256,ul_silver.history_projection.support_sha256) "
                "IS DISTINCT FROM "
                "(excluded.value,excluded.processing_sha256,excluded.support_sha256)",
                updates,
            )
        c.execute(
            "INSERT INTO ul_silver.history_baseline(entity_type) VALUES (%s) ON CONFLICT DO "
            "NOTHING",
            (kind,),
        )
        return dict(counts)

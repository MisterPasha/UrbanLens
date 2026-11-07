"""Transactional bounded working state; full monthly records remain in S3."""

from __future__ import annotations

import json
from collections import Counter
from datetime import UTC, datetime
from typing import Any
from uuid import NAMESPACE_URL, UUID, uuid5

import psycopg
from psycopg.types.json import Jsonb

from urbanlens.ingestion.contracts import Artifact, Snapshot, Stage
from urbanlens.ingestion.interfaces import ArtifactStore
from urbanlens.silver.models import Observation
from urbanlens.silver.pipeline import Batch, Bundle, verified_observations


def identifier(value: str) -> UUID:
    return uuid5(NAMESPACE_URL, "urbanlens/silver/" + value)


def artifact_id(connection: psycopg.Connection[Any], artifact: Artifact) -> UUID:
    if not artifact.uri.startswith("s3://"):
        raise ValueError("Database provenance requires portable S3 evidence")
    identity = identifier(artifact.uri)
    layer = artifact.uri.split("/", 3)[-1].split("/", 1)[0]
    if layer not in {"bronze", "silver", "evidence", "manifests"}:
        raise ValueError("Unsupported evidence layer")
    row = connection.execute(
        "INSERT INTO ul_control.artifact(artifact_id,uri,sha256,byte_count,compression,"
        "media_type,layer) "
        "VALUES (%s,%s,%s,%s,%s,%s,%s) ON CONFLICT(uri) DO UPDATE SET uri=excluded.uri "
        "WHERE ul_control.artifact.sha256=excluded.sha256 AND "
        "ul_control.artifact.byte_count=excluded.byte_count "
        "RETURNING artifact_id",
        (
            identity,
            artifact.uri,
            artifact.sha256,
            artifact.byte_count,
            artifact.compression,
            artifact.media_type,
            layer,
        ),
    ).fetchone()
    if row is None:
        raise ValueError("Artifact URI has conflicting bytes")
    return UUID(str(row[0]))


def project_snapshot(
    connection: psycopg.Connection[Any], snapshot: Snapshot, manifest: Artifact
) -> UUID:
    if (
        snapshot.run.status != Stage.ACCEPTED
        or snapshot.completeness != "complete"
        or not snapshot.validation.passed
    ):
        raise ValueError("Database load requires an accepted complete source")
    cycle, run = snapshot.cycle, snapshot.run
    scope = identifier(f"scope/{cycle.provider.value}/{cycle.scope}")
    connection.execute(
        "INSERT INTO ul_control.source_scope(scope_id,provider,scope_key) VALUES (%s,%s,%s) "
        "ON CONFLICT(provider,scope_key) DO NOTHING",
        (scope, cycle.provider.value, cycle.scope),
    )
    actual = connection.execute(
        "SELECT scope_id FROM ul_control.source_scope WHERE provider=%s AND scope_key=%s",
        (cycle.provider.value, cycle.scope),
    ).fetchone()
    assert actual is not None
    scope = actual[0]
    current = connection.execute(
        "SELECT s.snapshot_id,c.window_start FROM ul_control.source_head h "
        "JOIN ul_control.source_snapshot s USING(snapshot_id) JOIN ul_control.collection_cycle "
        "c USING(cycle_id) "
        "WHERE h.scope_id=%s",
        (scope,),
    ).fetchone()
    if current and current[0] != snapshot.snapshot_id and current[1] >= cycle.window_start:
        raise ValueError("Refusing to replace a newer/different accepted source head")
    connection.execute(
        "INSERT INTO ul_control.collection_cycle(cycle_id,scope_id,window_start,window_end,"
        "query_version,query_sha256,source_edition) "
        "VALUES (%s,%s,%s,%s,%s,%s,%s) ON CONFLICT(cycle_id) DO NOTHING",
        (
            cycle.cycle_id,
            scope,
            cycle.window_start,
            cycle.window_end,
            cycle.query_version,
            cycle.query_sha256,
            cycle.source_edition,
        ),
    )
    connection.execute(
        "INSERT INTO ul_control.ingestion_run(run_id,cycle_id,scope_id,mode,pipeline_version,"
        "status,attempt,started_at,ended_at) "
        "VALUES (%s,%s,%s,'collect',%s,'accepted',%s,%s,%s) ON CONFLICT(run_id) DO NOTHING",
        (
            run.run_id,
            cycle.cycle_id,
            scope,
            run.pipeline_version,
            run.attempt,
            run.started_at,
            run.ended_at,
        ),
    )
    fetched = max(p.fetched_at for p in snapshot.partitions if p.fetched_at is not None)
    dates = [p.source_extract_date for p in snapshot.partitions if p.source_extract_date]
    totals = [
        sum(getattr(p.counts, k) or 0 for p in snapshot.partitions)
        for k in ("raw", "parsed", "accepted", "rejected")
    ]
    connection.execute(
        "INSERT INTO ul_control.source_snapshot(snapshot_id,cycle_id,scope_id,provider,run_id,"
        "manifest_uri,manifest_sha256,"
        "fetched_at,source_published_at,source_extract_date,validation_version,"
        "validation_passed,expected_parts,complete_parts,raw_count,parsed_count,accepted_count,"
        "rejected_count) "
        "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,true,%s,%s,%s,%s,%s,%s) ON "
        "CONFLICT(snapshot_id) DO NOTHING",
        (
            snapshot.snapshot_id,
            cycle.cycle_id,
            scope,
            cycle.provider.value,
            run.run_id,
            manifest.uri,
            manifest.sha256,
            fetched,
            None,
            max(dates) if dates else None,
            snapshot.validation.rule_version,
            len(cycle.expected_partitions),
            len(snapshot.partitions),
            *totals,
        ),
    )
    connection.execute(
        "INSERT INTO ul_control.source_head VALUES (%s,%s) ON CONFLICT(scope_id) DO UPDATE SET "
        "snapshot_id=excluded.snapshot_id",
        (scope, snapshot.snapshot_id),
    )
    return UUID(str(scope))


def source_fields(record: Observation, batch: Batch) -> dict[str, Any]:
    data = record.model_dump(mode="json")
    # Large source footprints live in the portable artifact and area geometry table.
    # This allowlist is working state, not a second full monthly file copy.
    fields = {
        k: data[k]
        for k in (
            "schema_version",
            "record_kind",
            "rating",
            "company",
            "postcode",
            "tags",
            "field_sources",
            "source_published_at",
            "source_extracted_at",
            "ingested_at",
            "source_extract_date",
            "source_event_at",
        )
    }
    fields["source_values"] = {
        k: v for k, v in record.source_values.items() if k not in {"geometry", "original_geometry"}
    }
    fields["silver_artifact"] = batch.records.model_dump(mode="json")
    fields["field_states"] = {
        "name": record.name.state,
        "address": record.address.state,
        "postcode": record.postcode.state,
    }
    if record.area:
        fields["area"] = record.area.model_dump(mode="json", exclude={"geometry"})
    if len(json.dumps(fields).encode()) > 60_000:
        raise ValueError("Normalised working state exceeds bounded record budget")
    return fields


def load_observations(
    connection: psycopg.Connection[Any],
    batch: Batch,
    records: list[Observation],
    scope: UUID,
    artifacts: dict[str, UUID],
) -> Counter[str]:
    counts: Counter[str] = Counter()
    if not records:
        return counts
    keys = [r.source_key for r in records]
    provider = batch.provider.value
    identities = {
        r[0]: (r[1], r[2])
        for r in connection.execute(
            "SELECT source_key,identity_id,generation FROM ul_silver.source_identity WHERE "
            "provider=%s AND source_key=ANY(%s) AND retired_at IS NULL",
            (provider, keys),
        )
    }
    generations = dict(
        connection.execute(
            "SELECT source_key,max(generation) FROM ul_silver.source_identity "
            "WHERE provider=%s AND source_key=ANY(%s) GROUP BY source_key",
            (provider, keys),
        ).fetchall()
    )
    new = []
    for key in keys:
        if key not in identities:
            generation = generations.get(key, 0) + 1
            identity = identifier(f"identity/{provider}/{key}/{generation}")
            identities[key] = (identity, generation)
            new.append((identity, provider, key, generation))
    with connection.cursor() as cursor:
        cursor.executemany(
            "INSERT INTO ul_silver.source_identity(identity_id,provider,source_key,generation) "
            "VALUES (%s,%s,%s,%s)",
            new,
        )
    previous = {
        r[0]: r[1:]
        for r in connection.execute(
            "SELECT identity_id,observation_id,record_hash,last_snapshot_id,last_observed_at,"
            "normalisation_version "
            "FROM ul_silver.observation_version WHERE scope_id=%s AND identity_id=ANY(%s) AND "
            "valid_to IS NULL",
            (scope, [v[0] for v in identities.values()]),
        )
    }
    inserts, close, unchanged = [], [], []
    for record in records:
        identity = identities[record.source_key][0]
        old = previous.get(identity)
        if old and old[1] == record.record_hash:
            unchanged.append((record.snapshot_id, record.observed_at, old[0], record.observed_at))
            counts["unchanged"] += 1
            continue
        if old:
            if old[3] > record.observed_at or (
                old[2] == record.snapshot_id and old[4] == record.normalisation_version
            ):
                raise ValueError("Stale snapshot or nondeterministic same-rule correction")
            close.append((old[0],))
            counts["changed"] += 1
        else:
            counts["inserted"] += 1
        observation_key = (
            f"observation/{record.snapshot_id}/{provider}/{record.source_key}/"
            f"{record.normalisation_version}"
        )
        # Keep existing generation-one IDs stable. An explicitly reviewed reuse
        # can replay the same evidence into a new identity without PK collisions.
        generation = identities[record.source_key][1]
        if generation > 1:
            observation_key += f"/generation/{generation}"
        observation_id = identifier(observation_key)
        location = record.location
        geo = (
            None
            if location.longitude is None
            else json.dumps(
                {"type": "Point", "coordinates": [location.longitude, location.latitude]}
            )
        )
        inserts.append(
            (
                observation_id,
                identity,
                provider,
                record.record_hash,
                record.normalisation_version,
                record.snapshot_id,
                record.snapshot_id,
                record.observed_at,
                record.observed_at,
                artifacts[record.evidence[0].artifact.uri],
                record.evidence[0].locator,
                record.name.raw,
                record.name.value,
                Jsonb(record.address.raw),
                record.address.value,
                record.postcode.raw,
                record.postcode.value,
                "withheld"
                if record.address.state == "withheld"
                else "published"
                if record.address.value
                else "missing",
                geo,
                location.quality,
                location.primary_spatial_eligible,
                Jsonb(source_fields(record, batch)),
                scope,
            )
        )
    with connection.cursor() as cursor:
        cursor.executemany(
            "UPDATE ul_silver.observation_version SET valid_to=clock_timestamp() WHERE "
            "observation_id=%s",
            close,
        )
        cursor.executemany(
            "INSERT INTO ul_silver.observation_version(observation_id,identity_id,provider,"
            "record_hash,normalisation_version,"
            "first_snapshot_id,last_snapshot_id,first_observed_at,last_observed_at,"
            "source_artifact_id,raw_record_locator,"
            "name_raw,name_normalised,address_raw,address_normalised,postcode_raw,"
            "postcode_normalised,address_state,"
            "location,location_quality,primary_spatial_eligible,source_fields,scope_id) VALUES "
            "(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,"
            "public.ST_SetSRID(public.ST_GeomFromGeoJSON(%s),4326)::public.geography,%s,%s,%s,"
            "%s)",
            inserts,
        )
        cursor.executemany(
            "UPDATE ul_silver.observation_version SET last_snapshot_id=%s,last_observed_at=%s "
            "WHERE observation_id=%s AND last_observed_at<=%s",
            unchanged,
        )
    return counts


def load(
    connection: psycopg.Connection[Any],
    store: ArtifactStore,
    bundle: Bundle,
    *,
    interruptions: tuple[tuple[Snapshot, Artifact], ...] = (),
    compatible_queries: frozenset[tuple[str, str]] = frozenset(),
) -> dict[str, Any]:
    from urbanlens.silver.history import assess, interrupt

    counts: Counter[str] = Counter()
    with connection.transaction():
        connection.execute("SELECT pg_advisory_xact_lock(192020,1)")
        for failed, failure_artifact in interruptions:
            interrupt(connection, store, failed, failure_artifact)
        memberships: dict[UUID, set[str]] = {}
        partitions: dict[UUID, set[str]] = {}
        snapshots: dict[UUID, Snapshot] = {}
        artifacts: dict[str, UUID] = {}
        scopes: dict[UUID, UUID] = {}
        seen: set[tuple[str, str, str]] = set()
        for batch in bundle.batches:
            records = list(verified_observations(store, batch))
            for record in records:
                key = (record.provider.value, record.scope, record.source_key)
                if key in seen:
                    raise ValueError("Duplicate source identity across partitions")
                seen.add(key)
            evidence = {a.uri: a for r in records for a in (e.artifact for e in r.evidence)}
            evidence.update(
                {batch.records.uri: batch.records, batch.source_manifest.uri: batch.source_manifest}
            )
            for uri, artifact in evidence.items():
                if uri not in artifacts:
                    artifacts[uri] = artifact_id(connection, artifact)
            if batch.snapshot is not None:
                snapshot_id = batch.snapshot.snapshot_id
                snapshots[snapshot_id] = batch.snapshot
                memberships.setdefault(snapshot_id, set()).update(r.source_key for r in records)
                parts = partitions.setdefault(snapshot_id, set())
                if batch.partition in parts:
                    raise ValueError("Repeated snapshot partition")
                parts.add(batch.partition)
                if snapshot_id not in scopes:
                    original = Snapshot.model_validate_json(
                        b"".join(store.read_verified(batch.source_manifest))
                    )
                    if original != batch.snapshot:
                        raise ValueError("Source manifest differs from selected snapshot")
                    scopes[snapshot_id] = project_snapshot(
                        connection, batch.snapshot, batch.source_manifest
                    )
                counts.update(
                    load_observations(connection, batch, records, scopes[snapshot_id], artifacts)
                )
                area_rows, populations = [], []
                for r in records:
                    if r.area is None:
                        continue
                    a = r.area
                    area_rows.append(
                        (
                            a.geography_version,
                            r.source_key,
                            r.name.value,
                            a.authority_code,
                            json.dumps(a.geometry),
                            a.official_land_km2,
                            artifacts[r.evidence[0].artifact.uri],
                        )
                    )
                    populations.append(
                        (
                            a.geography_version,
                            r.source_key,
                            a.population_edition,
                            a.reference_date,
                            a.population,
                            a.age_0_15,
                            a.age_16_64,
                            a.age_65_plus,
                            artifacts[r.evidence[0].artifact.uri],
                        )
                    )
                with connection.cursor() as cursor:
                    cursor.executemany(
                        "INSERT INTO ul_silver.area(geography_version,area_code,area_name,"
                        "authority_code,geography_year,geom,official_land_km2,"
                        "source_artifact_id) "
                        "VALUES (%s,%s,%s,%s,2021,"
                        "public.ST_Multi(public.ST_SetSRID(public.ST_GeomFromGeoJSON(%s),"
                        "4326)),%s,%s) "
                        "ON CONFLICT(geography_version,area_code) DO UPDATE SET "
                        "source_artifact_id=excluded.source_artifact_id "
                        "WHERE ul_silver.area.area_name=excluded.area_name "
                        "AND ul_silver.area.authority_code=excluded.authority_code "
                        "AND ul_silver.area.official_land_km2=excluded.official_land_km2 "
                        "AND public.ST_Equals(ul_silver.area.geom,excluded.geom)",
                        area_rows,
                    )
                    if cursor.rowcount != len(area_rows):
                        raise ValueError(
                            "Changed geography requires a new pinned geography version"
                        )
                    cursor.executemany(
                        "INSERT INTO ul_silver.population VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s) "
                        "ON CONFLICT(geography_version,area_code,population_edition,"
                        "reference_date) DO UPDATE SET "
                        "source_artifact_id=excluded.source_artifact_id "
                        "WHERE ul_silver.population.population=excluded.population "
                        "AND ul_silver.population.age_0_15=excluded.age_0_15 "
                        "AND ul_silver.population.age_16_64=excluded.age_16_64 "
                        "AND ul_silver.population.age_65_plus=excluded.age_65_plus",
                        populations,
                    )
                    if cursor.rowcount != len(populations):
                        raise ValueError(
                            "Changed population requires a new pinned population edition"
                        )
            else:
                rows = []
                for r in records:
                    p = r.postcode_result
                    if p is None or r.postcode.value is None:
                        raise ValueError("Only retained, valid postcode queries enter the cache")
                    expiry = p.expires_at
                    if expiry is None:
                        raise ValueError("Retained postcode cache requires its original expiry")
                    rows.append(
                        (
                            r.source_key,
                            r.record_hash,
                            r.observed_at,
                            expiry,
                            p.state,
                            r.location.longitude,
                            r.location.latitude,
                            p.authority_code,
                            p.lsoa21,
                            p.dataset_version,
                            p.edition_note,
                            artifacts[r.evidence[0].artifact.uri],
                            artifacts[batch.records.uri],
                        )
                    )
                with connection.cursor() as cursor:
                    cursor.executemany(
                        "INSERT INTO ul_silver.postcode_cache VALUES (%s,%s,%s,%s,%s,%s,%s,%s,"
                        "%s,%s,%s,%s,%s) "
                        "ON CONFLICT(postcode) DO UPDATE SET record_hash=excluded.record_hash,"
                        "observed_at=excluded.observed_at,expires_at=excluded.expires_at,"
                        "state=excluded.state,longitude=excluded.longitude,"
                        "latitude=excluded.latitude,authority_code=excluded.authority_code,"
                        "area_code=excluded.area_code,dataset_version=excluded.dataset_version,"
                        "edition_note=excluded.edition_note,"
                        "source_artifact_id=excluded.source_artifact_id,"
                        "silver_artifact_id=excluded.silver_artifact_id "
                        "WHERE ul_silver.postcode_cache.observed_at<=excluded.observed_at",
                        rows,
                    )
                counts["postcode_cache"] += len(rows)
        for sid, snapshot in snapshots.items():
            if partitions[sid] != set(snapshot.cycle.expected_partitions):
                raise ValueError("History/load requires every expected snapshot partition")
            expected = sum(p.counts.accepted or 0 for p in snapshot.partitions)
            if len(memberships[sid]) != expected:
                raise ValueError("Snapshot membership count differs from accepted partition totals")
            assess(
                connection,
                store,
                snapshot,
                memberships[sid],
                bundle.normalisation_version,
                compatible_queries,
            )
        connection.execute(
            "SELECT count(*) FROM ul_silver.observation_version WHERE valid_to IS NULL"
        )
    return {
        "loaded_at": datetime.now(UTC).isoformat(),
        "counts": dict(counts),
        "source_observation_dates_preserved": True,
        "canonical_entities_created": False,
        "history_assessed": True,
        "gold_published": False,
    }

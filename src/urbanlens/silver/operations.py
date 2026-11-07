"""Private Silver coverage, logical fingerprints and conservative retention."""

from __future__ import annotations

import calendar
from datetime import UTC, datetime
from typing import Any
from urllib.parse import quote, unquote, urlsplit, urlunsplit

import psycopg
from psycopg import sql
from psycopg.rows import dict_row

from urbanlens.silver.eligibility import digest


def cutoff(now: datetime) -> datetime:
    """Twelve calendar months, including leap-day handling."""
    year = now.year - 1
    return now.replace(year=year, day=min(now.day, calendar.monthrange(year, now.month)[1]))


def retained_schemas(c: psycopg.Connection[Any]) -> list[str]:
    schemas = ["ul_control", "ul_silver", "ul_gold", "ul_api"]
    available = c.execute(
        "SELECT 1 FROM information_schema.columns WHERE table_schema='ul_control' "
        "AND table_name='data_release' AND column_name='model_schema'"
    ).fetchone()
    if available:
        rows = c.execute(
            "SELECT d.release_id,d.model_schema FROM ul_control.release_slot s JOIN "
            "ul_control.data_release d USING(release_id) WHERE s.slot IN ('active','previous') "
            "AND d.model_schema IS NOT NULL AND d.cleaned_at IS NULL"
        ).fetchall()
        for identifier, schema in rows:
            if schema != "ul_candidate_" + identifier.hex:
                raise ValueError("Unknown retained schema")
            c.execute(
                "SELECT pg_advisory_xact_lock_shared(192033,hashtext(%s))", (str(identifier),)
            )
            schemas.append(schema)
    return schemas


def fingerprint(c: psycopg.Connection[Any]) -> dict[str, Any]:
    tables = c.execute(
        "SELECT schemaname,tablename FROM pg_tables WHERE schemaname=ANY(%s) "
        "ORDER BY schemaname,tablename",
        (retained_schemas(c),),
    ).fetchall()
    result = {}
    for schema, table in tables:
        row = c.execute(
            sql.SQL(
                "SELECT count(*),md5(coalesce(string_agg(h,'' ORDER BY h),'')) "
                "FROM (SELECT md5(to_jsonb(t)::text) h FROM {}.{} t) v"
            ).format(sql.Identifier(schema), sql.Identifier(table))
        ).fetchone()
        assert row is not None
        result[f"{schema}.{table}"] = dict(rows=row[0], content_md5=row[1])
    return dict(tables=result, sha256=digest(result))


def coverage(c: psycopg.Connection[Any]) -> dict[str, Any]:
    with c.cursor(row_factory=dict_row) as cursor:
        cursor.execute(
            "SELECT * FROM ul_silver.establishment_coverage ORDER BY universe,dimension,bucket"
        )
        measures = cursor.fetchall()
        measures.sort(key=lambda row: (row["universe"], row["dimension"], row["bucket"]))
        for row in measures:
            row["percent"] = float(row["percent"]) if row["percent"] is not None else None
        cursor.execute(
            "SELECT sc.provider,sc.scope_key,s.snapshot_id::text,s.fetched_at::text,"
            "s.raw_count,s.parsed_count,s.accepted_count,s.rejected_count "
            "FROM ul_control.source_head h JOIN ul_control.source_snapshot s USING(snapshot_id) "
            "JOIN ul_control.source_scope sc ON sc.scope_id=h.scope_id ORDER BY "
            "sc.provider,sc.scope_key"
        )
        sources = cursor.fetchall()
        cursor.execute(
            "SELECT 'eligibility_excluded' reason,count(*) n FROM "
            "ul_silver.current_establishment WHERE eligibility='excluded' "
            "UNION ALL SELECT 'eligibility_unresolved',count(*) FROM "
            "ul_silver.current_establishment WHERE eligibility='unresolved' "
            "UNION ALL SELECT 'duplicate_unresolved',count(*) FROM "
            "ul_silver.current_establishment WHERE duplicate_status='unresolved' "
            "UNION ALL SELECT 'not_primary_spatial',count(*) FROM "
            "ul_silver.current_establishment WHERE NOT primary_spatial_eligible"
        )
        exclusions = cursor.fetchall()
    size = c.execute("SELECT pg_database_size(current_database())").fetchone()
    assert size is not None
    return dict(
        version="silver-coverage-v1",
        sources=sources,
        measures=measures,
        exclusions=dict(
            universe="current canonical roster",
            overlap=True,
            warning="Reasons overlap; never add these counts",
            reasons=exclusions,
        ),
        postcode_cache=dict(
            c.execute(
                "SELECT state,count(*) FROM ul_silver.postcode_cache GROUP BY state"
            ).fetchall()
        ),
        database_bytes=size[0],
        database_size_is_not_a_coverage_measure=True,
    )


def prune(
    c: psycopg.Connection[Any], *, now: datetime | None = None, apply: bool = False
) -> dict[str, Any]:
    """Prune only old history; foreign keys are an additional last line of protection."""
    now = now or datetime.now(UTC)
    if now > datetime.now(UTC):
        raise ValueError("Retention cannot use a future assessment time")
    boundary = cutoff(now)
    with c.transaction():
        c.execute("SELECT pg_advisory_xact_lock(192020,1)")
        events = c.execute(
            "SELECT event_id FROM ul_silver.change_event WHERE observed_at<%s", (boundary,)
        ).fetchall()
        fields = c.execute(
            "SELECT provenance_id FROM ul_silver.field_provenance WHERE valid_to<%s", (boundary,)
        ).fetchall()
        if apply:
            c.execute("DELETE FROM ul_silver.change_event WHERE observed_at<%s", (boundary,))
            c.execute("DELETE FROM ul_silver.field_provenance WHERE valid_to<%s", (boundary,))
            c.execute(
                "DELETE FROM ul_silver.company_context_run WHERE processed_at<%s "
                "AND artifact_id<>(SELECT artifact_id FROM ul_silver.company_context_run "
                "ORDER BY processed_at DESC LIMIT 1)",
                (boundary,),
            )
        # Discover every FK to observations, so later migrations cannot silently weaken protection.
        refs = c.execute(
            "SELECT ns.nspname,cl.relname,a.attname FROM pg_constraint f "
            "JOIN pg_class cl ON cl.oid=f.conrelid JOIN pg_namespace ns ON ns.oid=cl.relnamespace "
            "JOIN pg_attribute a ON a.attrelid=f.conrelid AND a.attnum=f.conkey[1] "
            "WHERE f.contype='f' AND f.confrelid='ul_silver.observation_version'::regclass "
            "AND array_length(f.conkey,1)=1"
        ).fetchall()
        # Classification and location are dependent assessments, not retention roots.
        refs = [
            r for r in refs if r[1] not in {"observation_classification", "observation_location"}
        ]
        protected: list[sql.Composable] = []
        for s, t, a in refs:
            if t == "event_observation_evidence":
                protected.append(
                    sql.SQL(
                        "NOT EXISTS(SELECT 1 FROM ul_silver.event_observation_evidence r "
                        "JOIN ul_silver.change_event e USING(event_id) "
                        "WHERE r.observation_id=o.observation_id AND e.observed_at>={})"
                    ).format(sql.Literal(boundary))
                )
            elif t == "field_provenance":
                protected.append(
                    sql.SQL(
                        "NOT EXISTS(SELECT 1 FROM ul_silver.field_provenance r "
                        "WHERE r.observation_id=o.observation_id "
                        "AND (r.valid_to IS NULL OR r.valid_to>={}))"
                    ).format(sql.Literal(boundary))
                )
            else:
                protected.append(
                    sql.SQL("NOT EXISTS(SELECT 1 FROM {}.{} r WHERE r.{}=o.observation_id)").format(
                        sql.Identifier(s), sql.Identifier(t), sql.Identifier(a)
                    )
                )
        protected.append(
            sql.SQL(
                "NOT EXISTS(SELECT 1 FROM ul_silver.identity_scope_state r WHERE "
                "r.current_observation_id=o.observation_id)"
            )
        )
        query = sql.SQL(
            "SELECT o.observation_id FROM ul_silver.observation_version o WHERE o.valid_to<%s AND "
        ) + sql.SQL(" AND ").join(protected)
        observations = [r[0] for r in c.execute(query, (boundary,))]
        if apply and observations:
            c.execute(
                "DELETE FROM ul_silver.observation_version WHERE observation_id=ANY(%s)",
                (observations,),
            )
        return dict(
            version="silver-retention-v1",
            cutoff=boundary.isoformat(),
            applied=apply,
            expired_events=len(events),
            expired_closed_provenance=len(fields),
            removable_changed_observations=len(observations),
            current_entities_links_reviews_and_observation_support_preserved=True,
        )


def json_uris(value: Any) -> set[str]:
    if isinstance(value, dict):
        return set().union(*(json_uris(v) for v in value.values()))
    if isinstance(value, (list, tuple)):
        return set().union(*(json_uris(v) for v in value))
    if isinstance(value, str) and value.startswith("s3://"):
        parsed = urlsplit(value)
        return {
            urlunsplit(
                (parsed.scheme, parsed.netloc, quote(unquote(parsed.path), safe="/-_.~"), "", "")
            )
        }
    return set()


def evidence_roots(c: psycopg.Connection[Any]) -> set[str]:
    """Retained relational rows own evidence; the artifact catalogue alone is not a root."""
    roots: set[str] = set()
    refs = c.execute(
        "SELECT ns.nspname,cl.relname,a.attname FROM pg_constraint f "
        "JOIN pg_class cl ON cl.oid=f.conrelid JOIN pg_namespace ns ON ns.oid=cl.relnamespace "
        "JOIN pg_attribute a ON a.attrelid=f.conrelid AND a.attnum=ANY(f.conkey) "
        "WHERE f.contype='f' AND f.confrelid='ul_control.artifact'::regclass"
    ).fetchall()
    for schema, table, column in refs:
        query = sql.SQL(
            "SELECT DISTINCT a.uri FROM {}.{} r JOIN ul_control.artifact a ON a.artifact_id=r.{}"
        ).format(sql.Identifier(schema), sql.Identifier(table), sql.Identifier(column))
        for row in c.execute(query):
            roots.update(json_uris(row[0]))
    # JSONB descriptors and URI columns carry portable references beyond relational FKs.
    columns = c.execute(
        "SELECT table_schema,table_name,column_name,data_type FROM information_schema.columns "
        "WHERE table_schema=ANY(%s) AND (data_type='jsonb' OR column_name LIKE '%%uri') "
        "AND table_name<>'artifact' ORDER BY 1,2,3",
        (["ul_control", "ul_silver", "ul_gold"],),
    ).fetchall()
    for schema, table, column, _ in columns:
        query = sql.SQL("SELECT {} FROM {}.{}").format(
            sql.Identifier(column), sql.Identifier(schema), sql.Identifier(table)
        )
        for row in c.execute(query):
            roots.update(json_uris(row[0]))
    return roots

"""Atomic release pointers, immutable model verification and reader-safe cleanup."""

from __future__ import annotations

import re
from datetime import UTC, datetime
from typing import Any
from uuid import UUID

import psycopg
from psycopg import sql


def schema_for(identifier: UUID) -> str:
    return "ul_candidate_" + identifier.hex


def check_contents(c: psycopg.Connection[Any], schema: str, contents: dict[str, Any]) -> None:
    if not re.fullmatch(r"ul_candidate_[a-z0-9_]{1,40}", schema):
        raise ValueError("Invalid release schema")
    for table, expected in contents["tables"].items():
        if not re.fullmatch(r"(?:mart_[a-z_]+|dim_area)", table):
            raise ValueError("Invalid release relation")
        row = c.execute(
            sql.SQL(
                "SELECT count(*),md5(coalesce(string_agg(h,'' ORDER BY h),'')) FROM "
                "(SELECT md5((to_jsonb(t)-%s::text[])::text) h FROM {}.{} t) v"
            ).format(sql.Identifier(schema), sql.Identifier(table)),
            (contents["omitted_columns"],),
        ).fetchone()
        if row != (expected["rows"], expected["md5"]):
            raise ValueError("Validated release contents changed: " + table)


def publish(
    c: psycopg.Connection[Any],
    identifier: UUID,
    *,
    rollback: bool = False,
    fail_after_pointer: bool = False,
    published_at: datetime | None = None,
    manifest: dict[str, Any] | None = None,
) -> dict[str, Any]:
    with c.transaction():
        c.execute("SELECT pg_advisory_xact_lock(192033,0)")
        row = c.execute(
            "SELECT status,quality_passed,model_schema,validated_contents "
            "FROM ul_control.data_release WHERE release_id=%s FOR UPDATE",
            (identifier,),
        ).fetchone()
        if (
            row is None
            or not row[1]
            or row[0] not in {"validated", "published"}
            or row[2] != schema_for(identifier)
        ):
            raise ValueError("Only an immutable validated release can be published")
        slots = dict(c.execute("SELECT slot,release_id FROM ul_control.release_slot"))
        old = slots.get("active")
        if old == identifier:
            return dict(
                active=str(identifier),
                previous=str(slots.get("previous")) if slots.get("previous") else None,
                unchanged=True,
            )
        if rollback and slots.get("previous") != identifier:
            raise ValueError("Rollback target must be the retained previous release")
        if not rollback and row[0] != "validated":
            raise ValueError("Use rollback for a previously published release")
        check_contents(c, row[2], row[3])
        previous = slots.get("previous")
        c.execute("DELETE FROM ul_control.release_slot WHERE slot IN ('active','previous')")
        if previous and previous != identifier:
            c.execute(
                "UPDATE ul_control.data_release SET status='retired',retired_at=clock_timestamp() "
                "WHERE release_id=%s",
                (previous,),
            )
        if old:
            c.execute(
                "INSERT INTO ul_control.release_slot(slot,release_id) VALUES ('previous',%s)",
                (old,),
            )
        now = published_at or datetime.now(UTC)
        if not rollback:
            c.execute(
                "UPDATE ul_control.data_release SET status='published',published_at=%s "
                "WHERE release_id=%s",
                (now, identifier),
            )
            if manifest:
                c.execute(
                    "UPDATE ul_control.data_release SET manifest_uri=%s,manifest_sha256=%s "
                    "WHERE release_id=%s",
                    (manifest["uri"], manifest["sha256"], identifier),
                )
        c.execute(
            "INSERT INTO ul_control.release_slot(slot,release_id) VALUES ('active',%s)",
            (identifier,),
        )
        c.execute(
            (
                "INSERT INTO"
                " ul_control.release_transition(from_release_id,to_release_id,acti"
                "on) VALUES (%s,%s,%s)"
            ),
            (old, identifier, "rollback" if rollback else "publish"),
        )
        if fail_after_pointer:
            raise ValueError("Injected publication failure")
        return dict(
            active=str(identifier),
            previous=str(old) if old else None,
            rollback=rollback,
            unchanged=False,
        )


def cleanup(c: psycopg.Connection[Any], *, apply: bool = False) -> dict[str, Any]:
    removed, busy = [], []
    with c.transaction():
        c.execute("SELECT pg_advisory_xact_lock(192033,0)")
        rows = c.execute(
            "SELECT release_id,model_schema FROM ul_control.data_release d"
            " WHERE model_schema IS NOT NULL AND cleaned_at IS NULL AND"
            " (status IN ('failed','retired') OR (status IN"
            " ('building','validated') AND created_at<now()-interval '24"
            " hours')) AND NOT EXISTS(SELECT 1 FROM ul_control.release_slot s"
            " WHERE s.release_id=d.release_id)"
        ).fetchall()
        for identifier, schema in rows:
            if schema != schema_for(identifier):
                raise ValueError("Unrecognised cleanup target")
            available = c.execute(
                "SELECT pg_try_advisory_xact_lock(192033,hashtext(%s))", (str(identifier),)
            ).fetchone()
            assert available is not None
            if not available[0]:
                busy.append(str(identifier))
                continue
            removed.append(str(identifier))
            if apply:
                c.execute(
                    sql.SQL("DROP SCHEMA IF EXISTS {} CASCADE").format(sql.Identifier(schema))
                )
                c.execute(
                    (
                        "UPDATE ul_control.data_release SET cleaned_at=clock_timestamp()"
                        " WHERE release_id=%s"
                    ),
                    (identifier,),
                )
    return dict(applied=apply, removable=removed, readers_protected=busy)

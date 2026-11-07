"""Transactional, checksum-pinned migrations and deliberately separate database roles."""

from __future__ import annotations

import hashlib
import secrets
from pathlib import Path
from typing import Any

import psycopg
from psycopg import sql

ROLES = {
    "migration": "urbanlens_migrator",
    "pipeline": "urbanlens_loader",
    "dbt": "urbanlens_dbt",
    "api": "urbanlens_api",
}
SCHEMAS = ("ul_control", "ul_silver", "ul_gold", "ul_api")
MARKER = "UrbanLens managed role v1"
SQL_DIRECTORY = Path(__file__).parent / "sql"
LOADER_CONTROL = (
    "source_scope",
    "collection_cycle",
    "ingestion_run",
    "source_snapshot",
    "source_head",
    "artifact",
    "review",
)
PUBLIC_VIEWS = (
    "release",
    "establishment",
    "company",
    "company_establishment",
    "activity",
    "area_metric",
    "source_health",
)


def connect(dsn: str) -> psycopg.Connection[Any]:
    return psycopg.connect(dsn, connect_timeout=20, prepare_threshold=None)


def bootstrap(connection: psycopg.Connection[Any], passwords: dict[str, str]) -> None:
    """An existing database administrator provisions roles/schemas, never production rows."""
    with connection.transaction():
        connection.execute("SELECT pg_advisory_xact_lock(192019,1)")
        identity = connection.execute("SELECT current_user,current_database()").fetchone()
        assert identity is not None
        owner, database = identity
        existing = connection.execute(
            "SELECT extnamespace::regnamespace::text FROM pg_extension WHERE extname='postgis'"
        ).fetchone()
        if existing is None:
            connection.execute("CREATE EXTENSION postgis WITH SCHEMA public")
        elif existing[0] != "public":
            raise ValueError(
                "PostGIS must be installed in public; existing extension was preserved"
            )
        for purpose, role in ROLES.items():
            row = connection.execute(
                "SELECT shobj_description(oid,'pg_authid'),rolsuper,rolcreaterole,rolcreatedb,"
                "rolreplication,rolbypassrls FROM pg_roles WHERE rolname=%s",
                (role,),
            ).fetchone()
            if row is None:
                connection.execute(
                    sql.SQL(
                        "CREATE ROLE {} LOGIN NOINHERIT NOSUPERUSER NOCREATEDB NOCREATEROLE "
                        "NOREPLICATION NOBYPASSRLS PASSWORD {}"
                    ).format(sql.Identifier(role), sql.Literal(passwords[purpose]))
                )
                connection.execute(
                    sql.SQL("COMMENT ON ROLE {} IS {}").format(
                        sql.Identifier(role), sql.Literal(MARKER)
                    )
                )
            elif row[0] != MARKER or any(row[1:]):
                raise ValueError("An existing project role is unrecognised or overprivileged")
            # Neon can already provide SET through its administrative membership.
            # Re-granting ADMIN back to that grantor is rejected by PostgreSQL.
            if connection.execute(
                "SELECT pg_has_role(current_user,%s,'SET')", (role,)
            ).fetchone() != (True,):
                connection.execute(
                    sql.SQL("GRANT {} TO {}").format(sql.Identifier(role), sql.Identifier(owner))
                )
            connection.execute(
                sql.SQL("ALTER ROLE {} SET timezone TO 'UTC'").format(sql.Identifier(role))
            )
            connection.execute(
                sql.SQL("ALTER ROLE {} SET statement_timeout TO {}").format(
                    sql.Identifier(role), sql.Literal("15s" if purpose == "api" else "120s")
                )
            )
            connection.execute(
                sql.SQL("ALTER ROLE {} SET search_path TO pg_catalog,{},public").format(
                    sql.Identifier(role),
                    sql.Identifier(
                        "ul_api"
                        if purpose == "api"
                        else "ul_gold"
                        if purpose == "dbt"
                        else "ul_silver"
                    ),
                )
            )
            connection.execute(
                sql.SQL("GRANT CONNECT ON DATABASE {} TO {}").format(
                    sql.Identifier(database), sql.Identifier(role)
                )
            )
        # Public PostGIS types/functions remain usable; persistent and temp writes do not.
        connection.execute("REVOKE CREATE ON SCHEMA public FROM PUBLIC")
        connection.execute(
            sql.SQL("REVOKE CREATE,TEMPORARY ON DATABASE {} FROM PUBLIC").format(
                sql.Identifier(database)
            )
        )
        for purpose in ("migration", "pipeline", "dbt"):
            connection.execute(
                sql.SQL("GRANT TEMPORARY ON DATABASE {} TO {}").format(
                    sql.Identifier(database), sql.Identifier(ROLES[purpose])
                )
            )
        for schema in SCHEMAS:
            namespace = connection.execute(
                "SELECT pg_get_userbyid(nspowner) FROM pg_namespace WHERE nspname=%s", (schema,)
            ).fetchone()
            if namespace is None:
                connection.execute(
                    sql.SQL("CREATE SCHEMA {} AUTHORIZATION {}").format(
                        sql.Identifier(schema), sql.Identifier(ROLES["migration"])
                    )
                )
            elif namespace[0] != ROLES["migration"]:
                raise ValueError("An existing application schema has an unexpected owner")
            connection.execute(
                sql.SQL("REVOKE ALL ON SCHEMA {} FROM PUBLIC").format(sql.Identifier(schema))
            )
        for purpose in ("migration", "dbt"):
            connection.execute(
                sql.SQL(
                    "ALTER DEFAULT PRIVILEGES FOR ROLE {} REVOKE EXECUTE ON FUNCTIONS FROM PUBLIC"
                ).format(sql.Identifier(ROLES[purpose]))
            )
        connection.execute(
            sql.SQL(
                "ALTER DEFAULT PRIVILEGES FOR ROLE {} IN SCHEMA ul_gold "
                "GRANT SELECT ON TABLES TO {}"
            ).format(sql.Identifier(ROLES["dbt"]), sql.Identifier(ROLES["migration"]))
        )
        connection.execute(
            sql.SQL("GRANT USAGE ON SCHEMA ul_control,ul_silver TO {},{}").format(
                sql.Identifier(ROLES["pipeline"]), sql.Identifier(ROLES["dbt"])
            )
        )
        connection.execute(
            sql.SQL("GRANT USAGE,CREATE ON SCHEMA ul_gold TO {}").format(
                sql.Identifier(ROLES["dbt"])
            )
        )
        connection.execute(
            sql.SQL("GRANT USAGE ON SCHEMA ul_api TO {}").format(sql.Identifier(ROLES["api"]))
        )


def grants(connection: psycopg.Connection[Any]) -> None:
    """Run as the migrator. API access is an explicit allowlist, never a default grant."""
    loader, dbt, api = (sql.Identifier(ROLES[p]) for p in ("pipeline", "dbt", "api"))
    connection.execute(
        sql.SQL("GRANT SELECT ON ALL TABLES IN SCHEMA ul_control TO {},{}").format(loader, dbt)
    )
    for table in LOADER_CONTROL:
        connection.execute(
            sql.SQL("GRANT INSERT,UPDATE,DELETE ON ul_control.{} TO {}").format(
                sql.Identifier(table), loader
            )
        )
    for table in ("data_release", "release_source", "release_slot"):
        connection.execute(
            sql.SQL("GRANT INSERT,UPDATE,DELETE ON ul_control.{} TO {}").format(
                sql.Identifier(table), dbt
            )
        )
    connection.execute(
        sql.SQL("GRANT SELECT,INSERT,UPDATE,DELETE ON ALL TABLES IN SCHEMA ul_silver TO {}").format(
            loader
        )
    )
    connection.execute(sql.SQL("GRANT SELECT ON ALL TABLES IN SCHEMA ul_silver TO {}").format(dbt))
    connection.execute(
        sql.SQL("GRANT SELECT,INSERT,UPDATE,DELETE ON ALL TABLES IN SCHEMA ul_gold TO {}").format(
            dbt
        )
    )
    for schema in SCHEMAS:
        connection.execute(
            sql.SQL("REVOKE ALL ON ALL TABLES IN SCHEMA {} FROM PUBLIC").format(
                sql.Identifier(schema)
            )
        )
        connection.execute(
            sql.SQL("REVOKE ALL ON ALL FUNCTIONS IN SCHEMA {} FROM PUBLIC").format(
                sql.Identifier(schema)
            )
        )
    for table in PUBLIC_VIEWS:
        connection.execute(
            sql.SQL("GRANT SELECT ON ul_api.{} TO {}").format(sql.Identifier(table), api)
        )
    connection.execute(
        sql.SQL(
            "GRANT EXECUTE ON FUNCTION ul_api.establishments_within"
            "(uuid,double precision,double precision,integer) TO {}"
        ).format(api)
    )
    if connection.execute(
        "SELECT to_regprocedure('ul_api.read_release(uuid,text,jsonb)')"
    ).fetchone() != (None,):
        connection.execute(
            sql.SQL("GRANT EXECUTE ON FUNCTION ul_api.read_release(uuid,text,jsonb) TO {}").format(
                api
            )
        )
    connection.execute(
        sql.SQL(
            "ALTER DEFAULT PRIVILEGES IN SCHEMA ul_silver "
            "GRANT SELECT,INSERT,UPDATE,DELETE ON TABLES TO {}"
        ).format(loader)
    )
    connection.execute(
        sql.SQL("ALTER DEFAULT PRIVILEGES IN SCHEMA ul_silver GRANT SELECT ON TABLES TO {}").format(
            dbt
        )
    )
    connection.execute(
        sql.SQL(
            "ALTER DEFAULT PRIVILEGES IN SCHEMA ul_gold "
            "GRANT SELECT,INSERT,UPDATE,DELETE ON TABLES TO {}"
        ).format(dbt)
    )


def migrate(connection: psycopg.Connection[Any], directory: Path = SQL_DIRECTORY) -> list[int]:
    applied = []
    with connection.transaction():
        connection.execute("SET LOCAL timezone='UTC'")
        connection.execute("SET LOCAL lock_timeout='5s'")
        connection.execute("SELECT pg_advisory_xact_lock(192019,2)")
        connection.execute(
            "CREATE TABLE IF NOT EXISTS ul_control.schema_migration ("
            "version integer PRIMARY KEY, filename text NOT NULL, sha256 text NOT NULL "
            "CHECK(sha256 ~ '^[a-f0-9]{64}$'), applied_at timestamptz NOT NULL DEFAULT now())"
        )
        known = {
            r[0]: (r[1], r[2])
            for r in connection.execute(
                "SELECT version,filename,sha256 FROM ul_control.schema_migration"
            )
        }
        files = sorted(directory.glob("[0-9][0-9][0-9][0-9]_*.sql"))
        versions = [int(p.name[:4]) for p in files]
        if not files or versions != list(range(1, len(files) + 1)) or set(known) - set(versions):
            raise ValueError(
                "Migration sequence is missing, duplicated or newer than this checkout"
            )
        for path, version in zip(files, versions, strict=True):
            body = path.read_bytes()
            digest = hashlib.sha256(body).hexdigest()
            if version in known:
                if known[version] != (path.name, digest):
                    raise ValueError("An applied migration was edited; add a new migration instead")
                continue
            if any(v > version for v in known):
                raise ValueError("Cannot backfill an older migration")
            connection.execute(body.decode("utf-8"), prepare=False)
            connection.execute(
                "INSERT INTO ul_control.schema_migration(version,filename,sha256) "
                "VALUES (%s,%s,%s)",
                (version, path.name, digest),
            )
            applied.append(version)
        grants(connection)
    return applied


def new_passwords() -> dict[str, str]:
    return {purpose: secrets.token_urlsafe(32) for purpose in ROLES}

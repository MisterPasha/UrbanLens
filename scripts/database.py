"""Provision, migrate and verify local/Neon databases without printing credentials."""

from __future__ import annotations

import argparse
import json
import os
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from urllib.parse import quote, urlencode
from uuid import uuid4

import psycopg
from dotenv import dotenv_values, set_key
from psycopg import sql
from psycopg.conninfo import conninfo_to_dict, make_conninfo

from urbanlens.database.migrations import ROLES, bootstrap, connect, migrate, new_passwords
from urbanlens.database.verification import exercise

ROOT = Path(__file__).resolve().parents[1]


def settings_for(dsn: str) -> dict[str, str]:
    return {k: str(v) for k, v in conninfo_to_dict(dsn).items() if v is not None}


def values() -> dict[str, str]:
    result = {k: v for k, v in dotenv_values(ROOT / ".env").items() if v is not None}
    result.update(
        {k: v for k, v in os.environ.items() if k.startswith(("URBANLENS_", "LOCAL_POSTGRES_"))}
    )
    return result


def key(target: str, purpose: str) -> str:
    prefix = "URBANLENS_" if target == "neon" else "URBANLENS_LOCAL_"
    return prefix + purpose.upper() + "_DATABASE_URL"


def administrator(target: str) -> str:
    env = values()
    if target == "local":
        return make_conninfo(
            host=env.get("LOCAL_POSTGRES_HOST", "127.0.0.1"),
            port=env.get("LOCAL_POSTGRES_PORT", "54322"),
            dbname=env.get("LOCAL_POSTGRES_DB", "urbanlens"),
            user=env.get("LOCAL_POSTGRES_USER", "urbanlens_local"),
            password=env.get("LOCAL_POSTGRES_PASSWORD", ""),
        )
    original = env.get("URBANLENS_DATABASE_ADMIN_URL") or env.get("URBANLENS_PIPELINE_DATABASE_URL")
    if not original:
        raise ValueError("Neon administrative URL is missing from local .env")
    parts = settings_for(original)
    host = parts.get("host", "").replace("-pooler.", ".", 1)
    if not host.endswith(".neon.tech") or parts.get("sslmode") not in {"require", "verify-full"}:
        raise ValueError("Use the configured Neon project with TLS")
    return make_conninfo(original, host=host)


def url(settings: dict[str, str]) -> str:
    parts = dict(settings)
    user, password, host, port, database = (
        parts.pop(k, "") for k in ("user", "password", "host", "port", "dbname")
    )
    authority = quote(user, safe="") + ":" + quote(password, safe="") + "@" + host
    if port:
        authority += ":" + port
    return (
        "postgresql://"
        + authority
        + "/"
        + quote(database, safe="")
        + ("?" + urlencode(parts) if parts else "")
    )


def passwords(target: str) -> dict[str, str]:
    result = new_passwords()
    env = values()
    for purpose, role in ROLES.items():
        existing = env.get(key(target, purpose))
        if existing:
            parts = settings_for(existing)
            if parts.get("user") == role and parts.get("password"):
                result[purpose] = parts["password"]
    return result


def credential(target: str, purpose: str, *, database: str | None = None) -> str:
    result = values().get(key(target, purpose))
    if not result or conninfo_to_dict(result).get("user") != ROLES[purpose]:
        raise ValueError("Scoped database credentials are missing; run database setup first")
    return make_conninfo(result, dbname=database) if database else result


def setup(target: str) -> dict[str, Any]:
    admin = administrator(target)
    role_passwords = passwords(target)
    with connect(admin) as connection:
        bootstrap(connection, role_passwords)
    # Preserve the original owner URL before replacing any runtime credentials.
    if target == "neon" and not values().get("URBANLENS_DATABASE_ADMIN_URL"):
        set_key(str(ROOT / ".env"), "URBANLENS_DATABASE_ADMIN_URL", admin)
    settings = settings_for(admin)
    for purpose, role in ROLES.items():
        role_settings = {**settings, "user": role, "password": role_passwords[purpose]}
        if target == "neon" and purpose == "api":
            host = role_settings["host"]
            first, rest = host.split(".", 1)
            role_settings["host"] = first + "-pooler." + rest
        set_key(str(ROOT / ".env"), key(target, purpose), url(role_settings))
    with connect(credential(target, "migration")) as connection:
        applied = migrate(connection)
    return {"applied": applied, **check(target)}


def check(target: str, *, database: str | None = None) -> dict[str, Any]:
    report: dict[str, Any] = {
        "target": target,
        "roles": {},
        "checked_at": datetime.now(UTC).isoformat(),
    }
    for purpose, role in ROLES.items():
        with connect(credential(target, purpose, database=database)) as connection:
            identity = connection.execute(
                "SELECT current_user,current_setting('TimeZone')"
            ).fetchone()
            assert identity is not None
            current, timezone = identity
            if current != role or timezone != "UTC":
                raise ValueError("Unexpected database role/timezone")
            flags = connection.execute(
                "SELECT rolsuper,rolcreaterole,rolcreatedb,rolreplication,rolbypassrls "
                "FROM pg_roles WHERE rolname=current_user"
            ).fetchone()
            if flags is None or any(flags):
                raise ValueError("Runtime role is overprivileged")
            report["roles"][purpose] = True
            if purpose == "api":
                for schema in ("ul_control", "ul_silver", "ul_gold"):
                    if connection.execute(
                        "SELECT has_schema_privilege(current_user,%s,'USAGE')", (schema,)
                    ).fetchone() != (False,):
                        raise ValueError("API role can access private schema")
                for statement in (
                    "SELECT * FROM ul_silver.observation_version",
                    "SELECT * FROM ul_control.artifact",
                    "SELECT * FROM ul_gold.establishment",
                    "CREATE TABLE public.api_forbidden(id integer)",
                    "CREATE TABLE ul_api.api_forbidden(id integer)",
                    "CREATE TEMP TABLE api_forbidden(id integer)",
                    "INSERT INTO ul_api.release(release_id) VALUES (gen_random_uuid())",
                    "SET ROLE urbanlens_migrator",
                ):
                    try:
                        with connection.transaction():
                            connection.execute(statement)
                    except (
                        psycopg.errors.InsufficientPrivilege,
                        psycopg.errors.ObjectNotInPrerequisiteState,
                    ):
                        pass
                    else:
                        connection.rollback()
                        raise ValueError("API privilege boundary failed")
                for view in (
                    "release",
                    "establishment",
                    "company",
                    "company_establishment",
                    "activity",
                    "area_metric",
                    "source_health",
                ):
                    writable = connection.execute(
                        (
                            "SELECT "
                            "has_table_privilege(current_user,%s,'INSERT,UPDATE,DELETE,TRUNCATE')"
                        ),
                        ("ul_api." + view,),
                    ).fetchone()
                    if writable != (False,):
                        raise ValueError("API view has write privileges")
                    connection.execute(
                        sql.SQL("SELECT * FROM ul_api.{} LIMIT 1").format(sql.Identifier(view))
                    )
                connection.execute(
                    "SELECT * FROM ul_api.establishments_within(%s,-0.14,50.82,500)", (uuid4(),)
                ).fetchall()
                report["api_private_reads_and_writes_denied"] = True
            elif purpose == "pipeline":
                if connection.execute(
                    "SELECT has_table_privilege(current_user,"
                    "'ul_control.schema_migration','UPDATE')"
                ).fetchone() != (False,):
                    raise ValueError("Loader can edit migration history")
            elif purpose == "dbt":
                if connection.execute(
                    "SELECT has_table_privilege(current_user,"
                    "'ul_silver.observation_version','UPDATE')"
                ).fetchone() != (False,):
                    raise ValueError("dbt can modify private source observations")
    with connect(credential(target, "migration", database=database)) as connection:
        report["migration_versions"] = [
            r[0]
            for r in connection.execute(
                "SELECT version FROM ul_control.schema_migration ORDER BY version"
            )
        ]
        tables = connection.execute(
            "SELECT count(*) FROM pg_tables "
            "WHERE schemaname IN ('ul_control','ul_silver','ul_gold')"
        ).fetchone()
        size = connection.execute("SELECT pg_database_size(current_database())").fetchone()
        assert tables is not None and size is not None
        report["application_tables"] = tables[0]
        report["database_bytes"] = size[0]
    return report


@contextmanager
def scratch_database(target: str) -> Iterator[str]:
    """Create and remove only a newly named scratch database; leave the main DB untouched."""
    admin = administrator(target)
    name = "urbanlens_step19_test_" + uuid4().hex[:16]
    with connect(admin) as connection:
        connection.autocommit = True
        if connection.execute("SELECT 1 FROM pg_database WHERE datname=%s", (name,)).fetchone():
            raise ValueError("Scratch database collision")
        connection.execute(
            sql.SQL("CREATE DATABASE {} TEMPLATE template0").format(sql.Identifier(name))
        )
        try:
            with connect(make_conninfo(admin, dbname=name)) as scratch:
                bootstrap(scratch, passwords(target))
            yield name
        finally:
            if not name.startswith("urbanlens_step19_test_") or name == conninfo_to_dict(admin).get(
                "dbname"
            ):
                raise ValueError("Refusing unsafe scratch database removal")
            connection.execute(
                sql.SQL("DROP DATABASE {} WITH (FORCE)").format(sql.Identifier(name))
            )


def clean_check(target: str) -> dict[str, Any]:
    with scratch_database(target) as name:
        with connect(credential(target, "migration", database=name)) as connection:
            first = migrate(connection)
            repeated = migrate(connection)
        report = check(target, database=name)
        report["contracts"] = exercise(
            {purpose: credential(target, purpose, database=name) for purpose in ROLES}
        )
        report.update({"fresh_database": True, "first_applied": first, "repeat_applied": repeated})
    report["scratch_database_removed"] = True
    return report


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("task", choices=["setup", "migrate", "check", "clean-check"])
    parser.add_argument("--target", choices=["local", "neon"], default="local")
    args = parser.parse_args()
    if args.task == "setup":
        report = setup(args.target)
    elif args.task == "migrate":
        with connect(credential(args.target, "migration")) as connection:
            report = {"applied": migrate(connection)}
    elif args.task == "clean-check":
        report = clean_check(args.target)
    else:
        report = check(args.target)
    directory = ROOT / "data/database"
    directory.mkdir(parents=True, exist_ok=True)
    (directory / f"{args.target}-{args.task}.json").write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    try:
        main()
    except (ValueError, psycopg.Error) as error:
        # Driver errors can embed hosts/SQL values: keep detailed diagnostics private.
        path = ROOT / ".cache/database-error.txt"
        path.parent.mkdir(exist_ok=True)
        path.write_text(str(error))
        code = getattr(error, "sqlstate", None) or type(error).__name__
        raise SystemExit(
            f"Database operation failed ({code}); inspect private .cache/database-error.txt"
        ) from None

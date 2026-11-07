"""Build and verify private dbt Gold candidates without publishing a release."""

from __future__ import annotations

import argparse
import importlib.metadata
import json
import os
import re
import subprocess
import sys
import tempfile
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, cast
from uuid import UUID, uuid4

import psycopg
from psycopg import sql
from psycopg.conninfo import conninfo_to_dict
from psycopg.rows import dict_row

from scripts.backup import code_archive
from scripts.database import administrator, credential
from scripts.history import failed_attempts
from scripts.silver import ROOT, environment
from urbanlens.database.migrations import connect
from urbanlens.silver.audit import semantic_state
from urbanlens.silver.classification import save_json
from urbanlens.silver.eligibility import digest


def dbt_run(
    target: str,
    schema: str,
    variables: dict[str, Any],
    arguments: list[str],
    *,
    database: str | None = None,
) -> dict[str, Any]:
    """The restricted role builds in a pre-provisioned private schema; no secret argv/files."""
    settings = conninfo_to_dict(credential(target, "dbt", database=database))
    env = os.environ.copy()
    env.update(DBT_SEND_ANONYMOUS_USAGE_STATS="false", DBT_USE_COLORS="false")
    for name in ("host", "port", "user", "password", "dbname", "sslmode"):
        env["UL_GOLD_" + name.upper()] = str(
            settings.get(name)
            or ("5432" if name == "port" else "prefer" if name == "sslmode" else "")
        )
    env["UL_GOLD_SCHEMA"] = schema
    if settings.get("channel_binding"):
        env["PGCHANNELBINDING"] = str(settings["channel_binding"])
    cache = ROOT / ".cache"
    cache.mkdir(exist_ok=True)
    output = cache / ("gold-dbt-" + target + "-" + schema)
    env["DBT_TARGET_PATH"] = str(output)
    env["DBT_LOG_PATH"] = str(output / "logs")
    with tempfile.TemporaryDirectory(prefix="gold-profile-", dir=cache) as directory:
        profile = (
            "urbanlens:\n  target: scoped\n  outputs:\n    scoped:\n"
            "      type: postgres\n      threads: 2\n      connect_timeout: 20\n"
        )
        for name in ("host", "port", "user", "password", "dbname", "sslmode", "schema"):
            expression = (
                "{{ env_var('UL_GOLD_"
                + name.upper()
                + "')"
                + (" | int" if name == "port" else "")
                + " }}"
            )
            profile += "      " + name + ': "' + expression + '"\n'
        (Path(directory) / "profiles.yml").write_text(profile, encoding="utf-8")
        executable = Path(sys.executable).with_name("dbt.exe" if os.name == "nt" else "dbt")
        command = [
            str(executable),
            *arguments,
            "--project-dir",
            str(ROOT / "dbt"),
            "--profiles-dir",
            directory,
            "--vars",
            json.dumps(variables),
        ]
        completed = subprocess.run(
            command,
            env=env,
            capture_output=True,
            text=True,
            check=False,
            timeout=600,
            encoding="utf-8",
            errors="replace",
        )
        # Detailed SQL diagnostics stay in ignored local logs, not terminal/CI output.
        log = completed.stdout + completed.stderr
        if settings.get("password"):
            log = log.replace(str(settings["password"]), "[redacted]")
        (cache / ("gold-" + target + "-output.txt")).write_text(log, encoding="utf-8")
        if completed.returncode:
            raise ValueError("dbt failed; inspect private .cache/gold-" + target + "-output.txt")
    results_path = output / "run_results.json"
    result = json.loads(results_path.read_text(encoding="utf-8")) if results_path.exists() else {}
    return dict(
        command=arguments,
        elapsed_seconds=result.get("elapsed_time"),
        results=[dict(id=r["unique_id"], status=r["status"]) for r in result.get("results", [])],
    )


def prepare_schema(target: str, schema: str) -> None:
    if not re.fullmatch(r"ul_candidate_[a-z0-9_]{1,40}", schema):
        raise ValueError("Gold builds require a private ul_candidate_ schema")
    with connect(administrator(target)) as c:
        row = c.execute(
            "SELECT pg_get_userbyid(nspowner) FROM pg_namespace WHERE nspname=%s", (schema,)
        ).fetchone()
        if row is None:
            c.execute(
                sql.SQL("CREATE SCHEMA {} AUTHORIZATION urbanlens_migrator").format(
                    sql.Identifier(schema)
                )
            )
        elif row[0] != "urbanlens_migrator":
            raise ValueError("Candidate schema has an unexpected owner")
        c.execute(
            sql.SQL("REVOKE ALL ON SCHEMA {} FROM PUBLIC,urbanlens_api,urbanlens_loader").format(
                sql.Identifier(schema)
            )
        )
        c.execute(
            sql.SQL("GRANT USAGE,CREATE ON SCHEMA {} TO urbanlens_dbt").format(
                sql.Identifier(schema)
            )
        )
        c.execute(
            sql.SQL(
                "ALTER DEFAULT PRIVILEGES FOR ROLE urbanlens_dbt IN SCHEMA"
                " {} GRANT SELECT ON TABLES TO urbanlens_migrator"
            ).format(sql.Identifier(schema))
        )


def build(target: str, schema: str, release_id: UUID | None = None) -> dict[str, Any]:
    _, store, snapshots, _, _, _ = environment("s3")
    with connect(credential(target, "migration")) as guard:
        has_registry = guard.execute(
            "SELECT 1 FROM information_schema.columns WHERE table_schema='ul_control' "
            "AND table_name='data_release' AND column_name='model_schema'"
        ).fetchone()
        if (
            has_registry
            and guard.execute(
                "SELECT 1 FROM ul_control.data_release "
                "WHERE model_schema=%s AND cleaned_at IS NULL",
                (schema,),
            ).fetchone()
        ):
            raise ValueError("A registered release schema cannot be rebuilt")
    prepare_schema(target, schema)
    with connect(credential(target, "migration")) as c:
        # Existing Silver writers use this lock. All dbt threads read one stable input state.
        c.execute("SELECT pg_advisory_lock(192020,1)")
        exists = c.execute("SELECT to_regclass(%s)", (schema + ".mart_release",)).fetchone()
        if exists and exists[0]:
            previous = c.execute(
                sql.SQL("SELECT release_id FROM {}.mart_release").format(sql.Identifier(schema))
            ).fetchone()
            if (
                previous
                and c.execute(
                    "SELECT 1 FROM ul_control.release_slot WHERE release_id=%s"
                    " AND slot IN ('active','previous')",
                    previous,
                ).fetchone()
            ):
                raise ValueError("Cannot rebuild an active or previous release schema")
        state = semantic_state(c)
        with c.cursor(row_factory=dict_row) as cursor:
            cursor.execute(
                "SELECT"
                " sc.provider,sc.scope_key,s.snapshot_id::text,s.manifest_uri"
                ",s.manifest_sha256,"
                " s.fetched_at::text,s.source_extract_date::text,cy.source_ed"
                "ition FROM ul_control.source_head h JOIN"
                " ul_control.source_scope sc USING(scope_id) JOIN"
                " ul_control.source_snapshot s ON"
                " s.snapshot_id=h.snapshot_id JOIN"
                " ul_control.collection_cycle cy ON cy.cycle_id=s.cycle_id"
                " ORDER BY sc.provider,sc.scope_key"
            )
            inputs = cursor.fetchall()
        if len(inputs) != 5:
            raise ValueError("Gold build requires all five accepted collection scopes")
        if {r["snapshot_id"] for r in inputs} != {str(s.snapshot_id) for s in snapshots}:
            raise ValueError("Database and S3 accepted heads differ; finish the Silver load first")
        population = c.execute(
            "SELECT DISTINCT"
            " geography_version,population_edition,reference_date FROM"
            " ul_silver.population"
        ).fetchall()
        if len(population) != 1:
            raise ValueError("Choose one compatible pinned population edition before building")
        geo, edition, reference_date = population[0]
        versions = c.execute(
            "SELECT DISTINCT taxonomy_version,identity_rule_version FROM"
            " ul_silver.current_establishment ORDER BY 1,2"
        ).fetchall()
        definitions = c.execute(
            "SELECT kind,version,sha256 FROM ul_control.classification_definition ORDER BY 1,2"
        ).fetchall()
        reviews = c.execute(
            "SELECT version,sha256 FROM ul_control.review_definition ORDER BY version"
        ).fetchall()
        failures = [
            dict(
                provider=s.cycle.provider.value,
                scope_key=s.cycle.scope,
                started_at=s.run.started_at.isoformat(),
                status="failed" if s.run.status.value == "failed" else "rejected",
                collection_status=s.run.status.value,
                evidence=a.model_dump(mode="json"),
            )
            for s, a in failed_attempts(store, snapshots)
            if s.run.mode == "collect"
        ]
        now = datetime.now(UTC).isoformat()
        manifest = dict(
            version="gold-inputs-v1",
            silver_state=state,
            source_snapshots=inputs,
            rule_versions=versions,
            classification_definitions=definitions,
            review_definitions=reviews,
            failed_attempts=failures,
            dbt_core=importlib.metadata.version("dbt-core"),
            dbt_postgres=importlib.metadata.version("dbt-postgres"),
            code=code_archive(store).model_dump(mode="json"),
            population_edition=edition,
            population_reference_date=reference_date.isoformat(),
            geography_version=geo,
        )
        identifier = str(release_id or uuid4())
        variables = dict(
            release_id=identifier,
            assessed_at=now,
            geography_version=geo,
            population_edition=edition,
            population_reference_date=reference_date.isoformat(),
            input_manifest=manifest,
            failed_attempts=failures,
        )
        # Release relation locks acquired by the previous-candidate check. The session
        # advisory lock still excludes Silver writers until this connection closes.
        c.commit()
        print("Building private Gold models and blocking dbt checks (" + target + ")", flush=True)
        built = dbt_run(target, schema, variables, ["build"])
        dbt_run(target, schema, variables, ["run-operation", "install_queries"])
        docs = dbt_run(target, schema, variables, ["docs", "generate", "--no-compile"])
        if semantic_state(c) != state:
            raise ValueError("Silver changed during Gold build")
        report = verify(c, schema)
        report.update(
            target=target,
            schema=schema,
            release_id=identifier,
            built_at=now,
            published=False,
            inputs=manifest,
            dbt=built,
            docs_generated=docs is not None,
        )
    with connect(credential(target, "api")) as api:
        if api.execute(
            "SELECT has_schema_privilege(current_user,%s,'USAGE')", (schema,)
        ).fetchone() != (False,):
            raise ValueError("API can access private candidate schema")
    report["api_candidate_access_denied"] = True
    report["evidence"] = save_json(store, "evidence/gold/build", report).model_dump(mode="json")
    path = ROOT / f"data/gold/build-{target}.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    (ROOT / f".cache/gold-{target}.json").write_text(
        json.dumps(dict(schema=schema, variables=variables), indent=2) + "\n", encoding="utf-8"
    )
    return report


def verify(c: psycopg.Connection[Any], schema: str) -> dict[str, Any]:
    """Reconcile shared selections and expose measured results; formulas live only in SQL."""
    prefix = sql.Identifier(schema)
    tables = (
        "mart_establishment",
        "mart_venue_context",
        "mart_category",
        "dim_area",
        "mart_area",
        "mart_city",
        "mart_coverage",
        "mart_company",
        "mart_company_link",
        "mart_activity",
        "mart_activity_status",
        "mart_source_health",
        "mart_release",
    )
    counts = {}
    contents = {}
    omitted = [
        "release_id",
        "built_at",
        "assessed_at",
        "established_at",
        "baseline_established_at",
        "retention_boundary",
        "resolution_artifact_id",
    ]
    for table in tables:
        row = c.execute(
            sql.SQL("SELECT count(*) FROM {}.{}").format(prefix, sql.Identifier(table))
        ).fetchone()
        assert row is not None
        counts[table] = row[0]
        hashed = c.execute(
            sql.SQL(
                "SELECT md5(coalesce(string_agg(h,'' ORDER BY h),'')) FROM "
                "(SELECT md5((to_jsonb(t)-%s::text[])::text) h FROM {}.{} t) rows"
            ).format(prefix, sql.Identifier(table)),
            (omitted,),
        ).fetchone()
        assert hashed is not None
        contents[table] = dict(rows=row[0], md5=hashed[0])
    with c.cursor(row_factory=dict_row) as cursor:
        cursor.execute(sql.SQL("SELECT * FROM {}.mart_city WHERE category='all'").format(prefix))
        city = cursor.fetchone()
        cursor.execute(
            sql.SQL(
                "SELECT"
                " provider,scope_key,status,last_success_at::text,next_due_at"
                "::text FROM {}.mart_source_health ORDER BY 1,2"
            ).format(prefix)
        )
        health = cursor.fetchall()
        cursor.execute(
            sql.SQL(
                "SELECT entity_type,state,retained_event_count FROM"
                " {}.mart_activity_status ORDER BY 1"
            ).format(prefix)
        )
        activity = cursor.fetchall()
    # A category subset, empty category set, and a partial-city radius use the exact same selection.
    examples = []
    for categories in (None, ["cafe", "bakery"], []):
        n = c.execute(
            sql.SQL("SELECT count(*) FROM {}.selected_venues(%s)").format(prefix), (categories,)
        ).fetchone()
        area = c.execute(
            sql.SQL("SELECT establishment_count FROM {}.area_summary(null,%s)").format(prefix),
            (categories,),
        ).fetchone()
        radius = c.execute(
            sql.SQL(
                "SELECT"
                " establishment_count,category_mix,supported,partial_coverage"
                " FROM {}.radius_summary(-0.14,50.82,500,%s)"
            ).format(prefix),
            (categories,),
        ).fetchone()
        radius_n = c.execute(
            sql.SQL("SELECT count(*) FROM {}.selected_venues(%s,null,-0.14,50.82,500)").format(
                prefix
            ),
            (categories,),
        ).fetchone()
        assert n and area and radius and radius_n
        if n[0] != area[0] or radius[0] != radius_n[0] or sum(radius[1].values()) != radius[0]:
            raise ValueError("Shared selection/summary reconciliation failed")
        examples.append(
            dict(
                categories=categories,
                city_count=n[0],
                radius_count=radius[0],
                radius_mix=radius[1],
                partial_coverage=radius[3],
            )
        )
    invalid_checks = 0
    for parameters in (
        (["not_a_category"], None, None, None),
        (None, -0.14, 50.82, 12),
        (None, 0.0, 0.0, 500),
    ):
        try:
            with c.transaction():
                c.execute(
                    sql.SQL("SELECT count(*) FROM {}.selected_venues(%s,null,%s,%s,%s)").format(
                        prefix
                    ),
                    parameters,
                ).fetchall()
        except psycopg.errors.InvalidParameterValue:
            invalid_checks += 1
        else:
            raise ValueError("Invalid query parameters were accepted")
    # JSON conversion keeps Decimal/date values deterministic and portable, not credential-bearing.
    return cast(
        dict[str, Any],
        json.loads(
            json.dumps(
                dict(
                    counts=counts,
                    city=city,
                    source_health=health,
                    activity=activity,
                    shared_filter_checks=examples,
                    invalid_parameter_checks=invalid_checks,
                    contents=dict(
                        tables=contents, sha256=digest(contents), omitted_columns=omitted
                    ),
                ),
                default=str,
            )
        ),
    )


def fixtures() -> dict[str, Any]:
    """CI examples use local PostGIS without cloud credentials."""
    schema = "ul_candidate_fixtures"
    prepare_schema("local", schema)
    return dbt_run(
        "local",
        schema,
        {},
        [
            "build",
            "--select",
            "+metric_examples",
            "+monthly_health_examples",
            "--indirect-selection",
            "cautious",
        ],
    )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("task", choices=["build", "check", "fixtures"])
    parser.add_argument("--target", choices=["local", "neon"], default="local")
    parser.add_argument("--schema", default="ul_candidate_gold")
    parser.add_argument("--release-id", type=UUID)
    args = parser.parse_args()
    if not re.fullmatch(r"ul_candidate_[a-z0-9_]{1,40}", args.schema):
        parser.error("Use a private ul_candidate_ schema")
    if args.task == "fixtures":
        if args.target != "local":
            parser.error("Run isolated fixture models against local PostGIS")
        report = fixtures()
    elif args.task == "build":
        report = build(args.target, args.schema, args.release_id)
    else:
        with connect(credential(args.target, "migration")) as c:
            report = verify(c, args.schema)
    print(json.dumps({k: v for k, v in report.items() if k not in {"inputs", "dbt"}}, indent=2))


if __name__ == "__main__":
    try:
        main()
    except (ValueError, psycopg.Error, subprocess.TimeoutExpired) as error:
        path = ROOT / ".cache/gold-error.txt"
        path.parent.mkdir(exist_ok=True)
        path.write_text(str(error), encoding="utf-8")
        raise SystemExit(
            "Gold operation failed; inspect private"
            " .cache/gold-error.txt and gold-<target>-output.txt"
        ) from None

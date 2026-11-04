"""Step 08 local Lambda-emulator build and bounded partition measurements."""

from __future__ import annotations

import argparse
import base64
import hashlib
import json
import os
import secrets
import statistics
import subprocess
import time
import urllib.error
import urllib.parse
import urllib.request
from collections.abc import Callable
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import psycopg
from dotenv import dotenv_values, set_key
from psycopg.conninfo import conninfo_to_dict, make_conninfo

from scripts.dev import tool
from urbanlens.feasibility import reconcile_partitions, require

ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / "data/pilots/runtime/2026-11-02"
DOCKERFILE = ROOT / "containers/Dockerfile"


def save(name: str, value: Any) -> None:
    OUT.mkdir(parents=True, exist_ok=True)
    (OUT / name).write_text(json.dumps(value, indent=2) + "\n", encoding="utf-8")


def docker(*arguments: str, binary: bool = False) -> Any:
    executable = tool("docker")
    environment = os.environ.copy()
    environment["PATH"] = str(Path(executable).parent) + os.pathsep + environment.get("PATH", "")
    result = subprocess.run([executable, *arguments], capture_output=True, env=environment)
    if result.returncode:
        # Docker arguments never include credentials; private DB env files are passed by path.
        raise RuntimeError(result.stderr.decode(errors="replace")[-3000:])
    return result.stdout if binary else result.stdout.decode()


def build() -> None:
    images = {}
    config = ROOT / "config/runtime-pilot.json"
    if config.exists():
        base = json.loads(config.read_text(encoding="utf-8"))["lambda_base_image"]
    else:
        docker("pull", "--platform", "linux/amd64", "public.ecr.aws/lambda/python:3.13")
        base = json.loads(docker("image", "inspect", "public.ecr.aws/lambda/python:3.13"))[0][
            "RepoDigests"
        ][0]
        config.write_text(
            json.dumps({"lambda_base_image": base, "platform": "linux/amd64"}, indent=2) + "\n",
            encoding="utf-8",
        )
    for target in ("api", "pipeline"):
        print("Building", target, flush=True)
        started = time.perf_counter()
        log = docker(
            "build",
            "--platform",
            "linux/amd64",
            "--provenance=false",
            "--build-arg",
            "LAMBDA_BASE_IMAGE=" + base,
            "--target",
            target,
            "-f",
            str(DOCKERFILE),
            "-t",
            "urbanlens-" + target + ":step08",
            str(ROOT),
        )
        details = json.loads(docker("image", "inspect", "urbanlens-" + target + ":step08"))[0]
        images[target] = {
            "id": details["Id"],
            "bytes": details["Size"],
            "architecture": details["Architecture"],
            "os": details["Os"],
            "build_seconds": round(time.perf_counter() - started, 3),
        }
        save("build-" + target + ".json", {"stdout": log})
        print(target, "image bytes:", details["Size"], flush=True)
    save("images.json", {"base": base, "images": images, "built_at": datetime.now(UTC).isoformat()})


def invoke(port: int, event: dict[str, Any]) -> tuple[dict[str, Any], float]:
    started = time.perf_counter()
    request = urllib.request.Request(
        f"http://127.0.0.1:{port}/2015-03-31/functions/function/invocations",
        data=json.dumps(event).encode(),
        headers={"Content-Type": "application/json"},
    )
    with urllib.request.urlopen(request, timeout=600) as response:
        payload = json.load(response)
    require(
        "errorMessage" not in payload, "Emulator function failed: " + str(payload.get("errorType"))
    )
    return payload, time.perf_counter() - started


def wait_ready(port: int) -> None:
    for _ in range(60):
        try:
            urllib.request.urlopen(f"http://127.0.0.1:{port}/", timeout=1).close()
            return
        except urllib.error.HTTPError:
            return  # HTTP service is ready; no function was invoked.
        except (urllib.error.URLError, ConnectionError, TimeoutError):
            time.sleep(0.5)
    raise RuntimeError("Emulator did not become ready")


def containers() -> None:
    results: dict[str, Any] = {
        "environment": "local Docker RIE, not managed Lambda",
        "memory_limit_bytes": 1024**3,
        "cpu_limit": 1,
        "read_only_root": True,
        "platform": "linux/amd64",
    }
    event = {
        "version": "2.0",
        "routeKey": "GET /health",
        "rawPath": "/health",
        "rawQueryString": "",
        "headers": {"host": "localhost"},
        "requestContext": {
            "http": {
                "method": "GET",
                "path": "/health",
                "sourceIp": "127.0.0.1",
                "protocol": "HTTP/1.1",
            },
            "requestId": "step08",
            "stage": "$default",
        },
        "isBase64Encoded": False,
    }
    for target, port in (("api", 8083), ("pipeline", 8084)):
        name = "urbanlens-step08-" + target
        started = time.perf_counter()
        docker(
            "run",
            "--detach",
            "--name",
            name,
            "--read-only",
            "--memory",
            "1g",
            "--cpus",
            "1",
            "--cap-drop",
            "ALL",
            "--security-opt",
            "no-new-privileges",
            "--tmpfs",
            "/tmp:rw,nosuid,nodev,size=1024m",
            "-p",
            f"127.0.0.1:{port}:8080",
            "--mount",
            f"type=bind,source={ROOT / 'data'},target=/inputs,readonly",
            "urbanlens-" + target + ":step08",
        )
        try:
            wait_ready(port)
            results[target + "_container_start_seconds"] = time.perf_counter() - started
            if target == "api":
                timings = []
                for _ in range(6):
                    payload, seconds = invoke(port, event)
                    require(
                        payload.get("statusCode") == 200
                        and json.loads(payload["body"])["status"] == "ok",
                        "Health route failed",
                    )
                    timings.append(seconds)
                results["api"] = {
                    "fresh_emulator_first_invoke_seconds": timings[0],
                    "warm_invoke_seconds": timings[1:],
                    "warm_median_seconds": statistics.median(timings[1:]),
                    "scope": "health only; not an analytical benchmark",
                }
                inventory = docker(
                    "exec",
                    name,
                    "/var/lang/bin/python",
                    "-c",
                    "import importlib.util,json; print(json.dumps({n:importlib.util.find_spec(n) "
                    "is not None for n in ['fastapi','mangum','dbt','shapely','openpyxl']}))",
                )
                results["api_dependency_presence"] = json.loads(inventory)
            else:
                records = []
                for part in range(1, 8):
                    print("Measuring independent company part", part, "of 7", flush=True)
                    payload, wall = invoke(port, {"task": "company-part", "part": part})
                    metrics = payload["metrics"]
                    artifact = base64.b64decode(payload["artifact_base64"], validate=True)
                    require(
                        hashlib.sha256(artifact).hexdigest() == metrics["artifact_sha256"],
                        "Partition artifact checksum mismatch",
                    )
                    require(metrics["part"] == part, "Partition identity mismatch")
                    metrics["emulator_invoke_seconds"] = wall
                    OUT.mkdir(parents=True, exist_ok=True)
                    (OUT / f"company-part-{part}.jsonl.gz").write_bytes(artifact)
                    save(f"company-part-{part}.json", metrics)
                    records.append(metrics)
                    print("Part", part, "seconds:", metrics["seconds"], flush=True)
                # Replay a completed part, as a coordinator retry would do after a lost response.
                replay, _ = invoke(port, {"task": "company-part", "part": 1})
                require(
                    replay["metrics"]["artifact_sha256"] == records[0]["artifact_sha256"],
                    "Retry changed partition output",
                )
                results["company_partitions"] = records
                results["coordinator"] = reconcile_partitions(records)
                results["coordinator"]["retry_artifact_identical"] = True
                results["managed_lambda_status"] = "not tested; RIE omits AWS service constraints"
            save("containers.json", results)
        finally:
            docker("rm", "--force", name)
    print("Container measurements complete; temporary containers removed", flush=True)


def database_dsn(target: str) -> str:
    values = dotenv_values(ROOT / ".env")
    if target == "neon":
        value = os.environ.get("URBANLENS_PIPELINE_DATABASE_URL") or values.get(
            "URBANLENS_PIPELINE_DATABASE_URL"
        )
        require(bool(value), "Save the direct Neon URL privately in .env first")
        assert value is not None
        settings = conninfo_to_dict(value)
        if "-pooler." in str(settings.get("host", "")):
            value = make_conninfo(value, host=str(settings["host"]).replace("-pooler.", ".", 1))
            settings = conninfo_to_dict(value)
        require(str(settings.get("host", "")).endswith(".neon.tech"), "Expected Neon host")
        require(settings.get("sslmode") in {"require", "verify-full"}, "Neon TLS is required")
        return value
    return make_conninfo(
        host=values.get("LOCAL_POSTGRES_HOST") or "127.0.0.1",
        port=values.get("LOCAL_POSTGRES_PORT") or "54322",
        dbname=values.get("LOCAL_POSTGRES_DB") or "urbanlens",
        user=values.get("LOCAL_POSTGRES_USER") or "urbanlens_local",
        password=values.get("LOCAL_POSTGRES_PASSWORD") or "",
    )


def smoke() -> None:
    """Check final rebuilt images without repeating the complete seven-part measurement."""
    name = "urbanlens-step08-smoke"
    image = "urbanlens-pipeline:step08"
    docker(
        "run",
        "--detach",
        "--name",
        name,
        "--read-only",
        "--memory",
        "1g",
        "--cpus",
        "1",
        "--tmpfs",
        "/tmp:rw,nosuid,nodev,size=1024m",
        "-p",
        "127.0.0.1:8084:8080",
        "--mount",
        f"type=bind,source={ROOT / 'data'},target=/inputs,readonly",
        image,
    )
    try:
        wait_ready(8084)
        payload, wall = invoke(8084, {"task": "company-part", "part": 7})
        old = json.loads((OUT / "company-part-7.json").read_text(encoding="utf-8"))
        require(
            payload["metrics"]["artifact_sha256"] == old["artifact_sha256"],
            "Final image changed partition output",
        )
        packages = json.loads(
            docker(
                "exec",
                name,
                "/var/lang/bin/python",
                "-c",
                "import importlib.metadata,json; print(json.dumps({n:importlib.metadata.version(n) "
                "for n in ['boto3','dbt-core','dbt-postgres','shapely','pyproj']}))",
            )
        )
        save(
            "final-image-smoke.json",
            {
                "pipeline_partition": 7,
                "artifact_identical": True,
                "invoke_seconds": wall,
                "pipeline_packages": packages,
                "pipeline_image_id": json.loads(docker("image", "inspect", image))[0]["Id"],
            },
        )
        print("Final pipeline image and included AWS SDK verified", flush=True)
    finally:
        docker("rm", "--force", name)


def database(target: str) -> None:
    from urbanlens.feasibility_db import benchmark

    OUT.mkdir(parents=True, exist_ok=True)
    metrics = benchmark(ROOT / "data", database_dsn(target), OUT)
    metrics["target"] = target
    metrics["measured_at"] = datetime.now(UTC).isoformat()
    save("database-" + target + ".json", metrics)
    print(
        target,
        "database benchmark:",
        metrics["venues"],
        "source records;",
        metrics["schema_total_bytes"],
        "schema bytes;",
        round(metrics["total_seconds"], 2),
        "seconds",
        flush=True,
    )


def database_container() -> None:
    settings = database_dsn("local")
    dsn = make_conninfo(settings, host="db", port="5432")
    environment = ROOT / ".cache/runtime-db.env"
    require("\n" not in dsn and "\r" not in dsn, "Multiline environment value")
    environment.write_text("URBANLENS_FEASIBILITY_DATABASE_URL=" + dsn + "\n", encoding="utf-8")
    name = "urbanlens-step08-db"
    try:
        docker(
            "run",
            "--detach",
            "--name",
            name,
            "--read-only",
            "--memory",
            "1g",
            "--cpus",
            "1",
            "--network",
            "urbanlens-local_default",
            "--env-file",
            str(environment),
            "--tmpfs",
            "/tmp:rw,nosuid,nodev,size=1024m",
            "-p",
            "127.0.0.1:8084:8080",
            "--mount",
            f"type=bind,source={ROOT / 'data'},target=/inputs,readonly",
            "urbanlens-pipeline:step08",
        )
    finally:
        environment.unlink(missing_ok=True)
    try:
        wait_ready(8084)
        payload, wall = invoke(8084, {"task": "database-local"})
        payload["emulator_invoke_seconds"] = wall
        payload["environment"] = "Linux pipeline RIE; external local PostGIS server"
        save("database-container.json", payload)
        print("Pipeline container load/dbt/memory measured:", payload["venues"], "rows", flush=True)
    finally:
        docker("rm", "--force", name)


def neon_read() -> None:
    """A separate scratch reader using transaction-local settings and explicit schemas."""
    from psycopg import sql

    values = dotenv_values(ROOT / ".env")
    direct = database_dsn("neon")
    settings = conninfo_to_dict(direct)
    role = "urbanlens_feasibility_reader"
    configured = values.get("URBANLENS_API_DATABASE_URL")
    if not configured:
        password = secrets.token_urlsafe(32)
        with psycopg.connect(direct, autocommit=True, connect_timeout=20) as connection:
            require(
                connection.execute("SELECT 1 FROM pg_roles WHERE rolname=%s", (role,)).fetchone()
                is None,
                "Reader exists; configure its URL rather than rotating its password",
            )
            connection.execute(
                sql.SQL("CREATE ROLE {} LOGIN PASSWORD {} CONNECTION LIMIT 2").format(
                    sql.Identifier(role), sql.Literal(password)
                )
            )
            connection.execute(
                sql.SQL("ALTER ROLE {} SET default_transaction_read_only=on").format(
                    sql.Identifier(role)
                )
            )
            connection.execute(
                sql.SQL("ALTER ROLE {} SET statement_timeout='3s'").format(sql.Identifier(role))
            )
            connection.execute(
                sql.SQL("GRANT USAGE ON SCHEMA urbanlens_feasibility TO {}").format(
                    sql.Identifier(role)
                )
            )
            connection.execute(
                sql.SQL("GRANT SELECT ON ALL TABLES IN SCHEMA urbanlens_feasibility TO {}").format(
                    sql.Identifier(role)
                )
            )
        first, rest = str(settings["host"]).split(".", 1)
        host = first + "-pooler." + rest
        configured = (
            "postgresql://" + role + ":" + urllib.parse.quote(password, safe="") + "@" + host
        )
        configured += "/" + urllib.parse.quote(str(settings["dbname"]), safe="")
        configured += "?sslmode=require&channel_binding=require"
        set_key(str(ROOT / ".env"), "URBANLENS_API_DATABASE_URL", configured)
    if not values.get("URBANLENS_DBT_DATABASE_URL"):
        original = str(values.get("URBANLENS_PIPELINE_DATABASE_URL") or direct)
        parsed = urllib.parse.urlsplit(original)
        if parsed.scheme in {"postgres", "postgresql"}:
            credentials, hostpart = parsed.netloc.rsplit("@", 1)
            original = urllib.parse.urlunsplit(
                parsed._replace(netloc=credentials + "@" + hostpart.replace("-pooler.", ".", 1))
            )
        set_key(str(ROOT / ".env"), "URBANLENS_PIPELINE_DATABASE_URL", original)
        set_key(str(ROOT / ".env"), "URBANLENS_DBT_DATABASE_URL", original)
    reader_settings = conninfo_to_dict(configured)
    if reader_settings.get("user") == role:
        # dbt replaces its scratch model; regrant SELECT when replaying the load/build.
        with psycopg.connect(direct, connect_timeout=20) as connection:
            connection.execute(
                sql.SQL("GRANT SELECT ON ALL TABLES IN SCHEMA urbanlens_feasibility TO {}").format(
                    sql.Identifier(role)
                )
            )
    require(
        "-pooler." in str(reader_settings.get("host", "")),
        "Reader must use the pooled endpoint",
    )
    observations = []
    for _ in range(4):
        start = time.perf_counter()
        with psycopg.connect(configured, connect_timeout=20, prepare_threshold=None) as connection:
            with connection.transaction():
                connection.execute("SET TRANSACTION READ ONLY")
                connection.execute("SET LOCAL statement_timeout='3s'")
                count = connection.execute(
                    "SELECT sum(venue_count) FROM urbanlens_feasibility.gold_probe"
                ).fetchone()
                rights = connection.execute("""SELECT
                    has_table_privilege(current_user,'urbanlens_feasibility.venues','SELECT'),
                    has_table_privilege(current_user,'urbanlens_feasibility.venues','INSERT'),
                    current_setting('transaction_read_only')""").fetchone()
                require(rights == (True, False, "on"), "Reader role/transaction check failed")
                require(count == (5112,), "Pooled result differs from source benchmark")
        observations.append(time.perf_counter() - start)
    save(
        "neon-pooled-reads.json",
        {
            "requests": 4,
            "seconds": observations,
            "prepare_threshold": None,
            "application_connections_per_request": 1,
            "probe_role_connection_limit": 2,
            "read_only_verified": True,
            "scope": "scratch schema only; first read is not certified as a database wake",
            "measured_at": datetime.now(UTC).isoformat(),
        },
    )
    print("Neon pooled reader: 4/4 reconciled reads; write privilege denied", flush=True)


def backup(target: str) -> None:
    """Use PostgreSQL 17 clients in the existing container; verify restore in a new local DB."""
    from psycopg import sql

    settings = conninfo_to_dict(database_dsn(target))
    if target == "local":
        settings["host"] = "127.0.0.1"
        settings["port"] = "5432"
    environment_path = ROOT / ".cache/runtime-pg.env"
    environment_path.write_text(
        "".join(
            "PG" + name.upper() + "=" + str(settings[name]) + "\n"
            for name in ("host", "port", "user", "password", "dbname", "sslmode")
            if name in settings
        ),
        encoding="utf-8",
    )
    # pg_dump uses PGDATABASE, not PGDBNAME.
    text = environment_path.read_text(encoding="utf-8").replace("PGDBNAME=", "PGDATABASE=")
    environment_path.write_text(text, encoding="utf-8")
    started = time.perf_counter()
    try:
        data = docker(
            "exec",
            "--env-file",
            str(environment_path),
            "urbanlens-local-db-1",
            "pg_dump",
            "--format=custom",
            "--schema=urbanlens_feasibility",
            "--no-owner",
            "--no-privileges",
            binary=True,
        )
    finally:
        environment_path.unlink(missing_ok=True)
    dump_seconds = time.perf_counter() - started
    OUT.mkdir(parents=True, exist_ok=True)
    path = OUT / ("database-" + target + ".dump")
    path.write_bytes(data)
    restore_name = "urbanlens_feasibility_restore"
    local_dsn = database_dsn("local")
    with psycopg.connect(local_dsn, autocommit=True) as admin:
        with admin.cursor() as cursor:
            cursor.execute("SELECT 1 FROM pg_database WHERE datname=%s", (restore_name,))
            require(
                cursor.fetchone() is None, "Restore database already exists; will not overwrite"
            )
            cursor.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(restore_name)))
        try:
            restore_dsn = make_conninfo(local_dsn, dbname=restore_name)
            with psycopg.connect(restore_dsn) as connection:
                connection.execute("CREATE EXTENSION postgis")
            docker("cp", str(path), "urbanlens-local-db-1:/tmp/urbanlens-feasibility.dump")
            restore_start = time.perf_counter()
            # Owner/user is passed through an env file, never password arguments.
            values = dotenv_values(ROOT / ".env")
            docker(
                "exec",
                "urbanlens-local-db-1",
                "pg_restore",
                "--no-owner",
                "--no-privileges",
                "--exit-on-error",
                "-U",
                values.get("LOCAL_POSTGRES_USER") or "urbanlens_local",
                "-d",
                restore_name,
                "/tmp/urbanlens-feasibility.dump",
            )
            with psycopg.connect(restore_dsn) as connection:
                counts = connection.execute(
                    "SELECT count(*) FROM urbanlens_feasibility.venues"
                ).fetchone()
                gold = connection.execute(
                    "SELECT sum(venue_count) FROM urbanlens_feasibility.gold_probe"
                ).fetchone()
                require(
                    counts is not None and gold is not None and counts[0] == gold[0],
                    "Restored source/model counts differ",
                )
                assert counts is not None
                original = json.loads((OUT / ("database-" + target + ".json")).read_text())
                require(counts[0] == original["venues"], "Restore differs from source benchmark")
            save(
                "backup-" + target + ".json",
                {
                    "bytes": len(data),
                    "sha256": hashlib.sha256(data).hexdigest(),
                    "dump_seconds": dump_seconds,
                    "restore_seconds": time.perf_counter() - restore_start,
                    "restored_source_rows": counts[0],
                    "restore_reconciled": True,
                    "target": target,
                    "restore_target": "temporary local database",
                },
            )
        finally:
            admin.execute(
                sql.SQL("DROP DATABASE {} WITH (FORCE)").format(sql.Identifier(restore_name))
            )
            docker("exec", "urbanlens-local-db-1", "rm", "-f", "/tmp/urbanlens-feasibility.dump")
    print(target, "backup/restore verified:", len(data), "bytes", flush=True)


def neon_wake() -> None:
    """One idle-window observation; no periodic database requests or keep-alives."""
    values = dotenv_values(ROOT / ".env")
    dsn = values.get("URBANLENS_API_DATABASE_URL")
    require(bool(dsn), "Configure/test the pooled reader first")
    assert dsn is not None
    with psycopg.connect(dsn, connect_timeout=20, prepare_threshold=None) as connection:
        before = connection.execute("SELECT pg_postmaster_start_time()").fetchone()
    print(
        "Beginning six-minute idle window; no database connections are held or polled", flush=True
    )
    for _ in range(6):
        time.sleep(60)
    observations = []
    after = None
    for _ in range(4):
        started = time.perf_counter()
        with psycopg.connect(dsn, connect_timeout=30, prepare_threshold=None) as connection:
            connected = time.perf_counter()
            after = connection.execute(
                "SELECT pg_postmaster_start_time(),sum(venue_count) "
                "FROM urbanlens_feasibility.gold_probe"
            ).fetchone()
            require(after is not None and after[1] == 5112, "Wake read failed to reconcile")
        observations.append(
            {"connect_seconds": connected - started, "total_seconds": time.perf_counter() - started}
        )
    assert before is not None and after is not None
    save(
        "neon-wake.json",
        {
            "idle_seconds": 360,
            "requests_during_idle": 0,
            "postmaster_before": before[0].isoformat(),
            "postmaster_after": after[0].isoformat(),
            "database_process_restarted": after[0] > before[0],
            "first_after_idle": observations[0],
            "subsequent_warm": observations[1:],
            "interpretation": "Restart after idle supports wake attribution; provider suspension "
            "status was not independently observed",
            "measured_at": datetime.now(UTC).isoformat(),
        },
    )
    print(
        "Neon idle/warm observation complete; database restarted:", after[0] > before[0], flush=True
    )


def spatial_compare() -> None:
    from urbanlens.spatial import DISTANCE_EPSILON_METRES

    observations = {}
    for target in ("local", "neon"):
        with psycopg.connect(database_dsn(target), connect_timeout=20) as connection:
            fingerprint = connection.execute("""SELECT md5(string_agg(
                id||':'||coalesce(lsoa_code,'unassigned'),',' ORDER BY id))
                FROM urbanlens_feasibility.venues""").fetchone()
            geometry = connection.execute("""SELECT count(*),bool_and(ST_IsValid(geom))
                FROM urbanlens_feasibility.areas""").fetchone()
            require(geometry == (165, True), "Invalid/incomplete loaded area geometry")
            radii = connection.execute(
                """WITH p AS (
                SELECT ST_SetSRID(ST_MakePoint(-0.14,50.82),4326)::geography p)
                SELECT r,ST_DWithin(p,ST_Project(p,r,0),r+%s),
                       ST_DWithin(p,ST_Project(p,r+0.01,0),r+%s)
                FROM p CROSS JOIN (VALUES(250),(500),(1000),(2000)) radius(r) ORDER BY r""",
                (DISTANCE_EPSILON_METRES, DISTANCE_EPSILON_METRES),
            ).fetchall()
            require(
                all(row[1] is True and row[2] is False for row in radii),
                "Inclusive geography radius contract failed",
            )
            observations[target] = {
                "assignment_fingerprint": fingerprint,
                "area_count_valid": geometry,
                "radius_probes": radii,
            }
    require(
        observations["local"]["assignment_fingerprint"]
        == observations["neon"]["assignment_fingerprint"],
        "PostGIS assignment results differ",
    )
    save("spatial-compatibility.json", observations)
    print("Local/Neon spatial assignments and all four inclusive radius probes agree", flush=True)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    tasks: dict[str, Callable[[], None]] = {
        "build": build,
        "containers": containers,
        "smoke": smoke,
        "database-local": lambda: database("local"),
        "database-container": database_container,
        "database-neon": lambda: database("neon"),
        "backup-local": lambda: backup("local"),
        "backup-neon": lambda: backup("neon"),
        "neon-read": neon_read,
        "neon-wake": neon_wake,
        "spatial": spatial_compare,
    }
    parser.add_argument("task", choices=list(tasks))
    task = parser.parse_args().task
    tasks[task]()


if __name__ == "__main__":
    main()

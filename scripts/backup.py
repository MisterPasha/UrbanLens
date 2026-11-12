"""Native private logical backup; restore only into a newly created local database."""

from __future__ import annotations

import gzip
import hashlib
import io
import json
import os
import subprocess
import tarfile
import tempfile
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from psycopg import sql
from psycopg.conninfo import conninfo_to_dict

from scripts.database import administrator, credential, scratch_database
from scripts.dev import ROOT, tool
from urbanlens.database.migrations import connect, grants
from urbanlens.ingestion.contracts import Artifact
from urbanlens.ingestion.interfaces import ArtifactStore
from urbanlens.ingestion.storage import chunks
from urbanlens.silver.classification import save_json
from urbanlens.silver.eligibility import digest
from urbanlens.silver.operations import fingerprint, retained_schemas


def code_archive(store: ArtifactStore) -> Artifact:
    paths: list[Path] = []
    directories = ["src", "scripts", "config", "dbt"]
    packaged = not (ROOT / "src").is_dir() and (ROOT / "urbanlens").is_dir()
    if packaged:
        directories.append("urbanlens")
    for directory in directories:
        paths.extend(
            p
            for p in (ROOT / directory).rglob("*")
            if p.is_file()
            and p.suffix in {".py", ".json", ".sql", ".yml", ".yaml", ".toml"}
            and not set(p.parts) & {"__pycache__", "target", "logs", "dbt_packages"}
            and p.name != "profiles.yml"
        )
    paths.extend(ROOT / name for name in ("pyproject.toml", "uv.lock"))
    content = {}
    for path in sorted(set(paths)):
        name = path.relative_to(ROOT).as_posix()
        if packaged and name.startswith("urbanlens/"):
            name = "src/" + name
        content[name] = path.read_bytes()
    hashes = {name: hashlib.sha256(body).hexdigest() for name, body in content.items()}
    buffer = io.BytesIO()
    with gzip.GzipFile(fileobj=buffer, mode="wb", mtime=0) as zipped:
        with tarfile.open(fileobj=zipped, mode="w") as archive:
            for name, body in content.items():
                info = tarfile.TarInfo(name)
                info.size = len(body)
                info.mode = 0o600
                archive.addfile(info, io.BytesIO(body))
    return store.put_immutable(
        "evidence/maintenance/code/" + digest(hashes) + ".tar.gz",
        iter([buffer.getvalue()]),
        media_type="application/gzip",
        compression="none",
    )


def pg_environment(dsn: str, *, local: bool) -> dict[str, str]:
    settings = conninfo_to_dict(dsn)
    if local:
        settings.update(host="127.0.0.1", port="5432")
    names = {
        "host": "PGHOST",
        "port": "PGPORT",
        "user": "PGUSER",
        "password": "PGPASSWORD",
        "dbname": "PGDATABASE",
        "sslmode": "PGSSLMODE",
        "channel_binding": "PGCHANNELBINDING",
    }
    result = {names[k]: str(v) for k, v in settings.items() if k in names and v is not None}
    if any("\n" in value or "\r" in value for value in result.values()):
        raise ValueError("Multiline database parameters are unsupported")
    return result


def native(
    arguments: list[str], env: dict[str, str], output: Path | None = None, data: bytes | None = None
) -> None:
    """Keep credentials in a temporary ignored env file and remove it on every path."""
    if os.environ.get("URBANLENS_CLOUD_RUNTIME") == "1":
        with output.open("wb") if output else io.BytesIO() as stream:
            completed = subprocess.run(
                arguments,
                input=data,
                env={**os.environ, **env},
                stdout=stream if output else subprocess.PIPE,
                stderr=subprocess.PIPE,
                timeout=300,
                check=False,
            )
        if completed.returncode:
            raise RuntimeError("Native backup command failed; private details withheld")
        return
    docker = tool("docker")
    cache = ROOT / ".cache"
    cache.mkdir(exist_ok=True)
    environment = os.environ.copy()
    environment["PATH"] = str(Path(docker).parent) + os.pathsep + environment.get("PATH", "")
    with tempfile.NamedTemporaryFile(
        mode="w", suffix=".env", prefix="backup-pg-", dir=cache, delete=False, encoding="utf-8"
    ) as handle:
        env_path = Path(handle.name)
        handle.write("".join(k + "=" + v + "\n" for k, v in env.items()))
    try:
        command = [
            docker,
            "exec",
            "-i",
            "--env-file",
            str(env_path),
            "urbanlens-local-db-1",
            *arguments,
        ]
        with output.open("wb") if output else io.BytesIO() as stream:
            result = subprocess.run(
                command,
                input=data,
                stdout=stream if output else subprocess.PIPE,
                stderr=subprocess.PIPE,
                env=environment,
                check=False,
                timeout=300,
            )
        if result.returncode:
            message = result.stderr.decode(errors="replace")
            if env.get("PGPASSWORD"):
                message = message.replace(env["PGPASSWORD"], "[redacted]")
            (cache / "backup-error.txt").write_text(message, encoding="utf-8")
            raise RuntimeError(
                "Native backup/restore failed; inspect private .cache/backup-error.txt"
            )
    finally:
        env_path.unlink(missing_ok=True)


def restore_check(
    store: ArtifactStore, artifact: Artifact, expected: dict[str, Any]
) -> dict[str, Any]:
    data = b"".join(store.read_verified(artifact))
    started = time.perf_counter()
    with scratch_database("local") as database:
        if not database.startswith("urbanlens_step19_test_"):
            raise ValueError("Restore target is not an isolated temporary database")
        with connect(administrator("local")) as owner:
            owner.execute(
                sql.SQL("GRANT CREATE ON DATABASE {} TO urbanlens_migrator").format(
                    sql.Identifier(database)
                )
            )
        env = pg_environment(credential("local", "migration", database=database), local=True)
        native(
            [
                "pg_restore",
                "--no-owner",
                "--no-privileges",
                "--clean",
                "--if-exists",
                "--exit-on-error",
                "--dbname",
                database,
            ],
            env,
            data=data,
        )
        with connect(credential("local", "migration", database=database)) as restored:
            actual = fingerprint(restored)
            if actual != expected:
                raise ValueError("Restored logical contents differ from the backup snapshot")
            grants(restored)
        with connect(administrator("local")) as owner:
            owner.execute(
                sql.SQL("REVOKE CREATE ON DATABASE {} FROM urbanlens_migrator").format(
                    sql.Identifier(database)
                )
            )
        with connect(credential("local", "api", database=database)) as api:
            if api.execute(
                "SELECT has_schema_privilege(current_user,'ul_silver','USAGE')"
            ).fetchone() != (False,):
                raise ValueError("Restored API role can access private Silver")
    return dict(
        restored=True,
        logical_contents_match=True,
        api_private_access_denied=True,
        temporary_database_removed=True,
        restore_seconds=round(time.perf_counter() - started, 3),
    )


def run(
    target: str, store: ArtifactStore, *, tier: str = "both", verify_restore: bool = True
) -> dict[str, Any]:
    now = datetime.now(UTC)
    daily = f"backups/{target}/daily/{now.date()}"
    weekly = f"backups/{target}/weekly/{now.isocalendar().year}-W{now.isocalendar().week:02d}"
    keys = [daily, weekly] if tier == "both" else [daily if tier == "daily" else weekly]
    manifests = []
    missing = []
    for key in keys:
        existing = store.stat(key + ".json")
        if existing:
            manifest = json.loads(b"".join(store.read_verified(existing)))
            manifests.append(manifest)
        else:
            missing.append(key)
    archive = code_archive(store)
    if missing:
        cache = ROOT / ".cache"
        with tempfile.TemporaryDirectory(prefix="silver-backup-", dir=cache) as directory:
            path = Path(directory) / "application.dump"
            started = time.perf_counter()
            with connect(credential(target, "migration")) as c:
                c.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
                c.execute("SELECT pg_advisory_xact_lock(192020,1)")
                snapshot = c.execute("SELECT pg_export_snapshot()").fetchone()
                assert snapshot is not None
                expected = fingerprint(c)
                native(
                    [
                        "pg_dump",
                        "--format=custom",
                        "--no-owner",
                        "--no-privileges",
                        *("--schema=" + schema for schema in retained_schemas(c)),
                        "--snapshot",
                        snapshot[0],
                    ],
                    pg_environment(credential(target, "migration"), local=target == "local"),
                    path,
                )
            for key in missing:
                with path.open("rb") as source:
                    artifact = store.put_immutable(
                        key + ".dump",
                        chunks(source),
                        media_type="application/octet-stream",
                        compression="none",
                    )
                manifest = dict(
                    version="silver-backup-v1",
                    target=target,
                    captured_at=now.isoformat(),
                    artifact=artifact.model_dump(mode="json"),
                    logical_contents=expected,
                    code=archive.model_dump(mode="json"),
                    dump_seconds=round(time.perf_counter() - started, 3),
                    includes="application schemas/data; bootstrap roles and PostGIS before restore",
                )
                store.put_immutable(
                    key + ".json",
                    iter([(json.dumps(manifest, sort_keys=True) + "\n").encode()]),
                    media_type="application/json",
                    compression="none",
                )
                manifests.append(manifest)
    checks = []
    verified = set()
    for manifest in manifests:
        artifact = Artifact.model_validate(manifest["artifact"])
        if artifact.sha256 not in verified:
            if verify_restore:
                checks.append(restore_check(store, artifact, manifest["logical_contents"]))
            else:
                for _ in store.read_verified(artifact):
                    pass
            verified.add(artifact.sha256)
    report = dict(
        target=target,
        manifests=manifests,
        restore_checks=checks,
        restore_verification="passed" if verify_restore and checks else "not_run",
        verified_backup_artifacts=len(verified),
        existing_points_reused=len(keys) - len(missing),
        verification_code=archive.model_dump(mode="json"),
        scheduled=False,
        retention="seven daily/four weekly points; reruns reuse the period's first successful "
        "point",
    )
    proof = save_json(store, "evidence/maintenance/backups", report)
    return dict(**report, evidence=proof.model_dump(mode="json"))

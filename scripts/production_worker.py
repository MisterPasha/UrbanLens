"""Bounded CodeBuild worker; all intermediate manifests are resumable S3 checkpoints."""

from __future__ import annotations

import hashlib
import json
import os
import re
import sqlite3
import subprocess
import sys
import tempfile
import traceback
from datetime import UTC, datetime
from typing import Any

from psycopg import sql
from psycopg.conninfo import conninfo_to_dict

from scripts.database import credential
from scripts.dev import ROOT
from urbanlens.database.migrations import connect, migrate
from urbanlens.ingestion.acceptance import AcceptanceService
from urbanlens.ingestion.archive import load_catalog
from urbanlens.ingestion.cloud import sdk_client
from urbanlens.ingestion.companies import CompanyAdapter, CompanyConfig
from urbanlens.ingestion.contracts import Artifact, Provider
from urbanlens.ingestion.engine import Collector
from urbanlens.ingestion.http import HttpDownloader, HttpPolicy
from urbanlens.ingestion.payload import payload
from urbanlens.ingestion.postcodes import CacheEntry, PostcodeService
from urbanlens.ingestion.reference import ReferenceConfig, check_editions
from urbanlens.ingestion.registry import Registry, S3StateBackend
from urbanlens.ingestion.storage import S3ArtifactStore
from urbanlens.operations.assets import configuration, hydrate
from urbanlens.operations.control import Coordinator
from urbanlens.silver.classification import save_json
from urbanlens.silver.eligibility import Reviews, Taxonomy, digest

CHECKPOINT_FILES = (
    ".cache/bronze-state.json",
    ".cache/silver-s3.json",
    ".cache/classification-s3.json",
    ".cache/gold-neon.json",
    "data/gold/build-neon.json",
    "data/gold/release-neon-prepare.json",
    ".cache/reference-check.json",
)


def classification_preflight() -> None:
    """Reject conflicting configuration versions before collection or source writes."""
    with connect(credential("neon", "pipeline")) as connection:
        connection.execute("SET TRANSACTION READ ONLY")
        for kind, model, filename in (
            ("taxonomy", Taxonomy, "taxonomy.json"),
            ("reviews", Reviews, "eligibility-reviews.json"),
        ):
            definition = model.model_validate_json((ROOT / "config" / filename).read_bytes())
            existing = connection.execute(
                "SELECT sha256 FROM ul_control.classification_definition "
                "WHERE kind=%s AND version=%s",
                (kind, definition.version),
            ).fetchone()
            if existing is not None and existing != (digest(definition.model_dump(mode="json")),):
                raise ValueError(f"Changed {kind} definition requires a new version")


def cleanup_workspaces() -> None:
    """Remove automated scratch schemas only; frozen/registered releases are protected."""
    with connect(credential("neon", "migration")) as connection:
        connection.execute("SELECT pg_advisory_xact_lock(192020,1)")
        connection.execute("SELECT pg_advisory_xact_lock(192033,0)")
        schemas = connection.execute(
            "SELECT nspname FROM pg_namespace n "
            "WHERE nspname ~ '^ul_candidate_auto_[0-9a-f]{20}$' "
            "AND pg_get_userbyid(nspowner)='urbanlens_migrator' "
            "AND NOT EXISTS (SELECT 1 FROM ul_control.data_release d "
            "WHERE d.model_schema=n.nspname)"
        ).fetchall()
        for (schema,) in schemas:
            connection.execute(sql.SQL("DROP SCHEMA {} CASCADE").format(sql.Identifier(schema)))


def allowed() -> None:
    state = S3StateBackend(sdk_client("s3"), os.environ["BRONZE_BUCKET"], "operations")
    coordinator = Coordinator(state, sdk_client("codebuild"), os.environ["BUILD_PROJECT"])
    switches, _ = coordinator.read("switches.json")
    active, _ = coordinator.read("active.json")
    if switches.get("paused", True) or active.get("token") != os.environ["URBANLENS_RUN_TOKEN"]:
        raise ValueError("Processing is paused or this worker no longer owns the run")


def secret_environment() -> None:
    client = sdk_client("ssm")
    for purpose in ("api", "migration", "pipeline", "dbt", "admin"):
        value = client.get_parameter(
            Name=f"/urbanlens/production/database/{purpose}", WithDecryption=True
        )["Parameter"]["Value"]
        key = (
            "URBANLENS_DATABASE_ADMIN_URL"
            if purpose == "admin"
            else f"URBANLENS_{purpose.upper()}_DATABASE_URL"
        )
        os.environ[key] = value


def command(module: str, *arguments: str) -> None:
    allowed()
    completed = subprocess.run(
        [sys.executable, "-m", "scripts." + module, *arguments],
        capture_output=True,
        timeout=1700,
        check=False,
    )
    if completed.returncode:
        report_failure(module, completed.stdout + completed.stderr)
        raise RuntimeError(f"Production stage failed: {module}; last published data retained")


def report_failure(stage: str, output: bytes) -> None:
    """Retain bounded private diagnostics with known credentials removed before upload."""
    text = output.decode(errors="replace")[-32000:]
    for path in sorted((ROOT / ".cache").glob("*error.txt")):
        text += "\n" + path.name + ":\n" + path.read_text(errors="replace")[-8000:]
    for key, value in os.environ.items():
        if value and any(
            word in key for word in ("PASSWORD", "SECRET", "TOKEN", "DATABASE_URL", "ADMIN_URL")
        ):
            text = text.replace(value, "[redacted]")
            if "DATABASE_URL" in key or "ADMIN_URL" in key:
                password = conninfo_to_dict(value).get("password")
                if password:
                    text = text.replace(str(password), "[redacted]")
    proof = save_json(
        S3ArtifactStore(sdk_client("s3"), os.environ["BRONZE_BUCKET"]),
        "evidence/production/failures",
        {"stage": stage, "diagnostics": text[-64000:]},
    )
    print(json.dumps({"failed_stage": stage, "private_diagnostics": proof.uri}), flush=True)


def postcodes(store: Any, state: dict[str, Any]) -> PostcodeService:
    from scripts.source_pipeline import area_codes

    service = PostcodeService(ROOT / ".cache/production-postcodes.sqlite", store, area_codes())
    export = Artifact.model_validate(state["postcode_evidence"])
    now = datetime.now(UTC)
    for item in json.loads(b"".join(store.read_verified(export)))["entries"]:
        entry = CacheEntry.model_validate(item)
        if entry.fetched_at.strftime("%Y-%m") != now.strftime("%Y-%m"):
            entry = entry.model_copy(update={"expires_at": now})
        service.save(entry)
    return service


def refresh_postcodes(service: PostcodeService, keys: list[str]) -> None:
    for offset in range(0, len(keys), 100):
        allowed()
        lookups = service.bulk(keys[offset : offset + 100])
        if any(row.state in {"unavailable", "throttled"} for row in lookups):
            raise ValueError("Postcode quota/provider unavailable; stop without futile retries")


def companies_and_postcodes(store: Any, config: dict[str, Any], state: dict[str, Any]) -> None:
    service = postcodes(store, state)
    with sqlite3.connect(service.path) as c:
        known = [
            row[0] for row in c.execute("SELECT postcode FROM postcode_cache ORDER BY postcode")
        ]
    refresh_postcodes(service, known)
    settings = CompanyConfig.model_validate_json(
        (ROOT / "config/companies-source.json").read_bytes()
    )
    registry = Registry(S3StateBackend(sdk_client("s3"), store.bucket), lease_seconds=2100)
    http = HttpDownloader(
        Provider.COMPANIES_HOUSE, HttpPolicy(max_requests=2, max_retries=1, max_bytes=1_000_000)
    )
    today = datetime.now(UTC).date()
    probe = settings.cycle(today, "discovery", "0" * 64, ("probe",))
    current = registry.current(probe)
    listing_key = (
        f"evidence/production/company-discovery/{today:%Y-%m}/"
        + hashlib.sha256(settings.model_dump_json().encode()).hexdigest()
        + ".json"
    )
    pinned = store.stat(listing_key)
    if pinned:
        descriptor = json.loads(b"".join(store.read_verified(pinned)))
        listing = b"".join(store.read_verified(Artifact.model_validate(descriptor["listing"])))
    elif current and current.cycle.window_start == probe.window_start:
        archived = load_catalog(store, config["bronze"]["catalog_key"])
        with payload(archived, current.partitions[0].artifacts[1]) as stream:
            listing = stream.read()
    else:
        with tempfile.TemporaryFile() as stream:
            http.download("https://download.companieshouse.gov.uk/en_output.html", stream)
            listing = stream.read()
    if not pinned:
        artifact = store.put_immutable(
            f"bronze/companies-house/discovery/{today:%Y-%m}/{hashlib.sha256(listing).hexdigest()}.html",
            iter([listing]),
            media_type="text/html",
            compression="none",
        )
        store.put_immutable(
            listing_key,
            iter([json.dumps({"listing": artifact.model_dump(mode="json")}).encode()]),
            media_type="application/json",
            compression="none",
        )
    adapter = CompanyAdapter(settings, listing, service, set(config["postal_prefilter"]))
    cycle = settings.cycle(
        datetime.now(UTC).date(),
        adapter.edition,
        hashlib.sha256(listing).hexdigest(),
        tuple(sorted(adapter.parts)),
    )
    policy = json.loads((ROOT / "config/source-acceptance.json").read_bytes())
    snapshot = Collector(
        store,
        registry,
        max_seconds=1400,
        max_records=settings.max_national_rows,
        acceptance=AcceptanceService(
            store,
            change_records=policy["change_records"],
            change_fraction=policy["change_fraction"],
        ),
    ).collect(cycle, adapter, settings.rules(), pipeline_version="company-source-v1")
    if snapshot.run.status.value != "accepted":
        raise ValueError("Company cohort was not accepted; no partial replacement")
    # Include new venue postcodes, not only the existing company/postcode cache.
    from scripts.silver import environment
    from urbanlens.silver.pipeline import source_observations

    source, _, snapshots, _, _, resolve = environment("s3")
    new_keys = set()
    for snapshot in snapshots:
        if snapshot.cycle.provider in {Provider.FSA, Provider.OSM}:
            for part in snapshot.partitions:
                for observation in source_observations(snapshot, part, source, resolve):
                    if observation.postcode.value and observation.address.state != "withheld":
                        new_keys.add(observation.postcode.value)
    # Recheck all known postcodes monthly; every response keeps its actual observation date.
    with sqlite3.connect(service.path) as c:
        keys = [r[0] for r in c.execute("SELECT postcode FROM postcode_cache ORDER BY postcode")]
    keys = sorted(set(keys) | new_keys)
    refresh_postcodes(service, keys)
    with sqlite3.connect(service.path) as c:
        entries = [
            json.loads(r[0])
            for r in c.execute("SELECT entry FROM postcode_cache ORDER BY postcode")
        ]
    exported = save_json(
        store, "evidence/postcodes", {"version": "postcode-cache-export-v1", "entries": entries}
    )
    state["postcode_evidence"] = exported.model_dump(mode="json")
    (ROOT / ".cache/bronze-state.json").write_text(json.dumps(state))


def main() -> None:
    os.environ["URBANLENS_CLOUD_RUNTIME"] = "1"
    store, config = configuration()
    hydrate(ROOT, store, config)
    (ROOT / ".cache").mkdir(exist_ok=True)
    (ROOT / "data/gold").mkdir(parents=True, exist_ok=True)
    (ROOT / "data/silver").mkdir(parents=True, exist_ok=True)
    secret_environment()
    allowed()
    state = config["bronze"]
    backend = S3StateBackend(sdk_client("s3"), store.bucket, "operations")
    latest = backend.load("postcode-export.json")
    if latest:
        state["postcode_evidence"] = json.loads(latest[0])
    (ROOT / ".cache/bronze-state.json").write_text(json.dumps(state))
    run_key, token = os.environ["URBANLENS_RUN_KEY"], os.environ["URBANLENS_RUN_TOKEN"]
    if not re.fullmatch(
        r"runs/(refresh|backup|migrate)/[A-Za-z0-9_.-]+\.json", run_key
    ) or not re.fullmatch(r"[0-9a-f]{64}", token):
        raise ValueError("Invalid run identity")
    code_version = os.environ["SOFTWARE_VERSION"]

    def stage(name: str, operation: Any) -> None:
        allowed()
        key = f"evidence/production/{run_key.removesuffix('.json')}/{code_version}/{name}.json"
        existing = store.stat(key)
        if existing:
            checkpoint = json.loads(b"".join(store.read_verified(existing)))
            for filename, artifact in checkpoint["files"].items():
                if filename not in CHECKPOINT_FILES:
                    raise ValueError("Unexpected checkpoint path")
                path = ROOT / filename
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_bytes(b"".join(store.read_verified(Artifact.model_validate(artifact))))
            return
        try:
            operation()
        except Exception:
            report_failure(name, traceback.format_exc().encode())
            raise
        files = {
            filename: store.put_immutable(
                "evidence/production/files/"
                + hashlib.sha256((ROOT / filename).read_bytes()).hexdigest()
                + ".json",
                iter([(ROOT / filename).read_bytes()]),
                media_type="application/json",
                compression="none",
            ).model_dump(mode="json")
            for filename in CHECKPOINT_FILES
            if (ROOT / filename).is_file()
        }
        store.put_immutable(
            key,
            iter(
                [
                    json.dumps(
                        {
                            "stage": name,
                            "files": files,
                            "completed_at": datetime.now(UTC).isoformat(),
                        }
                    ).encode()
                ]
            ),
            media_type="application/json",
            compression="none",
        )
        print(json.dumps({"completed_stage": name}), flush=True)

    kind = os.environ["URBANLENS_JOB"]
    if kind == "migrate":
        with connect(credential("neon", "migration")) as c:
            migrate(c)
        return
    if kind == "refresh":
        classification_preflight()
        import boto3
        from botocore.config import Config

        client = boto3.client(
            "lambda",
            config=Config(read_timeout=310, connect_timeout=5, retries={"total_max_attempts": 1}),
        )

        def collect_source(provider: str) -> None:
            response = client.invoke(
                FunctionName=os.environ["COLLECTOR_FUNCTION"],
                InvocationType="RequestResponse",
                Payload=json.dumps({"provider": provider}).encode(),
            )
            body = json.loads(response["Payload"].read())
            if response.get("FunctionError") or body.get("status") != "accepted":
                raise ValueError("Source collection was not accepted: " + provider)

        for provider in ("fsa", "osm", "companies-house-profiles"):
            stage(provider, lambda provider=provider: collect_source(provider))
        stage("companies-postcodes", lambda: companies_and_postcodes(store, config, state))

        def reference_check() -> None:
            settings = ReferenceConfig.model_validate_json(
                (ROOT / "config/reference-source.json").read_bytes()
            )
            report = check_editions(
                settings,
                (ROOT / "reference/geography/manifest.json").read_bytes(),
                store,
                baseline_directory=ROOT / "data/reference/geography",
            )
            report["check_passed"] = not report["population_discovery"][
                "different_from_pinned"
            ] and all(
                item["bytes_unchanged"] or item["edition_indicators_unchanged"] is True
                for item in report["products"]
            )
            proof = save_json(store, "evidence/reference-checks", report)
            (ROOT / ".cache/reference-check.json").write_text(
                json.dumps({"report": report, "artifact": proof.model_dump(mode="json")})
            )

        stage("reference", reference_check)
        stage("silver-build", lambda: command("silver", "build", "--storage", "s3"))
        stage(
            "silver-load", lambda: command("silver", "load", "--storage", "s3", "--target", "neon")
        )
        stage("classify-build", lambda: command("classify", "build", "--storage", "s3"))
        stage(
            "classify-load",
            lambda: command("classify", "load", "--storage", "s3", "--target", "neon"),
        )
        stage("locations", lambda: command("locations", "--target", "neon"))
        stage("identity", lambda: command("resolve", "run", "--target", "neon", "--apply-reviews"))
        stage("companies", lambda: command("companies", "run", "--target", "neon"))
        stage("history", lambda: command("history", "project", "--target", "neon"))
        stage(
            "release-cleanup", lambda: command("release", "cleanup", "--target", "neon", "--apply")
        )
        stage(
            "retention", lambda: command("maintenance", "retention", "--target", "neon", "--apply")
        )
        schema = (
            "ul_candidate_auto_"
            + hashlib.sha256((run_key + code_version).encode()).hexdigest()[:20]
        )
        stage("gold", lambda: command("gold", "build", "--target", "neon", "--schema", schema))
        stage(
            "freeze",
            lambda: command("release", "prepare", "--target", "neon", "--source-schema", schema),
        )
        stage("candidate-cleanup", cleanup_workspaces)

        from scripts.backup import run as backup

        stage("backup", lambda: backup("neon", store, verify_restore=False))

        def publish() -> None:
            release = json.loads((ROOT / "data/gold/release-neon-prepare.json").read_bytes())
            command("release", "publish", "--target", "neon", "--release-id", release["release_id"])

        stage("publish", publish)
    from scripts.backup import run as backup

    if kind == "backup":
        stage("backup", lambda: backup("neon", store, verify_restore=False))
    # Only a completed worker updates the shared cache pointer. Failure preserves older data.
    _, revision = Coordinator(backend, None, "").read("postcode-export.json")
    backend.compare_set(
        "postcode-export.json",
        revision,
        json.dumps(
            json.loads((ROOT / ".cache/bronze-state.json").read_bytes())["postcode_evidence"]
        ).encode(),
    )
    with connect(credential("neon", "migration")) as c:
        c.execute("SET TRANSACTION READ ONLY")
        size = c.execute("SELECT pg_database_size(current_database())").fetchone()
        connections = c.execute(
            "SELECT count(*) FROM pg_stat_activity WHERE datname=current_database()"
        ).fetchone()
    coordinator = Coordinator(backend, None, "")
    previous, _ = coordinator.read("measurements.json")
    reference = ROOT / ".cache/reference-check.json"
    reference_pending = previous.get("reference_update_pending", False)
    if kind == "refresh" and reference.is_file():
        reference_pending = not json.loads(reference.read_bytes())["report"]["check_passed"]
    summary = {
        "job": kind,
        "completed_at": datetime.now(UTC).isoformat(),
        "database_bytes": size[0] if size else None,
        "database_connections": connections[0] if connections else None,
        "storage_limit_bytes": 1_000_000_000,
        "compute_allowance_cu_hours": 100,
        "compute_usage": "not_available_without_Neon_account_API",
        "restore_status": "checksum_verified; isolated_restore_not_run",
        "reference_update_pending": reference_pending,
    }
    save_json(store, "evidence/production/summaries", summary)
    _, revision = Coordinator(backend, None, "").read("measurements.json")
    backend.compare_set("measurements.json", revision, json.dumps(summary).encode())
    print(json.dumps(summary))


if __name__ == "__main__":
    try:
        main()
    except Exception as error:
        print(
            json.dumps(
                {
                    "status": "failed",
                    "error_type": type(error).__name__,
                    "retry": "manual_only_max_two_per_period",
                }
            )
        )
        raise SystemExit(1) from None

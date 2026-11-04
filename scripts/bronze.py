"""Prepare, bootstrap and verify private S3 Bronze storage; never publish product data."""

from __future__ import annotations

import argparse
import base64
import hashlib
import json
import os
import re
import sqlite3
import subprocess
import time
import tomllib
import zipfile
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from uuid import uuid4

from botocore.exceptions import ClientError
from dotenv import dotenv_values

from scripts.dev import run, uv
from urbanlens.ingestion.acceptance import AcceptanceService, Review, evidence_digest
from urbanlens.ingestion.archive import RelocatedStore, archive_objects, load_catalog
from urbanlens.ingestion.cloud import sdk_client
from urbanlens.ingestion.companies import CompanyAdapter, CompanyConfig, ProfileAdapter
from urbanlens.ingestion.contracts import Artifact, Snapshot, Stage
from urbanlens.ingestion.engine import load_model, metadata_key
from urbanlens.ingestion.errors import Conflict
from urbanlens.ingestion.fsa import FsaAdapter, FsaConfig
from urbanlens.ingestion.osm import OsmAdapter, OsmConfig
from urbanlens.ingestion.payload import payload
from urbanlens.ingestion.postcodes import CacheEntry, PostcodeService, validate_result
from urbanlens.ingestion.reference import ReferenceAdapter, ReferenceConfig
from urbanlens.ingestion.registry import Registry, S3StateBackend, ScopeState
from urbanlens.ingestion.storage import LocalArtifactStore, S3ArtifactStore
from urbanlens.ingestion.verification import recovery_drill, replay_snapshot

ROOT = Path(__file__).resolve().parents[1]
DIRECTORY = ROOT / "data/ingestion"
PACKAGE = ROOT / ".cache/bronze-package"
STATE = ROOT / ".cache/bronze-state.json"
PROFILE = "urbanlens_dev"


def write(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, indent=2) + "\n", encoding="utf-8")


def prepare() -> None:
    """Install only the required locked Lambda dependencies, using Linux binary wheels."""
    lock = tomllib.loads((ROOT / "uv.lock").read_text())
    packages = {p["name"]: p for p in lock["package"]}
    needed = {"pydantic", "httpx", "shapely", "boto3"}
    pending = list(needed)
    while pending:
        for dependency in packages[pending.pop()].get("dependencies", []):
            name = dependency["name"]
            # Windows-only dependencies must not enter the Linux package.
            if name == "colorama":
                continue
            if name not in needed:
                needed.add(name)
                pending.append(name)
    requirements = ROOT / ".cache/bronze-requirements.txt"
    requirements.parent.mkdir(exist_ok=True)
    requirements.write_text(
        "".join(
            f"{name}=={packages[name]['version']} "
            + " ".join(f"--hash={wheel['hash']}" for wheel in packages[name]["wheels"])
            + "\n"
            for name in sorted(needed)
        )
    )
    run(
        [
            uv(),
            "pip",
            "install",
            "--target",
            str(PACKAGE),
            "--python-version",
            "3.13",
            "--python-platform",
            "x86_64-manylinux_2_34",
            "--only-binary",
            ":all:",
            "--no-deps",
            "--require-hashes",
            "--reinstall",
            "-r",
            str(requirements),
        ]
    )
    files: list[tuple[Path, str]] = []
    for path in PACKAGE.rglob("*"):
        if path.is_file() and "__pycache__" not in path.parts and path.suffix != ".pyc":
            files.append((path, path.relative_to(PACKAGE).as_posix()))
    for path in (ROOT / "src/urbanlens").rglob("*.py"):
        files.append((path, path.relative_to(ROOT / "src").as_posix()))
    for name in ("fsa-source", "osm-source", "companies-source", "source-acceptance"):
        path = ROOT / f"config/{name}.json"
        files.append((path, path.relative_to(ROOT).as_posix()))
    for name in (
        "data/reference/geography/city-coverage-wgs84.geojson",
        "reference/ingestion/osm-query.overpassql",
    ):
        files.append((ROOT / name, name))
    output = ROOT / ".cache/bronze-lambda.zip"
    with zipfile.ZipFile(output, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        for path, name in sorted(files, key=lambda item: item[1]):
            info = zipfile.ZipInfo(name, date_time=(2026, 1, 1, 0, 0, 0))
            info.compress_type = zipfile.ZIP_DEFLATED
            info.external_attr = 0o644 << 16
            archive.writestr(info, path.read_bytes())
    with zipfile.ZipFile(output) as archive:
        expanded = sum(item.file_size for item in archive.infolist())
        assert archive.testzip() is None
    if expanded > 250 * 1024 * 1024:
        raise ValueError("Lambda package exceeds the expanded-size limit")
    write(
        DIRECTORY / "bronze-package.json",
        {
            "sha256": hashlib.sha256(output.read_bytes()).hexdigest(),
            "compressed_bytes": output.stat().st_size,
            "expanded_bytes": expanded,
            "dependencies": {n: packages[n]["version"] for n in sorted(needed)},
            "secrets_included": False,
        },
    )
    print(f"Prepared Linux Lambda package: {output.stat().st_size:,} bytes; no credentials.")


def clients() -> dict[str, Any]:
    ca = ROOT / ".cache/aws-system-ca.pem"
    if ca.exists():
        os.environ.setdefault("AWS_CA_BUNDLE", str(ca))
    return {
        name: sdk_client(name, profile=PROFILE)
        for name in ("sts", "cloudformation", "s3", "lambda", "ssm", "events")
    }


def inventory(clients: dict[str, Any]) -> dict[str, str]:
    identity = clients["sts"].get_caller_identity()
    if identity["Arn"].endswith(":root"):
        raise ValueError("Use the non-root development identity")
    stack = clients["cloudformation"].describe_stacks(StackName="urbanlens-bronze")["Stacks"][0]
    if stack["StackStatus"] not in {"CREATE_COMPLETE", "UPDATE_COMPLETE"}:
        raise ValueError("Bronze stack is not ready")
    return {item["OutputKey"]: item["OutputValue"] for item in stack["Outputs"]}


def source_states() -> list[ScopeState]:
    with sqlite3.connect(DIRECTORY / "registry.sqlite") as connection:
        states = [
            ScopeState.model_validate_json(row[0])
            for row in connection.execute("SELECT body FROM scope_state")
        ]
    if len(states) != 5 or any(
        s.current is None or s.current.run.status != Stage.ACCEPTED for s in states
    ):
        raise ValueError("Expected the five accepted local snapshot scopes")
    if any(s.owner is not None for s in states):
        raise ValueError("Stop active local collectors before bootstrap")
    return states


def bootstrap(clients: dict[str, Any], outputs: dict[str, str]) -> None:
    local = LocalArtifactStore(DIRECTORY / "objects")
    remote = S3ArtifactStore(clients["s3"], outputs["Bucket"])
    baselines: list[Snapshot] = []
    expected_artifacts: list[Artifact] = []
    for state in source_states():
        assert state.current is not None
        snapshot = state.current
        digest = evidence_digest(snapshot.cycle, snapshot.partitions)
        review = load_model(local, AcceptanceService(local).key(snapshot.cycle, digest), Review)
        if review is None or review.decision != "approve" or review.evidence_sha256 != digest:
            raise ValueError("A source baseline lacks its evidence-bound approved inspection")
        for part in snapshot.partitions:
            expected_artifacts.extend(part.artifacts)
            normalized = load_model(
                local,
                metadata_key(
                    snapshot.cycle, f"attempts/{snapshot.run.run_id}/{part.partition_id}-silver"
                ),
                Artifact,
            )
            if normalized is None:
                raise ValueError("Accepted baseline lacks its normalized artifact")
            expected_artifacts.append(normalized)
        baselines.append(snapshot)
    for artifact in expected_artifacts:
        for _ in local.read_verified(artifact):
            pass
    catalog = archive_objects(local, remote)
    relocated = RelocatedStore(remote, catalog["objects"])
    for artifact in expected_artifacts:
        target = relocated.aliases.get(artifact.uri)
        if target is None or (target.sha256, target.byte_count) != (
            artifact.sha256,
            artifact.byte_count,
        ):
            raise ValueError("Archived bytes disagree with the inspected source baseline")
    # Preserve the independent postcode-service check and edition inspection alongside snapshots.
    for name in ("postcode-check.json", "reference-edition-check.json", "source-inspection.json"):
        body = (DIRECTORY / name).read_bytes()
        digest = hashlib.sha256(body).hexdigest()
        remote.put_immutable(
            f"evidence/bootstrap-inputs/{digest}-{name}",
            iter([body]),
            media_type="application/json",
            compression="none",
        )
    with sqlite3.connect(DIRECTORY / "postcodes.sqlite") as connection:
        entries = [
            CacheEntry.model_validate_json(row[0])
            for row in connection.execute("SELECT entry FROM postcode_cache ORDER BY postcode")
        ]
    for entry in entries:
        validate_result(entry.result, entry.postcode)
        for _ in local.read_verified(entry.artifact):
            pass
    postcodes = json.dumps(
        {
            "version": "postcode-cache-export-v1",
            "provider": "postcodes-io",
            "entries": [e.model_dump(mode="json") for e in entries],
        },
        sort_keys=True,
    ).encode()
    postcode_evidence = remote.put_immutable(
        f"evidence/bootstrap-inputs/{hashlib.sha256(postcodes).hexdigest()}-postcodes.json",
        iter([postcodes]),
        media_type="application/json",
        compression="none",
    )
    content = json.dumps(catalog, sort_keys=True).encode()
    catalog_key = f"evidence/bootstrap/{hashlib.sha256(content).hexdigest()}.json"
    remote.put_immutable(
        catalog_key, iter([content]), media_type="application/json", compression="none"
    )
    backend = S3StateBackend(clients["s3"], outputs["Bucket"])
    originals = []
    for snapshot in baselines:
        key = Registry.key(snapshot.cycle)
        existing = backend.load(key)
        if existing is None:
            seeded = ScopeState(current=snapshot, active_run=snapshot.run)
            backend.compare_set(key, None, seeded.model_dump_json().encode())
        elif ScopeState.model_validate_json(existing[0]).current != snapshot:
            raise ValueError("Cloud current differs; bootstrap cannot replace an existing snapshot")
        originals.append(snapshot.model_dump(mode="json"))
    # Local and cloud attempts share one initial monthly allowance, not two fresh budgets.
    month = time.strftime("%Y-%m", time.gmtime())
    with sqlite3.connect(DIRECTORY / "profile-requests.sqlite") as connection:
        row = connection.execute(
            "SELECT used FROM profile_requests WHERE month=?", (month,)
        ).fetchone()
    used = 0 if row is None else int(row[0])
    budget_backend = S3StateBackend(clients["s3"], outputs["Bucket"], "budgets")
    existing_budget = budget_backend.load(f"profiles/{month}.json")
    if existing_budget is None:
        budget_backend.compare_set(
            f"profiles/{month}.json", None, json.dumps({"used": used}).encode()
        )
    elif int(json.loads(existing_budget[0])["used"]) < used:
        budget_backend.compare_set(
            f"profiles/{month}.json", existing_budget[1], json.dumps({"used": used}).encode()
        )
    write(
        STATE,
        {
            "outputs": outputs,
            "catalog_key": catalog_key,
            "baseline_snapshots": originals,
            "postcode_evidence": postcode_evidence.model_dump(),
        },
    )
    write(
        DIRECTORY / "bronze-upload.json",
        {
            "objects": len(catalog["objects"]),
            "bytes": sum(e["stored"]["byte_count"] for e in catalog["objects"]),
            "all_download_hashes_verified": True,
            "source_dates_rewritten": False,
            "snapshots": len(originals),
            "postcode_service_evidence_retained": True,
        },
    )
    print(
        f"Archived and downloaded/verified {len(catalog['objects'])} objects; "
        "original baselines preserved."
    )


def deploy(clients: dict[str, Any], outputs: dict[str, str]) -> None:
    state = json.loads(STATE.read_text())
    package = ROOT / ".cache/bronze-lambda.zip"
    body = package.read_bytes()
    key = f"deployment/{hashlib.sha256(body).hexdigest()}.zip"
    remote = S3ArtifactStore(clients["s3"], outputs["Bucket"])
    remote.put_immutable(key, iter([body]), media_type="application/zip", compression="zip")
    client = clients["lambda"]
    client.update_function_code(
        FunctionName=outputs["Function"], S3Bucket=outputs["Bucket"], S3Key=key
    )
    client.get_waiter("function_updated_v2").wait(
        FunctionName=outputs["Function"], WaiterConfig={"Delay": 2, "MaxAttempts": 25}
    )
    client.update_function_configuration(
        FunctionName=outputs["Function"],
        Handler="urbanlens.ingestion.cloud.handler",
        Environment={
            "Variables": {
                "BRONZE_BUCKET": outputs["Bucket"],
                "BOOTSTRAP_CATALOG": state["catalog_key"],
            }
        },
    )
    client.get_waiter("function_updated_v2").wait(
        FunctionName=outputs["Function"], WaiterConfig={"Delay": 2, "MaxAttempts": 25}
    )
    state["package_sha256"] = hashlib.sha256(body).hexdigest()
    write(STATE, state)
    print("Uploaded the pinned Lambda package. Schedules remain disabled.")


def configure_secret(clients: dict[str, Any], outputs: dict[str, str]) -> None:
    """Separate credential transfer from code deployment; requires owner authorization."""
    state = json.loads(STATE.read_text())
    if state["outputs"]["Bucket"] != outputs["Bucket"]:
        raise ValueError("Credential destination differs from the prepared stack")
    # Keep the key out of Lambda configuration, deployment ZIPs, logs and command arguments.
    key_value = os.environ.get("COMPANIES_HOUSE_API_KEY") or dotenv_values(ROOT / ".env").get(
        "COMPANIES_HOUSE_API_KEY"
    )
    if not key_value:
        raise ValueError("Companies House key is missing locally")
    clients["ssm"].put_parameter(
        Name="/urbanlens/companies-house-api-key",
        Type="SecureString",
        Value=key_value,
        Overwrite=True,
    )
    state["company_key_configured"] = True
    write(STATE, state)
    print("Configured the authorized private company key in AWS SSM.")


def verify(clients: dict[str, Any], outputs: dict[str, str]) -> None:
    state = json.loads(STATE.read_text())
    config = clients["lambda"].get_function_configuration(FunctionName=outputs["Function"])
    if base64.b64decode(config["CodeSha256"]).hex() != state["package_sha256"]:
        raise ValueError("Live Lambda code differs from the deployed package")
    remote = S3ArtifactStore(clients["s3"], outputs["Bucket"])
    relocated = load_catalog(remote, state["catalog_key"])
    registry = Registry(S3StateBackend(clients["s3"], outputs["Bucket"]))
    results = []
    for provider in ("fsa", "osm", "companies-house-profiles"):
        runs = []
        measurements = []
        for _ in range(2):
            started = time.monotonic()
            response = clients["lambda"].invoke(
                FunctionName=outputs["Function"],
                InvocationType="RequestResponse",
                LogType="Tail",
                Payload=json.dumps({"provider": provider}).encode(),
            )
            body = response["Payload"].read()
            if response.get("FunctionError"):
                write(DIRECTORY / "bronze-lambda-error.json", json.loads(body))
                raise ValueError(
                    "Lambda failed; inspect private data/ingestion/bronze-lambda-error.json"
                )
            runs.append(json.loads(body))
            logs = base64.b64decode(response.get("LogResult", "")).decode(errors="replace")
            numeric = {}
            for label in (
                "Duration",
                "Billed Duration",
                "Memory Size",
                "Max Memory Used",
                "Init Duration",
            ):
                match = re.search(
                    r"(?:^|\t)" + re.escape(label) + r": ([0-9.]+)", logs, re.MULTILINE
                )
                if match:
                    numeric[label] = float(match[1])
            measurements.append(
                {"wall_seconds": round(time.monotonic() - started, 3), "runtime_report": numeric}
            )
        if runs[0] != runs[1] or runs[0]["status"] != "accepted":
            raise ValueError("Cloud monthly rerun changed the accepted baseline or was held")
        response = clients["lambda"].invoke(
            FunctionName=outputs["Function"],
            InvocationType="RequestResponse",
            Payload=json.dumps({"provider": provider, "operation": "replay"}).encode(),
        )
        body = response["Payload"].read()
        if response.get("FunctionError"):
            write(DIRECTORY / "bronze-lambda-error.json", json.loads(body))
            raise ValueError("Cloud source replay failed; inspect private bronze-lambda-error.json")
        cloud_replay = json.loads(body)["replay"]
        if not cloud_replay["normalized_hashes_match"] or cloud_replay["new_observations"] != 0:
            raise ValueError("Cloud source replay did not preserve the baseline")
        results.append({**runs[0], "measurements": measurements, "cloud_replay": cloud_replay})
    for item in state["baseline_snapshots"]:
        snapshot = Snapshot.model_validate(item)
        if registry.current(snapshot.cycle) != snapshot:
            raise ValueError("Cloud rerun changed original baseline or freshness")
        for part in snapshot.partitions:
            for artifact in part.artifacts:
                for _ in relocated.read_verified(artifact):
                    pass
    flags = clients["s3"].get_public_access_block(Bucket=outputs["Bucket"])[
        "PublicAccessBlockConfiguration"
    ]
    if not all(flags.values()) or len(flags) != 4:
        raise ValueError("Bucket public-access block is incomplete")
    lifecycle = clients["s3"].get_bucket_lifecycle_configuration(Bucket=outputs["Bucket"])["Rules"]
    if not any(
        r.get("Status") == "Enabled"
        and r.get("Expiration", {}).get("Days") == 90
        and (r.get("Prefix") == "bronze/" or r.get("Filter", {}).get("Prefix") == "bronze/")
        for r in lifecycle
    ):
        raise ValueError("Raw retention policy missing")
    write(
        DIRECTORY / "bronze-cloud-check.json",
        {
            "bucket": outputs["Bucket"],
            "catalog_key": state["catalog_key"],
            "package_sha256": state["package_sha256"],
            "checked_at": datetime.now(UTC).isoformat(),
            "cloud_invocations": results,
            "identical_cycle_reruns": True,
            "all_five_snapshot_currents_unchanged": True,
            "raw_read_hashes_verified": True,
            "private_bucket": True,
            "raw_retention_days": 90,
            "product_publication": False,
        },
    )
    print(
        "Live cloud reruns and S3 reads verified; "
        "original source dates/current snapshots unchanged."
    )


def security(clients: dict[str, Any], outputs: dict[str, str]) -> None:
    """Check the real bucket configuration and test guards using an invented object only."""
    client, bucket = clients["s3"], outputs["Bucket"]
    flags = client.get_public_access_block(Bucket=bucket)["PublicAccessBlockConfiguration"]
    public = client.get_bucket_policy_status(Bucket=bucket)["PolicyStatus"]["IsPublic"]
    encryption = client.get_bucket_encryption(Bucket=bucket)["ServerSideEncryptionConfiguration"][
        "Rules"
    ]
    ownership = client.get_bucket_ownership_controls(Bucket=bucket)["OwnershipControls"]["Rules"]
    if len(flags) != 4 or not all(flags.values()) or public:
        raise ValueError("Bucket is not fully private")
    if not any(
        r["ApplyServerSideEncryptionByDefault"]["SSEAlgorithm"] == "AES256" for r in encryption
    ):
        raise ValueError("Expected default S3 encryption")
    if ownership != [{"ObjectOwnership": "BucketOwnerEnforced"}]:
        raise ValueError("Bucket ownership/ACL policy changed")
    statements = json.loads(client.get_bucket_policy(Bucket=bucket)["Policy"])["Statement"]
    tls = any(
        r["Effect"] == "Deny"
        and str(r.get("Condition", {}).get("Bool", {}).get("aws:SecureTransport")).lower()
        == "false"
        for r in statements
    )
    if not tls:
        raise ValueError("TLS enforcement is missing")
    store = S3ArtifactStore(client, bucket)
    key = f"evidence/step18-security/{uuid4()}.json"
    original = store.put_immutable(
        key, iter([b'{"synthetic":true}']), media_type="application/json", compression="none"
    )
    assert (
        store.put_immutable(
            key, iter([b'{"synthetic":true}']), media_type="application/json", compression="none"
        )
        == original
    )
    try:
        store.put_immutable(
            key,
            iter([b'{"synthetic":"changed"}']),
            media_type="application/json",
            compression="none",
        )
    except Conflict:
        pass
    else:
        raise ValueError("A conflicting artifact overwrite succeeded")
    try:
        client.put_object(Bucket=bucket, Key=key, Body=b"synthetic-unconditional-overwrite")
    except ClientError as error:
        if error.response["Error"]["Code"] != "AccessDenied":
            raise
    else:
        raise ValueError("Bucket policy did not deny unconditional writes")
    for _ in store.read_verified(original):
        pass
    write(
        DIRECTORY / "bronze-security.json",
        {
            "bucket": bucket,
            "checked_at": datetime.now(UTC).isoformat(),
            "all_public_access_blocks": True,
            "policy_is_public": False,
            "encryption": "AES256",
            "ownership": "BucketOwnerEnforced",
            "tls_enforced": True,
            "identical_write_idempotent": True,
            "conflicting_write_blocked": True,
            "unconditional_write_denied_by_aws": True,
            "synthetic_object_only": True,
        },
    )
    print("Live privacy/encryption/ownership and immutable-write guards passed.")


def replay(clients: dict[str, Any], outputs: dict[str, str]) -> None:
    state = json.loads(STATE.read_text())
    store = load_catalog(S3ArtifactStore(clients["s3"], outputs["Bucket"]), state["catalog_key"])
    registry = Registry(S3StateBackend(clients["s3"], outputs["Bucket"]))
    results = []
    postcode_artifact = Artifact.model_validate(state["postcode_evidence"])
    postcode_export = json.loads(b"".join(store.read_verified(postcode_artifact)))
    verified_postcode_artifacts: set[tuple[str, str, int]] = set()
    for item in postcode_export["entries"]:
        entry = CacheEntry.model_validate(item)
        validate_result(entry.result, entry.postcode)
        identity = (entry.artifact.uri, entry.artifact.sha256, entry.artifact.byte_count)
        if identity not in verified_postcode_artifacts:
            for _ in store.read_verified(entry.artifact):
                pass
            verified_postcode_artifacts.add(identity)
    for item in state["baseline_snapshots"]:
        snapshot = Snapshot.model_validate(item)
        name = snapshot.cycle.provider.value
        if name == "fsa":
            fsa = FsaConfig.model_validate_json((ROOT / "config/fsa-source.json").read_text())
            result = replay_snapshot(snapshot, FsaAdapter(fsa), fsa.rules(), store)
        elif name == "osm":
            with payload(store, snapshot.partitions[0].artifacts[1]) as stream:
                osm = OsmConfig.model_validate(json.load(stream)["config"])
            result = replay_snapshot(snapshot, OsmAdapter(osm), osm.rules(), store)
        elif name == "ons":
            reference = ReferenceConfig.model_validate_json(
                (ROOT / "config/reference-source.json").read_text()
            )
            result = replay_snapshot(
                snapshot, ReferenceAdapter(reference), reference.rules(), store
            )
        elif name == "companies-house":
            companies = CompanyConfig.model_validate_json(
                (ROOT / "config/companies-source.json").read_text()
            )
            if snapshot.cycle.scope.endswith("-profiles"):
                result = replay_snapshot(
                    snapshot, ProfileAdapter(companies), companies.rules(profiles=True), store
                )
            else:
                with payload(store, snapshot.partitions[0].artifacts[1]) as stream:
                    listing = stream.read()
                postcodes = PostcodeService(
                    DIRECTORY / "replay-unused-postcodes.sqlite", store, set()
                )
                bulk = CompanyAdapter(companies, listing, postcodes, set())
                result = replay_snapshot(snapshot, bulk, companies.rules(), store)
        else:
            raise ValueError("Unexpected baseline provider")
        if registry.current(snapshot.cycle) != snapshot:
            raise ValueError("Replay changed current snapshot")
        results.append({"provider": name, "scope": snapshot.cycle.scope, **result})
        print(f"Replayed {name}/{snapshot.cycle.scope}: {result['records']:,} records.")
    write(
        DIRECTORY / "bronze-replay.json",
        {
            "bucket": outputs["Bucket"],
            "catalog_key": state["catalog_key"],
            "results": results,
            "postcode_cache_entries_verified": len(postcode_export["entries"]),
            "all_currents_unchanged": True,
        },
    )


def recover(clients: dict[str, Any], outputs: dict[str, str]) -> None:
    store = S3ArtifactStore(clients["s3"], outputs["Bucket"])
    registry = Registry(S3StateBackend(clients["s3"], outputs["Bucket"]))
    report = recovery_drill(store, registry)
    state = json.loads(STATE.read_text())
    for item in state["baseline_snapshots"]:
        original = Snapshot.model_validate(item)
        if registry.current(original.cycle) != original:
            raise ValueError("Recovery drill altered a production source")
    write(
        DIRECTORY / "bronze-recovery.json",
        {"bucket": outputs["Bucket"], "catalog_key": state["catalog_key"], **report},
    )
    print("S3 partition/provider recovery drills passed in isolated synthetic scopes.")


def enable(clients: dict[str, Any], outputs: dict[str, str]) -> None:
    state = json.loads(STATE.read_text())
    for name in ("bronze-cloud-check.json", "bronze-replay.json", "bronze-recovery.json"):
        path = DIRECTORY / name
        if not path.is_file():
            raise ValueError("Complete cloud verification, replay and recovery first")
        report = json.loads(path.read_text())
        if report["bucket"] != outputs["Bucket"] or report["catalog_key"] != state["catalog_key"]:
            raise ValueError("Verification belongs to different Bronze storage")
    secure = DIRECTORY / "bronze-security.json"
    if not secure.is_file() or json.loads(secure.read_text())["bucket"] != outputs["Bucket"]:
        raise ValueError("Complete live security verification first")
    checked = json.loads((DIRECTORY / "bronze-cloud-check.json").read_text())
    if checked["package_sha256"] != state["package_sha256"]:
        raise ValueError("Verify the current package before enabling schedules")
    if (datetime.now(UTC) - datetime.fromisoformat(checked["checked_at"])).total_seconds() > 86400:
        raise ValueError("Repeat live cloud verification before enabling schedules")
    schedules = ["FsaSchedule", "OsmSchedule"]
    if state.get("company_key_configured"):
        schedules.append("ProfilesSchedule")
    for output in schedules:
        clients["events"].enable_rule(Name=outputs[output])
    print(f"Enabled {len(schedules)} monthly collection rules (seventh, UTC).")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "task",
        choices=[
            "prepare",
            "bootstrap",
            "deploy",
            "configure-secret",
            "verify",
            "security",
            "replay",
            "recover",
            "enable",
        ],
    )
    args = parser.parse_args()
    if args.task == "prepare":
        prepare()
        return
    service_clients = clients()
    outputs = inventory(service_clients)
    {
        "bootstrap": bootstrap,
        "deploy": deploy,
        "configure-secret": configure_secret,
        "verify": verify,
        "security": security,
        "replay": replay,
        "recover": recover,
        "enable": enable,
    }[args.task](service_clients, outputs)


if __name__ == "__main__":
    try:
        main()
    except (ValueError, RuntimeError, subprocess.CalledProcessError) as error:
        raise SystemExit(str(error)) from None

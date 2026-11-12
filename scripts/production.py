"""Explicit production targets: prepare inputs, configure secrets, deploy and operate."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import subprocess
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from urllib.parse import urlencode

import boto3
from botocore.config import Config

from scripts.database import administrator, credential
from scripts.database import values as database_values
from scripts.dev import ROOT
from urbanlens.ingestion.registry import S3StateBackend
from urbanlens.ingestion.storage import S3ArtifactStore
from urbanlens.operations.assets import ASSETS
from urbanlens.operations.control import Coordinator


def client(name: str) -> Any:
    ca = ROOT / ".cache/aws-system-ca.pem"
    if ca.is_file():
        os.environ.setdefault("AWS_CA_BUNDLE", str(ca))
    session = boto3.Session(
        profile_name=None if os.environ.get("CI") else "urbanlens_dev", region_name="eu-west-2"
    )
    return session.client(
        name, config=Config(connect_timeout=5, read_timeout=60, retries={"total_max_attempts": 2})
    )


def outputs(stack: str) -> dict[str, str]:
    body = client("cloudformation").describe_stacks(StackName=stack)["Stacks"][0]
    if body["StackStatus"] not in {"CREATE_COMPLETE", "UPDATE_COMPLETE"}:
        raise ValueError("Stack is not ready")
    return {item["OutputKey"]: item["OutputValue"] for item in body.get("Outputs", [])}


def prepare() -> dict[str, Any]:
    from scripts.company_pilot import local_directory

    state = json.loads((ROOT / ".cache/bronze-state.json").read_bytes())
    store = S3ArtifactStore(client("s3"), state["outputs"]["Bucket"])
    assets = {}
    for name in sorted(ASSETS):
        body = (ROOT / name).read_bytes()
        artifact = store.put_immutable(
            f"deployment/assets/{hashlib.sha256(body).hexdigest()}/{Path(name).name}",
            iter([body]),
            media_type="application/json",
            compression="none",
        )
        assets[name] = artifact.model_dump(mode="json")
    definition = {
        "version": "production-runtime-v1",
        "bronze": state,
        "assets": assets,
        "postal_prefilter": sorted({key[:-3] for key in local_directory()}),
    }
    body = json.dumps(definition, sort_keys=True).encode()
    key = f"deployment/config/{hashlib.sha256(body).hexdigest()}.json"
    artifact = store.put_immutable(
        key, iter([body]), media_type="application/json", compression="none"
    )
    result = {
        "artifacts_bucket": store.bucket,
        "runtime_config_key": key,
        "artifact": artifact.model_dump(mode="json"),
        "provider_requests": 0,
        "secrets_uploaded": False,
    }
    path = ROOT / "data/operations/prepared.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(result, indent=2) + "\n")
    return result


def invoke(payload: dict[str, Any]) -> dict[str, Any]:
    result = client("lambda").invoke(
        FunctionName="urbanlens-production-control",
        InvocationType="RequestResponse",
        Payload=json.dumps(payload).encode(),
    )
    body = json.loads(result["Payload"].read())
    if result.get("FunctionError"):
        raise ValueError("Private control operation failed; inspect its scoped CloudWatch log")
    return dict(body)


def private_api_smoke() -> dict[str, Any]:
    """Exercise the deployed reader through IAM, without creating a public endpoint."""

    def get(path: str, parameters: dict[str, Any] | None = None) -> dict[str, Any]:
        event = {
            "version": "2.0",
            "routeKey": "GET /{proxy+}",
            "rawPath": path,
            "rawQueryString": urlencode(parameters or {}),
            "headers": {"host": "private.urbanlens"},
            "requestContext": {
                "http": {
                    "method": "GET",
                    "path": path,
                    "sourceIp": "127.0.0.1",
                    "protocol": "HTTP/1.1",
                },
                "stage": "$default",
                "requestId": "private-deployment-smoke",
                "domainName": "private.urbanlens",
            },
            "isBase64Encoded": False,
        }
        response = client("lambda").invoke(
            FunctionName="urbanlens-production-api",
            InvocationType="RequestResponse",
            Payload=json.dumps(event).encode(),
        )
        body = json.loads(response["Payload"].read())
        if response.get("FunctionError") or body.get("statusCode") != 200:
            raise ValueError("Deployed private API smoke failed; inspect scoped logs")
        return dict(json.loads(body["body"]))

    get("/health")
    metadata = get("/api/v1/meta")
    release = metadata["release_id"]
    venues = get("/api/v1/establishments", {"release_id": release, "limit": 1, "map_limit": 1})
    if venues["release_id"] != release:
        raise ValueError("API did not retain the selected release")
    return {
        "private_lambda_smoke_passed": True,
        "release_id": release,
        "matching_venues": venues["data"]["total"],
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "task",
        choices=[
            "prepare",
            "configure-secrets",
            "deploy",
            "status",
            "start",
            "retry",
            "recover",
            "cutoff",
            "resume",
            "smoke",
            "wait",
        ],
    )
    parser.add_argument("--job", choices=["refresh", "backup", "migrate"], default="refresh")
    parser.add_argument("--api-image")
    parser.add_argument("--pipeline-image")
    parser.add_argument("--runtime-config-key")
    parser.add_argument("--bucket")
    parser.add_argument("--alert-email", default=database_values().get("URBANLENS_ALERT_EMAIL"))
    parser.add_argument("--confirm-headroom", action="store_true")
    parser.add_argument("--confirm-recovery", action="store_true")
    parser.add_argument("--reason")
    parser.add_argument("--url")
    parser.add_argument("--private", action="store_true", help="Smoke-test API through IAM")
    parser.add_argument("--build-id")
    parser.add_argument("--wait", action="store_true")
    parser.add_argument("--pipeline-only", action="store_true")
    args = parser.parse_args()
    if args.task == "prepare":
        result = prepare()
    elif args.task == "configure-secrets":
        # This is an explicit command, never part of CI or a public request.
        for purpose in ("api", "pipeline", "dbt", "migration", "admin"):
            value = administrator("neon") if purpose == "admin" else credential("neon", purpose)
            client("ssm").put_parameter(
                Name=f"/urbanlens/production/database/{purpose}",
                Type="SecureString",
                Value=value,
                Overwrite=True,
            )
        result = {"scoped_database_parameters_configured": 5, "values_logged": False}
    elif args.task == "deploy":
        foundation = outputs("urbanlens-production-access")
        cf = client("cloudformation")
        existing_stack = None
        try:
            existing_stack = cf.describe_stacks(StackName="urbanlens-production")["Stacks"][0]
        except cf.exceptions.ClientError as error:
            if "does not exist" not in str(error):
                raise
        exists = existing_stack is not None
        if existing_stack:
            if existing_stack["StackStatus"] not in {
                "CREATE_COMPLETE",
                "UPDATE_COMPLETE",
                "UPDATE_ROLLBACK_COMPLETE",
            }:
                raise ValueError("Runtime stack is not ready for a software update")
            if not args.alert_email:
                # Existing alert configuration stays in AWS; CI need not store the address.
                args.alert_email = next(
                    p["ParameterValue"]
                    for p in existing_stack["Parameters"]
                    if p["ParameterKey"] == "AlertEmail"
                )
        if not all(
            (
                args.api_image,
                args.pipeline_image,
                args.runtime_config_key,
                args.bucket,
                args.alert_email,
            )
        ):
            parser.error("deploy requires digest-pinned images, config key, bucket and alert email")
        commit = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=ROOT, text=True).strip()
        # Local rehearsals can include uncommitted work: identify their immutable image,
        # rather than attributing that code to the previous Git commit.
        software_version = (
            commit if os.environ.get("CI") else args.pipeline_image.rsplit("@sha256:", 1)[-1][:40]
        )
        parameters = {
            "ArtifactsBucket": args.bucket,
            "BoundaryArn": foundation["BoundaryArn"],
            "ApiImage": args.api_image,
            "PipelineImage": args.pipeline_image,
            "RuntimeConfigKey": args.runtime_config_key,
            "SoftwareVersion": software_version,
            "AlertEmail": args.alert_email,
        }
        changed = True
        if exists:
            # Software deployment preserves the owner's deliberate public/schedule state.
            values = [{"ParameterKey": k, "ParameterValue": v} for k, v in parameters.items()]
            values += [
                {"ParameterKey": k, "UsePreviousValue": True}
                for k in ("ApiEnabled", "SchedulesEnabled", "ReserveFunctionConcurrency")
            ]
            if args.pipeline_only:
                values = [
                    v
                    if v["ParameterKey"] != "ApiImage"
                    else {"ParameterKey": "ApiImage", "UsePreviousValue": True}
                    for v in values
                ]
            try:
                cf.update_stack(
                    StackName="urbanlens-production",
                    TemplateBody=(ROOT / "infra/production/template.yaml").read_text(),
                    Parameters=values,
                    Capabilities=["CAPABILITY_NAMED_IAM"],
                    RoleARN=foundation["CloudFormationRole"],
                )
            except cf.exceptions.ClientError as error:
                if "No updates are to be performed" not in str(error):
                    raise
                changed = False
        else:
            cf.create_stack(
                StackName="urbanlens-production",
                TemplateBody=(ROOT / "infra/production/template.yaml").read_text(),
                Parameters=[
                    {"ParameterKey": k, "ParameterValue": v} for k, v in parameters.items()
                ],
                Capabilities=["CAPABILITY_NAMED_IAM"],
                RoleARN=foundation["CloudFormationRole"],
                OnFailure="ROLLBACK",
            )
        if args.wait and changed:
            cf.get_waiter("stack_update_complete" if exists else "stack_create_complete").wait(
                StackName="urbanlens-production", WaiterConfig={"Delay": 10, "MaxAttempts": 90}
            )
        result = {
            "submitted": True,
            "stack": "urbanlens-production",
            "operation": "update" if exists else "create",
            "public_state_preserved": exists,
            "new_stack_public_access": False,
        }
    elif args.task == "recover":
        if not args.confirm_recovery or not args.reason or not args.bucket:
            parser.error("recover requires --confirm-recovery, --reason and --bucket")
        stack = client("cloudformation").describe_stacks(StackName="urbanlens-production")[
            "Stacks"
        ][0]
        if stack["StackStatus"] not in {"CREATE_COMPLETE", "UPDATE_COMPLETE"}:
            raise ValueError("Runtime stack must be ready")
        parameters = {p["ParameterKey"]: p["ParameterValue"] for p in stack["Parameters"]}
        if parameters["ArtifactsBucket"] != args.bucket:
            raise ValueError("Recovery bucket differs from the deployed runtime")
        engine = Coordinator(
            S3StateBackend(client("s3"), args.bucket, "operations"),
            client("codebuild"),
            "urbanlens-production-pipeline",
            parameters["SoftwareVersion"],
        )
        now = datetime.now(UTC)
        engine.reconcile(now)
        key = "runs/refresh/" + now.strftime("%Y-%m") + ".json"
        state, _ = engine.read(key)
        if state.get("status") not in {"FAILED", "FAULT", "STOPPED", "TIMED_OUT"}:
            raise ValueError("Recovery requires a terminal failed refresh")
        if int(state.get("attempt", 0)) < 2:
            raise ValueError("Use the normal retry allowance first")
        approval = {
            "run_key": key,
            "previous_state_sha256": hashlib.sha256(
                json.dumps(state, sort_keys=True).encode()
            ).hexdigest(),
            "software_version": parameters["SoftwareVersion"],
            "approved_attempt": int(state["attempt"]) + 1,
            "approved_at": now.isoformat(),
            "reason": args.reason,
            "timeout_minutes": 30,
        }
        identifier = hashlib.sha256(json.dumps(approval, sort_keys=True).encode()).hexdigest()
        engine.write("recovery-approvals/" + identifier + ".json", None, approval)
        result = engine.start("refresh", now, retry=True, recovery_approval=identifier)
    elif args.task in {"start", "retry"}:
        result = invoke({"action": args.task, "kind": args.job})
    elif args.task == "cutoff":
        result = invoke({"action": "cutoff"})
    elif args.task == "resume":
        if not args.confirm_headroom:
            parser.error(
                "Check AWS spend/forecast and Neon allowances, then pass --confirm-headroom"
            )
        outputs("urbanlens-production")
        # Public access and schedules require a separate reviewed stack change.
        bucket = args.bucket
        if not bucket:
            parser.error("resume requires the explicit --bucket target")
        stack = client("cloudformation").describe_stacks(StackName="urbanlens-production")[
            "Stacks"
        ][0]
        parameters = {p["ParameterKey"]: p["ParameterValue"] for p in stack["Parameters"]}
        if parameters.get("ReserveFunctionConcurrency") == "true":
            client("lambda").put_function_concurrency(
                FunctionName="urbanlens-production-collect", ReservedConcurrentExecutions=1
            )
        else:
            client("lambda").delete_function_concurrency(
                FunctionName="urbanlens-production-collect"
            )
        if parameters.get("ApiEnabled") == "false":
            # Permit IAM-only reader checks when no public gateway exists.
            client("lambda").delete_function_concurrency(FunctionName="urbanlens-production-api")
        backend = S3StateBackend(client("s3"), bucket, "operations")
        current = backend.load("switches.json")
        backend.compare_set(
            "switches.json",
            current[1] if current else None,
            json.dumps({"paused": False, "headroom_confirmed": True}).encode(),
        )
        result = {"private_processing_resumed": True, "public_api_or_schedules_enabled": False}
    elif args.task == "wait":
        if not args.build_id:
            parser.error("wait requires --build-id")
        for _ in range(210):
            build = client("codebuild").batch_get_builds(ids=[args.build_id])["builds"][0]
            if build["projectName"] != "urbanlens-production-pipeline":
                raise ValueError("Unexpected build project")
            if build["buildStatus"] in {"SUCCEEDED", "FAILED", "FAULT", "STOPPED", "TIMED_OUT"}:
                if build["buildStatus"] != "SUCCEEDED":
                    raise ValueError("Production job did not succeed")
                invoke({"source": "aws.codebuild", "detail": {"build-id": build["id"]}})
                result = {"status": "SUCCEEDED"}
                break
            time.sleep(10)
        else:
            raise ValueError("Bounded build wait expired")
    elif args.task == "smoke" and args.private:
        result = private_api_smoke()
    elif args.task == "smoke":
        import httpx

        if not args.url or not args.url.startswith("https://"):
            parser.error("smoke requires an explicit HTTPS --url")
        with httpx.Client(timeout=15, follow_redirects=False) as http:
            health = http.get(args.url.rstrip("/") + "/health")
            health.raise_for_status()
            metadata = http.get(args.url.rstrip("/") + "/api/v1/meta")
            metadata.raise_for_status()
            release = metadata.json()["release_id"]
            venues = http.get(
                args.url.rstrip("/") + "/api/v1/establishments",
                params={"release_id": release, "limit": 1, "map_limit": 1},
            )
            venues.raise_for_status()
            result = {
                "https_smoke_passed": True,
                "release_id": release,
                "matching_venues": venues.json()["data"]["total"],
            }
    else:
        result = invoke({"action": "health"})
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    try:
        main()
    except Exception as error:
        raise SystemExit(
            f"Production operation failed ({type(error).__name__}); no credentials printed"
        ) from None

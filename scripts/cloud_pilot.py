"""Bounded, private step 08 AWS pilot. No public endpoints or schedules."""

from __future__ import annotations

import argparse
import base64
import csv
import gzip
import hashlib
import json
import os
import re
import secrets
import subprocess
import time
from pathlib import Path
from typing import Any

from scripts.dev import tool
from scripts.runtime_pilot import ROOT, database_dsn
from urbanlens.feasibility import file_sha, reconcile_partitions, require

OUT = ROOT / "data/pilots/runtime/2026-11-02"
STATE = ROOT / ".cache/cloud-pilot-state.json"
STACK = "urbanlens-step08-20261102"
PROFILE = "urbanlens_dev"


def write(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, indent=2) + "\n", encoding="utf-8")


def read(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def aws(*arguments: str, raw: bool = False) -> Any:
    environment = os.environ.copy()
    environment.update(AWS_MAX_ATTEMPTS="1", AWS_PAGER="", AWS_CLI_AUTO_PROMPT="off")
    ca = ROOT / ".cache/aws-system-ca.pem"
    if ca.exists():
        environment.setdefault("AWS_CA_BUNDLE", str(ca))
    result = subprocess.run(
        [tool("aws"), *arguments, "--profile", PROFILE, "--region", "eu-west-2", "--no-cli-pager"],
        capture_output=True,
        env=environment,
    )
    if result.returncode:
        (ROOT / ".cache/cloud-pilot-error.txt").write_bytes(result.stderr)
        raise RuntimeError(
            f"AWS {arguments[0]} {arguments[1]} failed; see private .cache/cloud-pilot-error.txt"
        )
    return result.stdout.decode().strip() if raw else json.loads(result.stdout or b"{}")


def identity() -> dict[str, Any]:
    result: dict[str, Any] = aws("sts", "get-caller-identity")
    require(not result["Arn"].endswith(":root"), "Use the non-root development identity")
    return result


def preflight() -> None:
    result = identity()
    aws(
        "iam",
        "get-policy",
        "--policy-arn",
        f"arn:aws:iam::{result['Account']}:policy/UrbanLensStep08RuntimeBoundary",
    )
    scanning = aws("ecr", "get-registry-scanning-configuration")["scanningConfiguration"]
    require(
        scanning.get("scanType") != "ENHANCED",
        "Enhanced registry scanning needs a separate pilot-cost review before image upload",
    )
    settings = aws("lambda", "get-account-settings")
    validated = aws(
        "cloudformation",
        "validate-template",
        "--template-body",
        "file://" + str(ROOT / "infra/pilot/template.yaml"),
    )
    write(
        OUT / "aws-preflight.json",
        {
            "identity": "non-root identity verified; identifiers omitted",
            "region": "eu-west-2",
            "concurrency": settings["AccountLimit"]["ConcurrentExecutions"],
            "template_validated": bool(validated),
            "deployment_performed": False,
            "account_fingerprint": hashlib.sha256(result["Account"].encode()).hexdigest(),
        },
    )
    print("Non-root identity, London Lambda settings and pilot template verified", flush=True)


def docker(*arguments: str, input_bytes: bytes | None = None) -> None:
    executable = tool("docker")
    environment = os.environ.copy()
    environment["PATH"] = str(Path(executable).parent) + os.pathsep + environment.get("PATH", "")
    environment["DOCKER_CONFIG"] = str(ROOT / ".cache/docker-ecr-pilot")
    result = subprocess.run(
        [executable, *arguments], input=input_bytes, capture_output=True, env=environment
    )
    if result.returncode:
        (ROOT / ".cache/cloud-docker-error.txt").write_bytes(result.stderr)
        raise RuntimeError(
            "Docker registry operation failed; see private .cache/cloud-docker-error.txt"
        )


def deploy() -> None:
    require(
        not STATE.exists(), "A pilot state already exists; inspect/clean it before a new deployment"
    )
    budget = read(ROOT / ".cache/aws-pilot-budget.json")
    require(
        budget["confirmed_on"] == "2026-11-02" and budget["pilot_reserve_gbp"] == 2,
        "Confirm current-date billing and the £2 pilot reserve first",
    )
    require(budget["owner_month_charge_gbp"] + 2 <= 10, "Pilot exceeds internal budget headroom")
    owner = identity()
    state: dict[str, Any] = {
        "account": owner["Account"],
        "profile": PROFILE,
        "region": "eu-west-2",
        "token": secrets.token_hex(8),
        "stack": STACK,
        "repositories": [],
    }
    # Refuse to adopt a similarly named pre-existing stack/repository.
    stacks = aws(
        "cloudformation",
        "list-stacks",
        "--stack-status-filter",
        "CREATE_COMPLETE",
        "CREATE_IN_PROGRESS",
        "UPDATE_COMPLETE",
        "ROLLBACK_COMPLETE",
    )
    require(
        not any(s["StackName"] == STACK for s in stacks.get("StackSummaries", [])),
        "Pilot stack name already exists",
    )
    repos = aws("ecr", "describe-repositories").get("repositories", [])
    require(
        not any(r["repositoryName"] in {STACK + "-api", STACK + "-pipeline"} for r in repos),
        "Pilot repository name already exists",
    )
    write(STATE, state)
    images = []
    for target in ("api", "pipeline"):
        name = STACK + "-" + target
        repo = aws(
            "ecr",
            "create-repository",
            "--repository-name",
            name,
            "--image-tag-mutability",
            "IMMUTABLE",
            "--tags",
            "Key=UrbanLensPilot,Value=" + state["token"],
        )["repository"]
        state["repositories"].append(repo)
        write(STATE, state)
        policy = {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Principal": {"Service": "lambda.amazonaws.com"},
                    "Action": ["ecr:BatchGetImage", "ecr:GetDownloadUrlForLayer"],
                    "Condition": {
                        "ArnLike": {
                            "aws:SourceArn": (
                                f"arn:aws:lambda:eu-west-2:{state['account']}:function:{STACK}-*"
                            )
                        }
                    },
                }
            ],
        }
        policy_path = ROOT / ".cache/pilot-ecr-policy.json"
        write(policy_path, policy)
        aws(
            "ecr",
            "set-repository-policy",
            "--repository-name",
            name,
            "--policy-text",
            "file://" + str(policy_path),
        )
        registry = repo["repositoryUri"].split("/")[0]
        try:
            password = aws("ecr", "get-login-password", raw=True)
            docker(
                "login",
                "--username",
                "AWS",
                "--password-stdin",
                registry,
                input_bytes=password.encode(),
            )
            docker("tag", "urbanlens-" + target + ":step08", repo["repositoryUri"] + ":pilot")
            print("Uploading private", target, "image", flush=True)
            docker("push", repo["repositoryUri"] + ":pilot")
        finally:
            (ROOT / ".cache/docker-ecr-pilot/config.json").unlink(missing_ok=True)
        info = aws(
            "ecr", "describe-images", "--repository-name", name, "--image-ids", "imageTag=pilot"
        )["imageDetails"][0]
        state["registry_bytes"] = state.get("registry_bytes", 0) + info["imageSizeInBytes"]
        write(STATE, state)
        require(state["registry_bytes"] <= 3_000_000_000, "Registry storage pilot budget exceeded")
        images.append(repo["repositoryUri"] + "@" + info["imageDigest"])
    parameters = ROOT / ".cache/pilot-private-parameters.json"
    write(
        parameters,
        [
            {
                "ParameterKey": "RuntimeBoundary",
                "ParameterValue": (
                    f"arn:aws:iam::{state['account']}:policy/UrbanLensStep08RuntimeBoundary"
                ),
            },
            {"ParameterKey": "ApiImage", "ParameterValue": images[0]},
            {"ParameterKey": "PipelineImage", "ParameterValue": images[1]},
            {"ParameterKey": "ProbeDatabase", "ParameterValue": database_dsn("neon")},
        ],
    )
    try:
        created = aws(
            "cloudformation",
            "create-stack",
            "--stack-name",
            STACK,
            "--template-body",
            "file://" + str(ROOT / "infra/pilot/template.yaml"),
            "--parameters",
            "file://" + str(parameters),
            "--capabilities",
            "CAPABILITY_IAM",
            "--tags",
            "Key=UrbanLensPilot,Value=" + state["token"],
        )
        state["stack_id"] = created["StackId"]
        write(STATE, state)
    finally:
        parameters.unlink(missing_ok=True)
    print("Creating private pilot functions and storage", flush=True)
    aws("cloudformation", "wait", "stack-create-complete", "--stack-name", state["stack_id"])
    details = aws("cloudformation", "describe-stacks", "--stack-name", state["stack_id"])["Stacks"][
        0
    ]
    state["outputs"] = {r["OutputKey"]: r["OutputValue"] for r in details["Outputs"]}
    write(STATE, state)
    print("Private pilot deployed; no schedules or public endpoints", flush=True)


def upload(path: Path, key: str, bucket: str) -> None:
    aws("s3api", "put-object", "--bucket", bucket, "--key", key, "--body", str(path))


def log_metrics(logs: str) -> dict[str, list[str]]:
    return {
        name: re.findall(pattern, logs, flags=re.MULTILINE)
        for name, pattern in {
            "duration_ms": r"(?:^|\t)Duration: ([0-9.]+) ms",
            "billed_ms": r"Billed Duration: ([0-9.]+) ms",
            "init_ms": r"Init Duration: ([0-9.]+) ms",
            "max_memory_mb": r"Max Memory Used: ([0-9]+) MB",
        }.items()
    }


def invoke(function: str, event: dict[str, Any], index: int) -> dict[str, Any]:
    state = read(STATE)
    require(state.get("invocations", 0) < 26, "Pilot invocation ceiling reached")
    state["invocations"] = state.get("invocations", 0) + 1
    write(STATE, state)  # A lost response still consumes the bounded invocation budget.
    payload = ROOT / ".cache/pilot-invoke.json"
    write(payload, event)
    response = OUT / f"aws-invoke-{index}.json"
    meta = aws(
        "lambda",
        "invoke",
        "--function-name",
        function,
        "--log-type",
        "Tail",
        "--cli-read-timeout",
        "660",
        "--cli-binary-format",
        "raw-in-base64-out",
        "--payload",
        "file://" + str(payload),
        str(response),
    )
    logs = base64.b64decode(meta.get("LogResult", "")).decode(errors="replace")
    report = log_metrics(logs)
    write(OUT / f"aws-report-{index}.json", report)
    require(
        "FunctionError" not in meta, "Pilot invocation failed; stop and inspect private response"
    )
    output: dict[str, Any] = read(response)
    return output


def run() -> None:
    state = read(STATE)
    require(identity()["Account"] == state["account"], "Pilot account mismatch")
    outputs = state["outputs"]
    bucket = outputs["Bucket"]
    source = ROOT / "data/pilots/companies/2026-11-02"
    for name in ("manifest.json", "bulk-inspection.json"):
        upload(source / name, "bootstrap/" + name, bucket)
    paths = [
        "pilots/venues/2026-11-02/fsa.xml",
        "pilots/venues/2026-11-02/osm.json",
        "reference/geography/areas-wgs84.geojson",
        "pilots/companies/2026-11-02/registered-office-cohort.csv",
    ]
    manifest = {}
    for path in paths:
        manifest[path] = file_sha(ROOT / "data" / path)
        upload(ROOT / "data" / path, "bootstrap/data/" + path, bucket)
    dataset_manifest = OUT / "aws-dataset-manifest.json"
    write(dataset_manifest, manifest)
    upload(dataset_manifest, "bootstrap/dataset-manifest.json", bucket)
    event = {
        "version": "2.0",
        "rawPath": "/health",
        "rawQueryString": "",
        "headers": {"host": "localhost"},
        "requestContext": {
            "http": {"method": "GET", "path": "/health", "sourceIp": "127.0.0.1"},
            "stage": "$default",
        },
        "isBase64Encoded": False,
    }
    for index in range(4):
        response = invoke(outputs["ApiFunction"], event, index)
        require(response.get("statusCode") == 200, "Managed API health check failed")
    records = []
    for part in range(1, 8):
        print("Managed Lambda company part", part, "of 7", flush=True)
        metrics = invoke(
            outputs["PipelineFunction"], {"task": "company-s3-part", "part": part}, 10 + part
        )
        require(metrics["end_to_end_seconds"] <= 240, "Partition exceeded pilot safety budget")
        records.append(metrics)
    write(
        OUT / "aws-collection.json",
        {"parts": records, "coordinator": reconcile_partitions(records)},
    )
    repeated = invoke(outputs["PipelineFunction"], {"task": "company-s3-part", "part": 7}, 18)
    require(
        repeated["artifact_sha256"] == records[-1]["artifact_sha256"],
        "Managed replay changed output",
    )
    db = invoke(outputs["PipelineFunction"], {"task": "database-neon"}, 19)
    write(OUT / "aws-database.json", db)
    from scripts.runtime_pilot import neon_read

    neon_read()  # dbt replaced its scratch model; restore/recheck the reader grant.
    print("Managed collection, replay and Neon load/dbt measurements complete", flush=True)


def verify() -> None:
    state = read(STATE)
    require(identity()["Account"] == state["account"], "Verification account mismatch")
    bucket = state["outputs"]["Bucket"]
    collection = read(OUT / "aws-collection.json")
    objects = aws("s3api", "list-objects-v2", "--bucket", bucket).get("Contents", [])
    raw = [item for item in objects if item["Key"].startswith("raw/")]
    expected = read(ROOT / "data/pilots/companies/2026-11-02/manifest.json")
    require(len(raw) == 7, "Incomplete cloud raw ZIP set")
    for item in raw:
        name = item["Key"].removeprefix("raw/")
        require(item["Size"] == expected[name]["bytes"], "Cloud raw object size mismatch")
    rows: list[dict[str, Any]] = []
    for record in collection["parts"]:
        path = OUT / f"aws-filtered-part-{record['part']}.jsonl.gz"
        aws("s3api", "get-object", "--bucket", bucket, "--key", record["output_key"], str(path))
        require(file_sha(path) == record["artifact_sha256"], "S3 filtered checksum mismatch")
        local = read(OUT / f"company-part-{record['part']}.json")
        require(
            record["artifact_sha256"] == local["artifact_sha256"],
            "Managed and local partition outputs differ",
        )
        rows.extend(json.loads(line) for line in gzip.decompress(path.read_bytes()).splitlines())
    with (ROOT / "data/pilots/companies/2026-11-02/postcode-candidates.csv").open(
        encoding="utf-8", newline=""
    ) as stream:
        original = list(csv.DictReader(stream))
    require(
        len(rows) == len({r["company_number"] for r in rows}) == len(original),
        "Cloud candidate identity/count mismatch",
    )
    require(
        {r["company_number"] for r in rows} == {r["company_number"] for r in original},
        "Cloud candidate identities differ from pilot",
    )
    flags = aws("s3api", "get-public-access-block", "--bucket", bucket)[
        "PublicAccessBlockConfiguration"
    ]
    require(len(flags) == 4 and all(flags.values()), "Bucket public access protection mismatch")
    write(
        OUT / "aws-artifact-verification.json",
        {
            "raw_zip_count": len(raw),
            "raw_bytes": sum(item["Size"] for item in raw),
            "filtered_hashes_match_local": True,
            "unique_company_candidates": len(rows),
            "candidate_identities_match_step07": True,
            "public_access_blocked": True,
            "stored_objects": len(objects),
            "stored_bytes": sum(item["Size"] for item in objects),
        },
    )
    print(
        "Private S3 artifacts verified: all seven raw parts and identical candidate outputs",
        flush=True,
    )


def dbt_update() -> None:
    """Extend only the owned pilot stack after administrator approval of CodeBuild access."""
    state = read(STATE)
    require(identity()["Account"] == state["account"], "Pilot update account mismatch")
    repository = next(r for r in state["repositories"] if r["repositoryName"].endswith("-pipeline"))
    if not state.get("dbt_image"):
        registry = repository["repositoryUri"].split("/")[0]
        try:
            password = aws("ecr", "get-login-password", raw=True)
            docker(
                "login",
                "--username",
                "AWS",
                "--password-stdin",
                registry,
                input_bytes=password.encode(),
            )
            uri = repository["repositoryUri"] + ":dbt-runtime"
            docker("tag", "urbanlens-pipeline:step08", uri)
            docker("push", uri)
        finally:
            (ROOT / ".cache/docker-ecr-pilot/config.json").unlink(missing_ok=True)
        details = aws(
            "ecr",
            "describe-images",
            "--repository-name",
            repository["repositoryName"],
            "--image-ids",
            "imageTag=dbt-runtime",
        )["imageDetails"][0]
        state["registry_bytes"] += details["imageSizeInBytes"]
        require(state["registry_bytes"] <= 3_000_000_000, "Registry pilot budget exceeded")
        state["dbt_image"] = repository["repositoryUri"] + "@" + details["imageDigest"]
        write(STATE, state)
    parameters = ROOT / ".cache/pilot-update-parameters.json"
    write(
        parameters,
        [
            {"ParameterKey": name, "UsePreviousValue": True}
            for name in ("ApiImage", "RuntimeBoundary", "ProbeDatabase")
        ]
        + [{"ParameterKey": "PipelineImage", "ParameterValue": state["dbt_image"]}],
    )
    try:
        aws(
            "cloudformation",
            "update-stack",
            "--stack-name",
            state["stack_id"],
            "--template-body",
            "file://" + str(ROOT / "infra/pilot/template.yaml"),
            "--parameters",
            "file://" + str(parameters),
            "--capabilities",
            "CAPABILITY_IAM",
        )
    finally:
        parameters.unlink(missing_ok=True)
    print("Updating the private pilot with an on-demand dbt job", flush=True)
    aws("cloudformation", "wait", "stack-update-complete", "--stack-name", state["stack_id"])
    details = aws("cloudformation", "describe-stacks", "--stack-name", state["stack_id"])["Stacks"][
        0
    ]
    state["outputs"] = {row["OutputKey"]: row["OutputValue"] for row in details["Outputs"]}
    write(STATE, state)


def build_observation(build: dict[str, Any]) -> dict[str, Any]:
    """CodeBuild API responses include plaintext env values: persist only phase metadata."""
    return {
        "status": build["buildStatus"],
        "phase": build.get("currentPhase"),
        "phases": build.get("phases", []),
        "logs": build.get("logs", {}),
    }


def dbt_build() -> None:
    state = read(STATE)
    require(identity()["Account"] == state["account"], "Build account mismatch")
    require(state.get("build_attempts", 0) < 2, "Pilot CodeBuild attempt ceiling reached")
    state["build_attempts"] = state.get("build_attempts", 0) + 1
    write(STATE, state)
    build = aws("codebuild", "start-build", "--project-name", state["outputs"]["DbtProject"])[
        "build"
    ]
    state.setdefault("build_ids", []).append(build["id"])
    write(STATE, state)
    started = time.monotonic()
    while True:
        build = aws("codebuild", "batch-get-builds", "--ids", build["id"])["builds"][0]
        write(OUT / "codebuild-run-private.json", build_observation(build))
        if build["buildStatus"] != "IN_PROGRESS":
            break
        if time.monotonic() - started > 600:
            aws("codebuild", "stop-build", "--id", build["id"])
            raise RuntimeError("Pilot build exceeded ten-minute outer budget; stop requested")
        time.sleep(10)
    require(
        build["buildStatus"] == "SUCCEEDED", "CodeBuild probe failed; inspect private build phases"
    )
    bucket = state["outputs"]["Bucket"]
    aws(
        "s3api",
        "get-object",
        "--bucket",
        bucket,
        "--key",
        "results/codebuild-dbt.json",
        str(OUT / "codebuild-database.json"),
    )
    aws(
        "s3api",
        "get-object",
        "--bucket",
        bucket,
        "--key",
        "results/codebuild-probe.dump",
        str(OUT / "codebuild-probe.dump"),
    )
    result = read(OUT / "codebuild-database.json")
    require(
        file_sha(OUT / "codebuild-probe.dump") == result["backup"]["sha256"],
        "Managed backup checksum mismatch",
    )
    from scripts.runtime_pilot import neon_read

    neon_read()
    write(
        OUT / "codebuild-summary.json",
        {
            "status": build["buildStatus"],
            "compute": build["environment"]["computeType"],
            "phases": [
                {
                    "phase": p["phaseType"],
                    "status": p.get("phaseStatus"),
                    "seconds": p.get("durationInSeconds"),
                }
                for p in build["phases"]
            ],
            "runtime": "on-demand Linux container; no reserved fleet or schedule",
        },
    )
    print(
        "Managed dbt and native Neon backup passed; downloaded backup checksum verified", flush=True
    )


def cleanup() -> None:
    state = read(STATE)
    if state.get("cleanup_complete"):
        print("This pilot was already cleaned up", flush=True)
        return
    require(identity()["Account"] == state["account"], "Cleanup account mismatch")
    if state.get("stack_id"):
        stack = aws("cloudformation", "describe-stacks", "--stack-name", state["stack_id"])[
            "Stacks"
        ][0]
        require(
            {t["Key"]: t["Value"] for t in stack.get("Tags", [])}.get("UrbanLensPilot")
            == state["token"],
            "Cleanup ownership tag mismatch",
        )
        outputs = {r["OutputKey"]: r["OutputValue"] for r in stack.get("Outputs", [])}
        if "Bucket" in outputs:
            objects = aws("s3api", "list-objects-v2", "--bucket", outputs["Bucket"])
            for item in objects.get("Contents", []):
                aws("s3api", "delete-object", "--bucket", outputs["Bucket"], "--key", item["Key"])
        aws("cloudformation", "delete-stack", "--stack-name", state["stack_id"])
        aws("cloudformation", "wait", "stack-delete-complete", "--stack-name", state["stack_id"])
    for repository in state["repositories"]:
        tags = aws("ecr", "list-tags-for-resource", "--resource-arn", repository["repositoryArn"])
        require(
            {t["Key"]: t["Value"] for t in tags["tags"]}.get("UrbanLensPilot") == state["token"],
            "Repository cleanup ownership mismatch",
        )
        aws(
            "ecr", "delete-repository", "--repository-name", repository["repositoryName"], "--force"
        )
    state["cleanup_complete"] = True
    write(STATE, state)
    write(
        OUT / "aws-cleanup.json",
        {
            "complete": True,
            "scope": "only tagged pilot stack/repositories",
            "residual_charges": "accrued usage and billing lag remain",
        },
    )
    print("Pilot stack, objects, logs and private registry images removed", flush=True)


def main() -> None:
    global PROFILE
    parser = argparse.ArgumentParser(description=__doc__)
    tasks = {
        "preflight": preflight,
        "deploy": deploy,
        "run": run,
        "verify": verify,
        "dbt-update": dbt_update,
        "dbt": dbt_build,
        "cleanup": cleanup,
    }
    parser.add_argument("task", choices=list(tasks))
    parser.add_argument("--profile", default=PROFILE)
    args = parser.parse_args()
    PROFILE = args.profile
    tasks[args.task]()


if __name__ == "__main__":
    main()

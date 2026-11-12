"""Private event handlers for workflow, monitoring and cost cutoff."""

from __future__ import annotations

import json
import os
from datetime import UTC, datetime
from typing import Any

from urbanlens.ingestion.cloud import sdk_client
from urbanlens.ingestion.registry import S3StateBackend
from urbanlens.operations.control import Coordinator


def cutoff(backend: S3StateBackend, *, reason: str) -> dict[str, Any]:
    coordinator = Coordinator(
        backend,
        sdk_client("codebuild"),
        os.environ["BUILD_PROJECT"],
        os.environ.get("SOFTWARE_VERSION", "v1"),
    )
    switches, version = coordinator.read("switches.json")
    # Persist the stop first: a racing scheduled trigger cannot start new paid work.
    coordinator.write("switches.json", version, {**switches, "paused": True, "reason": reason})
    failures = []
    actions: list[tuple[Any, str, dict[str, Any]]] = []
    events = sdk_client("events")
    for rule in json.loads(os.environ["PAID_RULES"]):
        actions.append((events, "disable_rule", {"Name": rule}))
    functions = sdk_client("lambda")
    for name in (os.environ["API_FUNCTION"], os.environ["COLLECTOR_FUNCTION"]):
        actions.append(
            (
                functions,
                "put_function_concurrency",
                {"FunctionName": name, "ReservedConcurrentExecutions": 0},
            )
        )
    builds = sdk_client("codebuild")
    try:
        for identifier in builds.list_builds_for_project(
            projectName=os.environ["BUILD_PROJECT"], sortOrder="DESCENDING"
        ).get("ids", [])[:20]:
            for build in builds.batch_get_builds(ids=[identifier])["builds"]:
                if build["buildStatus"] not in {
                    "SUCCEEDED",
                    "FAILED",
                    "STOPPED",
                    "FAULT",
                    "TIMED_OUT",
                }:
                    actions.append((builds, "stop_build", {"id": identifier}))
    except Exception:
        failures.append("discover_active_builds")
    for client, operation, arguments in actions:
        try:
            getattr(client, operation)(**arguments)
        except Exception:
            failures.append(operation)  # No provider exception text or credentials in logs.
    # Remove the paid public gateway through its owning stack, rather than leave a
    # throttled endpoint accumulating requests. CloudFormation finishes asynchronously.
    if os.environ.get("PRODUCTION_STACK"):
        try:
            cf = sdk_client("cloudformation")
            stack = cf.describe_stacks(StackName=os.environ["PRODUCTION_STACK"])["Stacks"][0]
            values = [
                {"ParameterKey": p["ParameterKey"], "ParameterValue": "false"}
                if p["ParameterKey"] in {"ApiEnabled", "SchedulesEnabled"}
                else {"ParameterKey": p["ParameterKey"], "UsePreviousValue": True}
                for p in stack["Parameters"]
            ]
            if any(
                p["ParameterKey"] in {"ApiEnabled", "SchedulesEnabled"}
                and p["ParameterValue"] == "true"
                for p in stack["Parameters"]
            ):
                cf.update_stack(
                    StackName=os.environ["PRODUCTION_STACK"],
                    UsePreviousTemplate=True,
                    Parameters=values,
                    Capabilities=["CAPABILITY_NAMED_IAM"],
                )
        except Exception:
            failures.append("suspend_public_gateway")
    result = {
        "status": "service_limited",
        "actions_attempted": len(actions),
        "failed_actions": failures,
    }
    if failures:
        raise RuntimeError("Cutoff incomplete: " + ",".join(failures))
    return result


def handler(event: dict[str, Any], context: Any) -> dict[str, Any]:
    backend = S3StateBackend(sdk_client("s3"), os.environ["BRONZE_BUCKET"], "operations")
    coordinator = Coordinator(
        backend,
        sdk_client("codebuild"),
        os.environ["BUILD_PROJECT"],
        os.environ.get("SOFTWARE_VERSION", "v1"),
    )
    now = datetime.now(UTC)
    if event.get("Records"):
        # Only the dedicated cutoff SNS topic has permission to invoke this handler.
        result = cutoff(backend, reason="conservative-USD15-budget-threshold")
    elif event.get("source") == "aws.codebuild":
        result = coordinator.finish(event["detail"]["build-id"], now)
    elif event.get("action") in {"start", "retry"}:
        result = coordinator.start(event["kind"], now, retry=event["action"] == "retry")
    elif event.get("action") == "cutoff":
        result = cutoff(backend, reason="owner-cutoff")
    elif event.get("action") == "health":
        switches, _ = coordinator.read("switches.json")
        if switches.get("paused") and switches.get("reason"):
            cutoff(backend, reason=switches["reason"])
        result = coordinator.health(now)
        if result["alerts"]:
            sdk_client("sns").publish(
                TopicArn=os.environ["ALERT_TOPIC"],
                Subject="UrbanLens operating warning",
                Message=json.dumps(result),
            )
    else:
        raise ValueError("Unsupported private control action")
    print(
        json.dumps(
            {
                "operation_status": result.get("status", "checked"),
                "alerts": result.get("alerts", []),
            }
        )
    )
    return result

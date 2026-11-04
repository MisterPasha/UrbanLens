"""External mutation boundaries and invocation-cost controls, without AWS access."""

import importlib
from pathlib import Path
from typing import Any
from unittest.mock import patch

import pytest

from scripts import cloud_pilot
from scripts.runtime_pilot import ROOT


def cfn_file(path: Path) -> dict[str, Any]:
    yaml: Any = importlib.import_module("yaml")
    loader: Any = type("CfnLoader", (yaml.SafeLoader,), {})
    loader.add_multi_constructor(
        "!", lambda loader, tag, node: {tag: loader.construct_scalar(node)}
    )
    value: dict[str, Any] = yaml.load(path.read_text(encoding="utf-8"), Loader=loader)
    return value


def test_runtime_roles_must_use_the_fixed_boundary() -> None:
    template = cfn_file(ROOT / "infra/pilot/template.yaml")
    for role in ("ApiRole", "PipelineRole", "DbtRole"):
        assert template["Resources"][role]["Properties"]["PermissionsBoundary"] == {
            "Ref": "RuntimeBoundary"
        }
    access = cfn_file(ROOT / "infra/pilot/access.yaml")
    statements = access["Resources"]["DeveloperPolicy"]["Properties"]["PolicyDocument"]["Statement"]
    create = next(s for s in statements if s["Sid"] == "CreateOnlyBoundedPilotRoles")
    assert create["Action"] == "iam:CreateRole"
    assert create["Condition"]["ArnEquals"]["iam:PermissionsBoundary"] == {"Ref": "RuntimeBoundary"}
    actions = [
        a
        for s in statements
        for a in (s["Action"] if isinstance(s["Action"], list) else [s["Action"]])
    ]
    assert "iam:DeleteRolePermissionsBoundary" not in actions
    assert "iam:CreatePolicyVersion" not in actions


def test_root_identity_is_rejected() -> None:
    with patch.object(cloud_pilot, "aws", return_value={"Arn": "arn:aws:iam::123:root"}):
        with pytest.raises(ValueError, match="non-root"):
            cloud_pilot.identity()


def test_codebuild_observation_excludes_environment_secrets() -> None:
    result = cloud_pilot.build_observation(
        {
            "buildStatus": "IN_PROGRESS",
            "currentPhase": "BUILD",
            "environment": {"environmentVariables": [{"name": "DATABASE_URL", "value": "secret"}]},
        }
    )
    assert "environment" not in result and "secret" not in str(result)


def test_build_attempt_ceiling_prevents_another_paid_build(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    state = tmp_path / "state.json"
    cloud_pilot.write(state, {"account": "example", "build_attempts": 2})
    monkeypatch.setattr(cloud_pilot, "STATE", state)
    with patch.object(cloud_pilot, "identity", return_value={"Account": "example"}):
        with patch.object(cloud_pilot, "aws") as request:
            with pytest.raises(ValueError, match="ceiling"):
                cloud_pilot.dbt_build()
    request.assert_not_called()


def test_lambda_report_separates_handler_billing_and_initialisation() -> None:
    result = cloud_pilot.log_metrics(
        "REPORT RequestId: example\tDuration: 42.50 ms\tBilled Duration: 143 ms\t"
        "Memory Size: 512 MB\tMax Memory Used: 80 MB\tInit Duration: 100.10 ms\n"
    )
    assert result == {
        "duration_ms": ["42.50"],
        "billed_ms": ["143"],
        "max_memory_mb": ["80"],
        "init_ms": ["100.10"],
    }


def test_invocation_limit_prevents_a_request(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    state = tmp_path / "state.json"
    cloud_pilot.write(state, {"invocations": 26})
    monkeypatch.setattr(cloud_pilot, "STATE", state)
    with patch.object(cloud_pilot, "aws") as request:
        with pytest.raises(ValueError, match="ceiling"):
            cloud_pilot.invoke("example", {}, 1)
    request.assert_not_called()


def test_lost_response_still_consumes_invocation_budget(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    state = tmp_path / "state.json"
    cloud_pilot.write(state, {"invocations": 25})
    monkeypatch.setattr(cloud_pilot, "STATE", state)
    monkeypatch.setattr(cloud_pilot, "ROOT", tmp_path)
    monkeypatch.setattr(cloud_pilot, "OUT", tmp_path)
    with patch.object(cloud_pilot, "aws", side_effect=RuntimeError("lost response")):
        with pytest.raises(RuntimeError):
            cloud_pilot.invoke("example", {}, 1)
    assert cloud_pilot.read(state)["invocations"] == 26


def test_cleanup_rejects_an_unowned_stack_before_deleting(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    state = tmp_path / "state.json"
    cloud_pilot.write(state, {"account": "example", "stack_id": "example", "token": "ours"})
    monkeypatch.setattr(cloud_pilot, "STATE", state)
    calls: list[tuple[Any, ...]] = []

    def fake_aws(*args: Any) -> dict[str, Any]:
        calls.append(args)
        return {"Stacks": [{"Tags": [{"Key": "UrbanLensPilot", "Value": "other"}]}]}

    with patch.object(cloud_pilot, "identity", return_value={"Account": "example"}):
        with patch.object(cloud_pilot, "aws", side_effect=fake_aws):
            with pytest.raises(ValueError, match="ownership"):
                cloud_pilot.cleanup()
    assert len(calls) == 1 and calls[0][:2] == ("cloudformation", "describe-stacks")

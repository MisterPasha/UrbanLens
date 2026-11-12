"""Protect paid-work boundaries: duplicate delivery, overlap, ambiguous starts and cutoff."""

import hashlib
import json
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest

from urbanlens.ingestion.registry import SQLiteStateBackend
from urbanlens.operations import aws
from urbanlens.operations.control import Coordinator


def test_monthly_postcodes_refresh_even_when_february_cache_has_not_expired(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    import scripts.production_worker as worker
    import scripts.source_pipeline as sources
    from urbanlens.ingestion.postcodes import CacheEntry
    from urbanlens.ingestion.storage import LocalArtifactStore

    now = datetime(2027, 3, 7, tzinfo=UTC)

    class Clock(datetime):
        @classmethod
        def now(cls, tz: Any = None) -> datetime:
            return now

    store = LocalArtifactStore(tmp_path / "objects")
    response = store.put_immutable(
        "response.json", iter([b"{}"]), media_type="application/json", compression="none"
    )
    entries = [
        CacheEntry(
            postcode=postcode,
            result=None,
            fetched_at=fetched,
            expires_at=datetime(2027, 3, 9, tzinfo=UTC),
            artifact=response,
        )
        for postcode, fetched in (
            ("BN11AA", datetime(2027, 2, 7, tzinfo=UTC)),
            ("BN11AB", datetime(2027, 3, 1, tzinfo=UTC)),
        )
    ]
    exported = store.put_immutable(
        "export.json",
        iter([json.dumps({"entries": [e.model_dump(mode="json") for e in entries]}).encode()]),
        media_type="application/json",
        compression="none",
    )
    monkeypatch.setattr(worker, "ROOT", tmp_path)
    monkeypatch.setattr(worker, "datetime", Clock)
    monkeypatch.setattr(sources, "area_codes", lambda: set())
    service = worker.postcodes(store, {"postcode_evidence": exported.model_dump(mode="json")})
    assert service.cached("BN11AA", now) is None
    assert service.cached("BN11AB", now) == entries[1]


NOW = datetime(2026, 11, 7, 8, tzinfo=UTC)


class Builds:
    def __init__(self) -> None:
        self.items: dict[str, dict[str, Any]] = {}
        self.fail_start = False

    def start_build(self, **values: Any) -> dict[str, Any]:
        if self.fail_start:
            raise TimeoutError("ambiguous provider response")
        identifier = f"build-{len(self.items) + 1}"
        item = {
            "id": identifier,
            "projectName": "project",
            "buildStatus": "IN_PROGRESS",
            "environment": {"environmentVariables": values["environmentVariablesOverride"]},
        }
        self.items[identifier] = item
        return {"build": item}

    def batch_get_builds(self, *, ids: list[str]) -> dict[str, Any]:
        return {"builds": [self.items[i.rsplit("/", 1)[-1]] for i in ids]}

    def list_builds_for_project(self, **values: Any) -> dict[str, Any]:
        return {"ids": list(self.items)}

    def stop_build(self, *, id: str) -> None:
        self.items[id]["buildStatus"] = "STOPPED"


def setup(tmp_path: Path) -> tuple[Coordinator, Builds]:
    builds = Builds()
    engine = Coordinator(SQLiteStateBackend(tmp_path / "state.sqlite"), builds, "project")
    engine.write("switches.json", None, {"paused": False})
    return engine, builds


def test_duplicate_schedule_overlap_and_duplicate_completion(tmp_path: Path) -> None:
    engine, builds = setup(tmp_path)
    started = engine.start("refresh", NOW)
    assert engine.start("refresh", NOW) == started
    assert engine.start("backup", NOW)["status"] == "overlap_prevented"
    assert len(builds.items) == 1
    builds.items[started["build_id"]]["buildStatus"] = "SUCCEEDED"
    event_arn = "arn:aws:codebuild:eu-west-2:123456789012:build/" + started["build_id"]
    assert engine.finish(event_arn, NOW)["status"] == "SUCCEEDED"
    assert engine.finish(started["build_id"], NOW)["status"] == "duplicate_completion_ignored"
    assert engine.start("refresh", NOW)["status"] == "SUCCEEDED"
    assert len(builds.items) == 1


def test_recovery_approval_is_bound_to_exact_failed_run_and_cannot_allow_fourth_attempt(
    tmp_path: Path,
) -> None:
    engine, builds = setup(tmp_path)
    for retry in (False, True):
        started = engine.start("refresh", NOW, retry=retry)
        builds.items[started["build_id"]]["buildStatus"] = "FAILED"
        engine.finish(started["build_id"], NOW)
    state, _ = engine.read("runs/refresh/2026-11.json")
    with pytest.raises(ValueError):
        engine.start("refresh", NOW, retry=True, recovery_approval="missing")
    engine.write(
        "recovery-approvals/owner.json",
        None,
        {
            "run_key": "runs/refresh/2026-11.json",
            "previous_state_sha256": hashlib.sha256(
                json.dumps(state, sort_keys=True).encode()
            ).hexdigest(),
            "software_version": engine.software_version,
            "approved_attempt": 3,
        },
    )
    recovery = engine.start("refresh", NOW, retry=True, recovery_approval="owner")
    assert recovery["attempt"] == 3
    assert engine.read("recovery-history/refresh/2026-11/attempt-2.json")[0] == state
    assert engine.start("refresh", NOW) == recovery
    builds.items[recovery["build_id"]]["buildStatus"] = "FAILED"
    engine.finish(recovery["build_id"], NOW)
    with pytest.raises(ValueError):
        engine.start("refresh", NOW, retry=True, recovery_approval="owner")


def test_failed_job_requires_explicit_bounded_retry(tmp_path: Path) -> None:
    engine, builds = setup(tmp_path)
    first = engine.start("refresh", NOW)
    builds.items[first["build_id"]]["buildStatus"] = "FAILED"
    engine.finish(first["build_id"], NOW)
    assert engine.start("refresh", NOW)["status"] == "FAILED"
    second = engine.start("refresh", NOW, retry=True)
    builds.items[second["build_id"]]["buildStatus"] = "FAILED"
    engine.finish(second["build_id"], NOW)
    with pytest.raises(ValueError, match="at most two attempts"):
        engine.start("refresh", NOW, retry=True)


def test_migrations_are_pinned_to_the_software_version(tmp_path: Path) -> None:
    engine, builds = setup(tmp_path)
    engine.software_version = "a" * 40
    first = engine.start("migrate", NOW)
    builds.items[first["build_id"]]["buildStatus"] = "SUCCEEDED"
    engine.finish(first["build_id"], NOW)
    assert engine.start("migrate", NOW) == engine.read("runs/migrate/" + "a" * 40 + ".json")[0]
    engine.software_version = "b" * 40
    assert engine.start("migrate", NOW)["build_id"] != first["build_id"]


def test_ambiguous_start_does_not_launch_duplicate_paid_work(tmp_path: Path) -> None:
    engine, builds = setup(tmp_path)
    builds.fail_start = True
    with pytest.raises(TimeoutError):
        engine.start("refresh", NOW)
    builds.fail_start = False
    assert engine.start("refresh", NOW)["status"] == "STARTING"
    assert engine.start("backup", NOW)["status"] == "overlap_prevented"
    assert not builds.items


def test_health_grace_and_running_job_deadline(tmp_path: Path) -> None:
    engine, builds = setup(tmp_path)
    started = engine.start("refresh", NOW)
    assert "monthly_refresh_overdue" not in engine.health(NOW)["alerts"]
    assert "workflow_deadline_exceeded" in engine.health(NOW + timedelta(hours=1))["alerts"]
    assert builds.items[started["build_id"]]["buildStatus"] == "STOPPED"
    assert "monthly_refresh_overdue" in engine.health(NOW + timedelta(days=3))["alerts"]
    engine.write("measurements.json", None, {"reference_update_pending": True})
    assert "reference_edition_requires_review" in engine.health(NOW)["alerts"]
    engine.write("last-refresh.json", None, {"period": "2026-11", "ended_at": NOW.isoformat()})
    assert "monthly_refresh_overdue" not in engine.health(NOW + timedelta(days=3))["alerts"]


def test_stale_completion_cannot_release_another_job(tmp_path: Path) -> None:
    engine, builds = setup(tmp_path)
    first = engine.start("refresh", NOW)
    builds.items[first["build_id"]]["buildStatus"] = "SUCCEEDED"
    builds.items[first["build_id"]]["endTime"] = NOW - timedelta(seconds=10)
    # A later trigger must reconcile terminal work even if its event was lost.
    second = engine.start("backup", NOW)
    assert (
        engine.read("last-refresh.json")[0]["ended_at"] == (NOW - timedelta(seconds=10)).isoformat()
    )
    assert engine.finish(first["build_id"], NOW)["status"] == "unrelated_completion_ignored"
    assert engine.read("active.json")[0]["token"] == second["token"]


def test_cutoff_continues_stopping_requests_when_build_discovery_fails(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    engine, _ = setup(tmp_path)
    calls = []

    class Client:
        def disable_rule(self, **values: Any) -> None:
            calls.append(("disable", values))

        def put_function_concurrency(self, **values: Any) -> None:
            calls.append(("concurrency", values))

        def list_builds_for_project(self, **values: Any) -> Any:
            raise PermissionError("denied")

    monkeypatch.setattr(aws, "sdk_client", lambda _: Client())
    for key, value in {
        "BUILD_PROJECT": "project",
        "PAID_RULES": json.dumps(["monthly", "daily"]),
        "API_FUNCTION": "api",
        "COLLECTOR_FUNCTION": "collector",
    }.items():
        monkeypatch.setenv(key, value)
    with pytest.raises(RuntimeError, match="discover_active_builds"):
        aws.cutoff(engine.backend, reason="test")  # type: ignore[arg-type]
    assert engine.read("switches.json")[0]["paused"] is True
    assert len(calls) == 4
    assert all(
        values["ReservedConcurrentExecutions"] == 0 for op, values in calls if op == "concurrency"
    )
    assert engine.start("refresh", NOW)["status"] == "service_limited"


def test_cutoff_removes_the_paid_gateway_through_its_stack(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    engine, _ = setup(tmp_path)
    updates = []

    class Client:
        def disable_rule(self, **values: Any) -> None:
            pass

        def put_function_concurrency(self, **values: Any) -> None:
            assert values["ReservedConcurrentExecutions"] == 0

        def list_builds_for_project(self, **values: Any) -> dict[str, Any]:
            return {"ids": []}

        def describe_stacks(self, **values: Any) -> dict[str, Any]:
            return {
                "Stacks": [
                    {
                        "Parameters": [
                            {"ParameterKey": "ApiEnabled", "ParameterValue": "true"},
                            {"ParameterKey": "SchedulesEnabled", "ParameterValue": "true"},
                            {"ParameterKey": "ApiImage", "ParameterValue": "keep-digest"},
                        ]
                    }
                ]
            }

        def update_stack(self, **values: Any) -> None:
            updates.append(values)

    monkeypatch.setattr(aws, "sdk_client", lambda _: Client())
    for key, value in {
        "BUILD_PROJECT": "project",
        "PAID_RULES": "[]",
        "API_FUNCTION": "api",
        "COLLECTOR_FUNCTION": "collector",
        "PRODUCTION_STACK": "urbanlens-production",
    }.items():
        monkeypatch.setenv(key, value)
    assert aws.cutoff(engine.backend, reason="test")["status"] == "service_limited"  # type: ignore[arg-type]
    assert updates[0]["UsePreviousTemplate"] is True
    assert updates[0]["Parameters"] == [
        {"ParameterKey": "ApiEnabled", "ParameterValue": "false"},
        {"ParameterKey": "SchedulesEnabled", "ParameterValue": "false"},
        {"ParameterKey": "ApiImage", "UsePreviousValue": True},
    ]

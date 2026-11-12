"""A durable S3 coordinator. CodeBuild events finish jobs; there is no polling loop."""

from __future__ import annotations

import hashlib
import json
from datetime import datetime, timedelta
from typing import Any

from urbanlens.ingestion.registry import StateBackend

TERMINAL = {"SUCCEEDED", "FAILED", "FAULT", "STOPPED", "TIMED_OUT"}


class Coordinator:
    def __init__(
        self, backend: StateBackend, builds: Any, project: str, software_version: str = "v1"
    ) -> None:
        self.backend, self.builds, self.project = backend, builds, project
        self.software_version = software_version

    def read(self, key: str) -> tuple[dict[str, Any], str | None]:
        item = self.backend.load(key)
        return (json.loads(item[0]), item[1]) if item else ({}, None)

    def write(self, key: str, version: str | None, body: dict[str, Any]) -> None:
        self.backend.compare_set(key, version, json.dumps(body, sort_keys=True).encode())

    def start(
        self,
        kind: str,
        now: datetime,
        *,
        retry: bool = False,
        recovery_approval: str | None = None,
    ) -> dict[str, Any]:
        if kind not in {"refresh", "backup", "migrate"}:
            raise ValueError("Unsupported production job")
        switches, _ = self.read("switches.json")
        if switches.get("paused", True):
            return {"status": "service_limited"}
        self.reconcile(now)
        period = now.strftime("%Y-%m" if kind == "refresh" else "%Y-%m-%d")
        if kind == "migrate":
            period = self.software_version
        key = f"runs/{kind}/{period}.json"
        state, version = self.read(key)
        if state.get("status") == "SUCCEEDED":
            return state
        if state and not retry:
            return state  # Duplicate schedules never turn into automatic retry loops.
        attempt = int(state.get("attempt", 0)) + 1
        approved_recovery = False
        if recovery_approval and kind == "refresh" and attempt > 2 and retry:
            approval, _ = self.read(f"recovery-approvals/{recovery_approval}.json")
            approved_recovery = (
                approval.get("run_key") == key
                and approval.get("previous_state_sha256")
                == hashlib.sha256(json.dumps(state, sort_keys=True).encode()).hexdigest()
                and approval.get("software_version") == self.software_version
                and approval.get("approved_attempt") == attempt
            )
        if (attempt > 2 and not approved_recovery) or (
            state and state.get("status") not in TERMINAL
        ):
            raise ValueError("Retry requires a terminal job and at most two attempts per period")
        active, lock_version = self.read("active.json")
        if active.get("run_key"):
            return {"status": "overlap_prevented", "run_key": active["run_key"]}
        if approved_recovery:
            self.write(f"recovery-history/{kind}/{period}/attempt-{attempt - 1}.json", None, state)
        token = hashlib.sha256(f"{key}/{attempt}".encode()).hexdigest()
        self.write("active.json", lock_version, {"run_key": key, "token": token})
        body = dict(
            kind=kind,
            period=period,
            attempt=attempt,
            token=token,
            status="STARTING",
            started_at=now.isoformat(),
            deadline=(now + timedelta(minutes=45)).isoformat(),
        )
        if approved_recovery:
            body["recovery_approval"] = recovery_approval
        self.write(key, version, body)
        # Persist the intent before StartBuild. If the call is ambiguous, recovery is fail-closed.
        result = self.builds.start_build(
            projectName=self.project,
            idempotencyToken=token,
            environmentVariablesOverride=[
                {"name": "URBANLENS_JOB", "value": kind, "type": "PLAINTEXT"},
                {"name": "URBANLENS_RUN_KEY", "value": key, "type": "PLAINTEXT"},
                {"name": "URBANLENS_RUN_TOKEN", "value": token, "type": "PLAINTEXT"},
            ],
        )["build"]
        body.update(build_id=result["id"], status=result["buildStatus"])
        _, current_version = self.read(key)
        self.write(key, current_version, body)
        return body

    def finish(self, build_id: str, now: datetime) -> dict[str, Any]:
        active, lock_version = self.read("active.json")
        if not active.get("run_key"):
            return {"status": "duplicate_completion_ignored"}
        state, version = self.read(active["run_key"])
        builds = self.builds.batch_get_builds(ids=[build_id])["builds"]
        if not builds or builds[0]["projectName"] != self.project:
            raise ValueError("Unrecognised build completion")
        build = builds[0]
        # EventBridge sends an ARN; the SDK start response stores the canonical id.
        build_id = build["id"]
        env = {v["name"]: v["value"] for v in build["environment"]["environmentVariables"]}
        if env.get("URBANLENS_RUN_TOKEN") != state["token"]:
            return {"status": "unrelated_completion_ignored"}
        status = build["buildStatus"]
        if status not in TERMINAL:
            return {"status": "still_running"}
        if state.get("build_id") not in {None, build_id}:
            raise ValueError("Build identity changed")
        ended_at = build.get("endTime") or now
        state.update(status=status, build_id=build_id, ended_at=ended_at.isoformat())
        self.write(active["run_key"], version, state)
        if status == "SUCCEEDED":
            _, latest_version = self.read(f"last-{state['kind']}.json")
            self.write(f"last-{state['kind']}.json", latest_version, state)
            if state["kind"] == "refresh":
                _, backup_version = self.read("last-backup.json")
                self.write("last-backup.json", backup_version, state)
        self.write("active.json", lock_version, {})
        return state

    def reconcile(self, now: datetime) -> None:
        """Recover a known completed build without relying on event delivery."""
        active, _ = self.read("active.json")
        if active.get("run_key"):
            state, _ = self.read(active["run_key"])
            if state.get("build_id"):
                self.finish(state["build_id"], now)

    def health(self, now: datetime) -> dict[str, Any]:
        """Inspect durable summaries only; never connect to Neon or invoke the public API."""
        alerts = []
        self.reconcile(now)
        active, _ = self.read("active.json")
        if active.get("run_key"):
            state, _ = self.read(active["run_key"])
            if now > datetime.fromisoformat(state["deadline"]):
                # Discover an ambiguous StartBuild by its token; never start another blind job.
                ids = self.builds.list_builds_for_project(
                    projectName=self.project, sortOrder="DESCENDING"
                ).get("ids", [])[:20]
                for build in self.builds.batch_get_builds(ids=ids)["builds"] if ids else []:
                    variables = {
                        v["name"]: v["value"] for v in build["environment"]["environmentVariables"]
                    }
                    if variables.get("URBANLENS_RUN_TOKEN") == state["token"]:
                        if build["buildStatus"] in TERMINAL:
                            self.finish(build["id"], now)
                        else:
                            self.builds.stop_build(id=build["id"])
                        break
                alerts.append("workflow_deadline_exceeded")
        refresh, _ = self.read("last-refresh.json")
        due = now.replace(day=7, hour=12, minute=0, second=0, microsecond=0)
        if now > due + timedelta(hours=48) and (
            not refresh or refresh.get("period") != now.strftime("%Y-%m")
        ):
            alerts.append("monthly_refresh_overdue")
        backup, _ = self.read("last-backup.json")
        if not backup or now - datetime.fromisoformat(backup["ended_at"]) > timedelta(hours=48):
            alerts.append("backup_overdue")
        switches, _ = self.read("switches.json")
        measurements, _ = self.read("measurements.json")
        if measurements.get("database_bytes", 0) > 800_000_000:
            alerts.append("neon_storage_near_limit")
        if measurements.get("database_connections", 0) >= 8:
            alerts.append("database_connection_headroom_low")
        if measurements.get("reference_update_pending"):
            alerts.append("reference_edition_requires_review")
        return {
            "checked_at": now.isoformat(),
            "paused": switches.get("paused", True),
            "alerts": alerts,
        }

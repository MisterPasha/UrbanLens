"""Fenced scope leases and current-snapshot pointers, local SQLite or shared S3 CAS."""

from __future__ import annotations

import json
import sqlite3
from collections.abc import Callable
from contextlib import closing
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Protocol
from uuid import UUID, uuid4

from pydantic import AwareDatetime

from urbanlens.ingestion.contracts import (
    CollectionCycle,
    Contract,
    Failure,
    FailureClass,
    Run,
    Snapshot,
    Stage,
    advance_run,
)
from urbanlens.ingestion.errors import IngestionError, LeaseLost
from urbanlens.ingestion.storage import S3Client, error_code, safe_key


class StateBackend(Protocol):
    def load(self, key: str) -> tuple[bytes, str] | None: ...
    def compare_set(self, key: str, version: str | None, data: bytes) -> str: ...


class SQLiteStateBackend:
    def __init__(self, path: Path) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        self.path = path
        with closing(sqlite3.connect(path)) as connection, connection:
            connection.execute(
                "CREATE TABLE IF NOT EXISTS scope_state "
                "(key TEXT PRIMARY KEY, version TEXT NOT NULL, body BLOB NOT NULL)"
            )

    def load(self, key: str) -> tuple[bytes, str] | None:
        with closing(sqlite3.connect(self.path)) as connection:
            row = connection.execute(
                "SELECT body, version FROM scope_state WHERE key=?", (key,)
            ).fetchone()
        return (bytes(row[0]), str(row[1])) if row else None

    def compare_set(self, key: str, version: str | None, data: bytes) -> str:
        revision = str(uuid4())
        with closing(sqlite3.connect(self.path, timeout=5)) as connection, connection:
            connection.execute("BEGIN IMMEDIATE")
            row = connection.execute(
                "SELECT version FROM scope_state WHERE key=?", (key,)
            ).fetchone()
            if (str(row[0]) if row else None) != version:
                raise LeaseLost()
            connection.execute(
                "INSERT INTO scope_state VALUES (?,?,?) ON CONFLICT(key) "
                "DO UPDATE SET version=excluded.version, body=excluded.body",
                (key, revision, data),
            )
        return revision


class S3StateBackend:
    """Use a single conditional-write object per scope for lease + pointer atomicity."""

    def __init__(self, client: S3Client, bucket: str, prefix: str = "registry") -> None:
        self.client, self.bucket = client, bucket
        self.prefix = safe_key(prefix)

    def load(self, key: str) -> tuple[bytes, str] | None:
        try:
            response = self.client.get_object(
                Bucket=self.bucket, Key=self.prefix + "/" + safe_key(key)
            )
        except Exception as error:
            if error_code(error) in {"404", "NoSuchKey", "NotFound"}:
                return None
            raise IngestionError(
                FailureClass.NETWORK, "registry-read-failed", retryable=True
            ) from None
        body = response["Body"]
        try:
            data = body.read(2_000_001)
            if len(data) > 2_000_000:
                raise IngestionError(FailureClass.BUDGET, "registry-size-budget")
            return data, str(response["ETag"])
        finally:
            body.close()

    def compare_set(self, key: str, version: str | None, data: bytes) -> str:
        try:
            response = self.client.put_object(
                Bucket=self.bucket,
                Key=self.prefix + "/" + safe_key(key),
                Body=data,
                ContentType="application/json",
                **({"IfNoneMatch": "*"} if version is None else {"IfMatch": version}),
            )
            return str(response["ETag"])
        except Exception as error:
            if error_code(error) in {
                "409",
                "412",
                "PreconditionFailed",
                "ConditionalRequestConflict",
            }:
                raise LeaseLost() from None
            raise IngestionError(
                FailureClass.NETWORK, "registry-write-failed", retryable=True
            ) from None


class ScopeState(Contract):
    owner: UUID | None = None
    expires_at: AwareDatetime | None = None
    active_run: Run | None = None
    pending_history: tuple[Run, ...] = ()
    current: Snapshot | None = None


class Lease(Contract):
    key: str
    owner: UUID
    cycle_id: UUID
    run: Run


class Registry:
    def __init__(
        self,
        backend: StateBackend,
        *,
        lease_seconds: int = 360,
        max_attempts: int = 3,
        now: Callable[[], datetime] | None = None,
    ) -> None:
        if lease_seconds <= 0 or max_attempts < 1:
            raise ValueError("Lease duration and attempt limit must be positive")
        self.backend, self.seconds = backend, lease_seconds
        self.max_attempts = max_attempts
        self.now = now or (lambda: datetime.now(UTC))

    @staticmethod
    def key(cycle: CollectionCycle) -> str:
        return f"{cycle.provider.value}/{cycle.scope}/control.json"

    def state(self, key: str) -> tuple[ScopeState, str | None]:
        stored = self.backend.load(key)
        return (
            (ScopeState.model_validate_json(stored[0]), stored[1])
            if stored
            else (ScopeState(), None)
        )

    def save(self, key: str, version: str | None, state: ScopeState) -> None:
        data = state.model_dump_json().encode()
        if len(data) > 2_000_000:
            raise IngestionError(FailureClass.BUDGET, "registry-size-budget")
        self.backend.compare_set(key, version, data)

    def acquire(self, cycle: CollectionCycle, run: Run) -> Lease:
        key, now = self.key(cycle), self.now()
        if run.cycle_id != cycle.cycle_id:
            raise ValueError("Run/cycle mismatch")
        state, version = self.state(key)
        if state.owner is not None and state.expires_at is not None and state.expires_at > now:
            raise IngestionError(FailureClass.INTERNAL, "scope-already-running", retryable=True)
        pending = list(state.pending_history)
        if state.active_run is not None:
            previous = state.active_run
            if previous.ended_at is None:
                previous = Run.model_validate(
                    {
                        **previous.model_dump(),
                        "status": Stage.FAILED,
                        "ended_at": now,
                        "failure": Failure(
                            category=FailureClass.INTERNAL,
                            code="worker-interrupted",
                            retryable=True,
                        ),
                    }
                )
            pending.append(previous)
            if (
                previous.cycle_id == run.cycle_id
                and previous.mode == run.mode
                and previous.pipeline_version == run.pipeline_version
                and previous.replay_of_snapshot_id == run.replay_of_snapshot_id
                and previous.failure is not None
                and previous.failure.retryable
            ):
                if previous.attempt >= self.max_attempts:
                    raise IngestionError(FailureClass.BUDGET, "run-retry-budget")
                run = Run.model_validate(
                    {
                        **run.model_dump(),
                        "attempt": previous.attempt + 1,
                        "retry_of_run_id": previous.run_id,
                        "started_at": now,
                    }
                )
        lease = Lease(key=key, owner=uuid4(), cycle_id=cycle.cycle_id, run=run)
        self.save(
            key,
            version,
            ScopeState(
                owner=lease.owner,
                expires_at=now + timedelta(seconds=self.seconds),
                active_run=run,
                pending_history=tuple(pending),
                current=state.current,
            ),
        )
        return lease

    def owned(self, lease: Lease) -> tuple[ScopeState, str | None]:
        state, version = self.state(lease.key)
        if state.owner != lease.owner or state.expires_at is None or state.expires_at <= self.now():
            raise LeaseLost()
        return state, version

    def update(
        self,
        lease: Lease,
        run: Run,
        *,
        current: Snapshot | None = None,
        history_saved: bool = False,
    ) -> None:
        state, version = self.owned(lease)
        if run.cycle_id != lease.cycle_id:
            raise ValueError("Run is outside lease cycle")
        if state.active_run is None or state.active_run.run_id != run.run_id:
            raise LeaseLost()
        changing = {"status", "ended_at", "failure"}
        if run.model_dump(exclude=changing) != lease.run.model_dump(exclude=changing):
            raise ValueError("Run identity/mode is pinned when the lease is acquired")
        if state.active_run.ended_at is not None and state.active_run != run:
            raise ValueError("Finished run state is immutable")
        if state.active_run.status != run.status:
            advance_run(state.active_run, run.status, ended_at=run.ended_at, failure=run.failure)
        if current is not None:
            if (
                run.mode != "collect"
                or current.run.status != Stage.ACCEPTED
                or current.cycle.cycle_id != lease.cycle_id
                or current.run != run
                or self.key(current.cycle) != lease.key
            ):
                raise ValueError("Only this lease's accepted collection can become current")
            if state.current and state.current.cycle.window_start > current.cycle.window_start:
                raise IngestionError(FailureClass.SCOPE, "older-cycle-cannot-replace-current")
            if (
                state.current
                and state.current.cycle.window_start == current.cycle.window_start
                and state.current.cycle.cycle_id != current.cycle.cycle_id
            ):
                raise IngestionError(FailureClass.SCOPE, "duplicate-collection-window")
        self.save(
            lease.key,
            version,
            ScopeState(
                owner=lease.owner,
                expires_at=self.now() + timedelta(seconds=self.seconds),
                active_run=run,
                pending_history=() if history_saved else state.pending_history,
                current=current or state.current,
            ),
        )

    def release(self, lease: Lease) -> None:
        state, version = self.owned(lease)
        self.save(
            lease.key,
            version,
            ScopeState(
                active_run=state.active_run,
                pending_history=state.pending_history,
                current=state.current,
            ),
        )

    def current(self, cycle: CollectionCycle) -> Snapshot | None:
        return self.state(self.key(cycle))[0].current


def safe_log(run: Run) -> str:
    """Allowlist metadata, rather than attempting to redact arbitrary exception text."""
    return json.dumps(
        {
            "run_id": str(run.run_id),
            "cycle_id": str(run.cycle_id),
            "mode": run.mode,
            "stage": run.status.value,
            "attempt": run.attempt,
            "failure": run.failure.model_dump(mode="json") if run.failure else None,
        }
    )

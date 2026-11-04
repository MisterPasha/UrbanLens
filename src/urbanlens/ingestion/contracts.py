"""Versioned, immutable collection metadata, independent of network/storage clients."""

from __future__ import annotations

from datetime import date, datetime, timedelta
from enum import StrEnum
from typing import Annotated, Literal, Self
from urllib.parse import urlsplit
from uuid import UUID

from pydantic import (
    AwareDatetime,
    BaseModel,
    ConfigDict,
    Field,
    NonNegativeInt,
    StringConstraints,
    field_validator,
    model_validator,
)

Token = Annotated[str, StringConstraints(pattern=r"^[a-z0-9][a-z0-9_-]{0,95}$")]
Digest = Annotated[str, StringConstraints(pattern=r"^[0-9a-f]{64}$")]
Version = Annotated[str, StringConstraints(min_length=1, max_length=120)]


class Contract(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, validate_default=True)

    @field_validator("*", mode="after")
    @classmethod
    def require_utc(cls, value: object) -> object:
        if isinstance(value, datetime) and value.utcoffset() != timedelta(0):
            raise ValueError("Operational timestamps must be UTC")
        return value


class Provider(StrEnum):
    FSA = "fsa"
    OSM = "osm"
    COMPANIES_HOUSE = "companies-house"
    POSTCODES = "postcodes-io"
    ONS = "ons"


class Stage(StrEnum):
    PENDING = "pending"
    FETCHED = "fetched"
    PARSED = "parsed"
    COMPLETE = "complete"
    ACCEPTED = "accepted"
    REJECTED = "rejected"
    TRANSFORMED = "transformed"
    MODELLED = "modelled"
    PUBLISHED = "published"
    FAILED = "failed"


class FailureClass(StrEnum):
    TIMEOUT = "timeout"
    NETWORK = "network"
    RATE_LIMIT = "rate-limit"
    AUTHENTICATION = "authentication"
    SCHEMA = "schema"
    INTEGRITY = "integrity"
    SCOPE = "scope"
    INCOMPLETE = "incomplete"
    BUDGET = "budget"
    INTERNAL = "internal"


class Failure(Contract):
    category: FailureClass
    code: Token
    retryable: bool
    # Stable codes only: raw exception text, URLs and credentials belong nowhere here.


class CollectionCycle(Contract):
    contract_version: Literal["1"] = "1"
    cycle_id: UUID
    provider: Provider
    scope: Token
    query_version: Version
    query_sha256: Digest
    window_start: AwareDatetime
    window_end: AwareDatetime
    expected_partitions: tuple[Token, ...] = ("full",)
    source_edition: Version | None = None

    @model_validator(mode="after")
    def check_window_and_parts(self) -> Self:
        if self.window_end <= self.window_start:
            raise ValueError("Collection window must have a positive duration")
        if not self.expected_partitions or len(set(self.expected_partitions)) != len(
            self.expected_partitions
        ):
            raise ValueError("Expected partition identities must be nonempty and unique")
        return self


class Run(Contract):
    contract_version: Literal["1"] = "1"
    run_id: UUID
    cycle_id: UUID
    pipeline_version: Version
    mode: Literal["collect", "replay"] = "collect"
    replay_of_snapshot_id: UUID | None = None
    attempt: Annotated[int, Field(ge=1)] = 1
    retry_of_run_id: UUID | None = None
    started_at: AwareDatetime
    ended_at: AwareDatetime | None = None
    status: Stage = Stage.PENDING
    failure: Failure | None = None

    @model_validator(mode="after")
    def check_attempt_and_outcome(self) -> Self:
        if (self.mode == "replay") != (self.replay_of_snapshot_id is not None):
            raise ValueError(
                "Replay requires an original snapshot; collection cannot impersonate replay"
            )
        if (self.attempt > 1) != (self.retry_of_run_id is not None):
            raise ValueError("Retries require the preceding run ID; first attempts cannot have one")
        if self.retry_of_run_id == self.run_id:
            raise ValueError("A run cannot retry itself")
        if self.ended_at is not None and self.ended_at < self.started_at:
            raise ValueError("Run end precedes start")
        if (self.status == Stage.FAILED) != (self.failure is not None):
            raise ValueError("Only failed runs carry a failure, and failure must be classified")
        if self.status in {Stage.FAILED, Stage.REJECTED, Stage.PUBLISHED} and self.ended_at is None:
            raise ValueError("Terminal outcomes require an end timestamp")
        if self.status == Stage.PENDING and self.ended_at is not None:
            raise ValueError("A pending run cannot already be finished")
        return self


_NEXT: dict[Stage, frozenset[Stage]] = {
    Stage.PENDING: frozenset({Stage.FETCHED}),
    Stage.FETCHED: frozenset({Stage.PARSED}),
    Stage.PARSED: frozenset({Stage.COMPLETE, Stage.REJECTED}),
    Stage.COMPLETE: frozenset({Stage.ACCEPTED, Stage.REJECTED}),
    Stage.ACCEPTED: frozenset({Stage.TRANSFORMED}),
    Stage.TRANSFORMED: frozenset({Stage.MODELLED}),
    Stage.MODELLED: frozenset({Stage.PUBLISHED}),
}


def advance_run(
    run: Run, status: Stage, *, ended_at: datetime | None = None, failure: Failure | None = None
) -> Run:
    """Validate one stage change; retries create a new run instead of reopening this one."""
    if run.ended_at is not None or run.status in {Stage.FAILED, Stage.REJECTED, Stage.PUBLISHED}:
        raise ValueError("A finished run cannot advance")
    if status != Stage.FAILED and status not in _NEXT.get(run.status, frozenset()):
        raise ValueError(f"Invalid stage transition: {run.status} -> {status}")
    return Run.model_validate(
        {**run.model_dump(), "status": status, "ended_at": ended_at, "failure": failure}
    )


class Artifact(Contract):
    uri: str
    sha256: Digest
    byte_count: NonNegativeInt
    media_type: Version
    compression: Literal["none", "gzip", "zip"] = "none"

    @field_validator("uri")
    @classmethod
    def private_storage_uri(cls, value: str) -> str:
        parsed = urlsplit(value)
        if parsed.scheme not in {"file", "s3"} or not parsed.path.startswith("/"):
            raise ValueError("Artifact URI must be an absolute file:// or s3:// location")
        if parsed.query or parsed.fragment or parsed.username or parsed.password:
            raise ValueError("Artifact URIs must not contain credentials or signed query strings")
        if parsed.scheme == "s3" and not parsed.netloc:
            raise ValueError("S3 artifact URI requires a bucket")
        if parsed.scheme == "file" and parsed.netloc not in {"", "localhost"}:
            raise ValueError("Local artifacts cannot reference a remote host")
        return value


class Counts(Contract):
    raw: NonNegativeInt | None = None
    parsed: NonNegativeInt | None = None
    accepted: NonNegativeInt | None = None
    rejected: NonNegativeInt | None = None

    @model_validator(mode="after")
    def reconcile(self) -> Self:
        if self.raw is not None and self.parsed is not None and self.parsed > self.raw:
            raise ValueError("Parsed source records exceed raw records")
        if (self.accepted is None) != (self.rejected is None):
            raise ValueError("Accepted/rejected counts must be reported together")
        if self.accepted is not None and self.rejected is not None:
            if self.parsed is None or self.accepted + self.rejected != self.parsed:
                raise ValueError("Accepted + rejected must equal parsed source records")
        return self


class Partition(Contract):
    partition_id: Token
    cycle_id: UUID
    run_id: UUID
    source_edition: Version | None = None
    schema_version: Version
    pipeline_version: Version
    status: Literal["pending", "fetched", "parsed", "complete", "failed"]
    fetched_at: AwareDatetime | None = None
    source_extracted_at: AwareDatetime | None = None
    source_extract_date: date | None = None
    artifacts: tuple[Artifact, ...] = ()
    counts: Counts = Field(default_factory=Counts)
    failure: Failure | None = None

    @model_validator(mode="after")
    def validate_result(self) -> Self:
        if (self.status == "failed") != (self.failure is not None):
            raise ValueError("Partition failures must be explicit and classified")
        if self.status in {"fetched", "parsed", "complete"}:
            if self.fetched_at is None or not self.artifacts:
                raise ValueError("Successful fetch requires retained artifacts and fetched-at UTC")
        if self.status in {"parsed", "complete"} and self.counts.parsed is None:
            raise ValueError("Parse outcome requires a record count, including zero")
        if self.status == "complete" and (
            self.counts.raw is None or self.counts.raw != self.counts.parsed
        ):
            raise ValueError("A complete partition must parse all raw source records")
        return self


class ValidationCheck(Contract):
    code: Token
    blocking: bool = True
    outcome: Literal["passed", "failed", "not-run"]


class ValidationReport(Contract):
    rule_version: Version
    required_checks: tuple[Token, ...] = ("schema", "scope", "completeness", "counts")
    checks: tuple[ValidationCheck, ...] = ()

    @model_validator(mode="after")
    def unique_checks(self) -> Self:
        if len({c.code for c in self.checks}) != len(self.checks):
            raise ValueError("Validation check codes must be unique")
        if not self.required_checks or len(set(self.required_checks)) != len(self.required_checks):
            raise ValueError("Required validation check codes must be nonempty and unique")
        return self

    @property
    def passed(self) -> bool:
        blocking = [c for c in self.checks if c.blocking]
        return set(self.required_checks) <= {c.code for c in blocking} and all(
            c.outcome == "passed" for c in blocking
        )


class Snapshot(Contract):
    contract_version: Literal["1"] = "1"
    snapshot_id: UUID
    cycle: CollectionCycle
    run: Run
    completeness: Literal["pending", "partial", "complete", "failed"]
    partitions: tuple[Partition, ...] = ()
    validation: ValidationReport

    @model_validator(mode="after")
    def reconcile_collection(self) -> Self:
        if self.run.mode != "collect" or self.run.cycle_id != self.cycle.cycle_id:
            raise ValueError("A fresh snapshot requires a collection run in its original cycle")
        ids = [p.partition_id for p in self.partitions]
        expected = set(self.cycle.expected_partitions)
        if len(set(ids)) != len(ids) or not set(ids) <= expected:
            raise ValueError("Snapshot has duplicate or unexpected partitions")
        for part in self.partitions:
            if part.cycle_id != self.cycle.cycle_id:
                raise ValueError("Partition belongs to a different collection cycle")
            if part.source_edition != self.cycle.source_edition:
                raise ValueError("Partition editions differ from the collection edition")
            if part.pipeline_version != self.run.pipeline_version:
                raise ValueError("Partition pipeline versions differ from the collection run")
        if self.completeness == "complete":
            if set(ids) != expected or any(p.status != "complete" for p in self.partitions):
                raise ValueError("Complete collection requires every expected complete partition")
            if len({p.schema_version for p in self.partitions}) != 1:
                raise ValueError("Complete collection cannot mix partition schemas")
        progressed = {
            Stage.COMPLETE,
            Stage.ACCEPTED,
            Stage.TRANSFORMED,
            Stage.MODELLED,
            Stage.PUBLISHED,
        }
        if self.run.status in progressed and self.completeness != "complete":
            raise ValueError("An incomplete snapshot cannot progress to complete or accepted")
        if self.run.status in progressed - {Stage.COMPLETE}:
            if not self.validation.passed:
                raise ValueError("Acceptance requires all blocking validation checks to pass")
            if any(p.counts.accepted is None for p in self.partitions):
                raise ValueError("Acceptance requires reconciled accepted/rejected counts")
        if self.run.status == Stage.FAILED and self.completeness != "failed":
            raise ValueError("Failed collection cannot masquerade as partial or empty success")
        if self.completeness == "failed" and self.run.status != Stage.FAILED:
            raise ValueError("Failed completeness requires a failed collection run")
        if self.run.status == Stage.REJECTED and not any(
            c.blocking and c.outcome == "failed" for c in self.validation.checks
        ):
            raise ValueError("Rejection requires a recorded blocking validation failure")
        return self

    @property
    def successful_empty(self) -> bool:
        return (
            self.completeness == "complete"
            and self.validation.passed
            and self.run.status
            in {Stage.ACCEPTED, Stage.TRANSFORMED, Stage.MODELLED, Stage.PUBLISHED}
            and all(p.counts.raw == 0 for p in self.partitions)
        )


def validate_replay(run: Run, original: Snapshot) -> None:
    """Replay references existing observation metadata; it creates no fresh source snapshot."""
    if (
        run.mode != "replay"
        or run.replay_of_snapshot_id != original.snapshot_id
        or run.cycle_id != original.cycle.cycle_id
    ):
        raise ValueError("Replay must reference the original snapshot and collection cycle")


def validate_retry(run: Run, previous: Run) -> None:
    if (
        previous.ended_at is None
        or previous.status != Stage.FAILED
        or previous.failure is None
        or not previous.failure.retryable
        or run.retry_of_run_id != previous.run_id
        or run.cycle_id != previous.cycle_id
        or run.attempt != previous.attempt + 1
        or run.mode != previous.mode
        or run.pipeline_version != previous.pipeline_version
        or run.replay_of_snapshot_id != previous.replay_of_snapshot_id
        or run.started_at < previous.ended_at
    ):
        raise ValueError("Retry must follow the preceding finished attempt in the same cycle/mode")

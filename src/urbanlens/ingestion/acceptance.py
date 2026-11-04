"""Immutable, evidence-bound inspection decisions before source promotion."""

from __future__ import annotations

import hashlib
import json
from datetime import datetime
from typing import Literal
from uuid import UUID

from urbanlens.ingestion.contracts import (
    Artifact,
    CollectionCycle,
    Contract,
    Partition,
    Run,
    Snapshot,
    ValidationCheck,
    ValidationReport,
)
from urbanlens.ingestion.interfaces import ArtifactStore


class Review(Contract):
    cycle_id: UUID
    evidence_sha256: str
    rule_version: str = "source-acceptance-v1"
    change_records: int = 20
    change_fraction: float = 0.2
    reviewer: str
    reviewed_at: datetime
    decision: Literal["approve", "reject"]
    reason: str


def evidence_digest(cycle: CollectionCycle, partitions: tuple[Partition, ...]) -> str:
    evidence = {
        "cycle": cycle.model_dump(mode="json"),
        "partitions": [
            {
                "id": p.partition_id,
                "schema": p.schema_version,
                "pipeline": p.pipeline_version,
                "artifacts": [a.model_dump(mode="json") for a in p.artifacts],
                "counts": p.counts.model_dump(),
                "fetched_at": None if p.fetched_at is None else p.fetched_at.isoformat(),
            }
            for p in partitions
        ],
    }
    return hashlib.sha256(json.dumps(evidence, sort_keys=True).encode()).hexdigest()


class AcceptanceService:
    def __init__(
        self, store: ArtifactStore, *, change_records: int = 20, change_fraction: float = 0.2
    ) -> None:
        if change_records < 1 or not 0 < change_fraction <= 1:
            raise ValueError("Invalid anomaly threshold")
        self.store, self.change_records, self.change_fraction = (
            store,
            change_records,
            change_fraction,
        )

    def key(self, cycle: CollectionCycle, digest: str) -> str:
        return f"evidence/reviews/{cycle.provider}/{cycle.scope}/{cycle.cycle_id}/{digest}.json"

    def record(
        self,
        cycle: CollectionCycle,
        partitions: tuple[Partition, ...],
        *,
        reviewer: str,
        reviewed_at: datetime,
        decision: Literal["approve", "reject"],
        reason: str,
    ) -> Artifact:
        if not reviewer.strip() or not reason.strip():
            raise ValueError("Inspection needs a reviewer and reason")
        digest = evidence_digest(cycle, partitions)
        review = Review(
            cycle_id=cycle.cycle_id,
            evidence_sha256=digest,
            reviewer=reviewer,
            reviewed_at=reviewed_at,
            decision=decision,
            reason=reason,
            change_records=self.change_records,
            change_fraction=self.change_fraction,
        )
        existing = self.store.stat(self.key(cycle, digest))
        if existing is not None:
            previous = Review.model_validate_json(b"".join(self.store.read_verified(existing)))
            if previous.model_dump(exclude={"reviewed_at"}) == review.model_dump(
                exclude={"reviewed_at"}
            ):
                return existing  # Repeated review preserves the original decision time/bytes.
        return self.store.put_immutable(
            self.key(cycle, digest),
            iter([review.model_dump_json().encode()]),
            media_type="application/json",
            compression="none",
        )

    def reviewed(self, cycle: CollectionCycle, partitions: tuple[Partition, ...]) -> bool:
        digest = evidence_digest(cycle, partitions)
        artifact = self.store.stat(self.key(cycle, digest))
        if artifact is None:
            return False
        review = Review.model_validate_json(b"".join(self.store.read_verified(artifact)))
        return (
            review.cycle_id == cycle.cycle_id
            and review.evidence_sha256 == digest
            and review.rule_version == "source-acceptance-v1"
            and review.change_records == self.change_records
            and review.change_fraction == self.change_fraction
            and review.decision == "approve"
        )

    def evaluate(
        self,
        cycle: CollectionCycle,
        run: Run,
        partitions: tuple[Partition, ...],
        validation: ValidationReport,
        current: Snapshot | None,
    ) -> ValidationReport:
        reviewed = self.reviewed(cycle, partitions)
        trusted_baseline = current is not None and (
            ("baseline-review" in current.validation.required_checks and current.validation.passed)
            or self.reviewed(current.cycle, current.partitions)
        )
        checks = list(validation.checks)
        current_counts = {p.partition_id: p.counts.accepted or 0 for p in partitions}
        previous_counts = (
            {}
            if current is None
            else {p.partition_id: p.counts.accepted or 0 for p in current.partitions}
        )
        old, new = sum(previous_counts.values()), sum(current_counts.values())

        def anomaly(before: int, after: int) -> bool:
            difference = abs(after - before)
            return difference >= self.change_records and (
                before == 0 or difference / before >= self.change_fraction
            )

        changed = current is not None and (
            anomaly(old, new)
            or any(
                anomaly(previous_counts.get(k, 0), current_counts.get(k, 0))
                for k in previous_counts.keys() | current_counts.keys()
            )
        )
        checks.extend(
            [
                ValidationCheck(
                    code="baseline-review",
                    outcome="passed" if trusted_baseline or reviewed else "failed",
                ),
                ValidationCheck(
                    code="anomaly-review", outcome="passed" if not changed or reviewed else "failed"
                ),
            ]
        )
        return ValidationReport(
            rule_version="source-acceptance-v1",
            required_checks=(*validation.required_checks, "baseline-review", "anomaly-review"),
            checks=tuple(checks),
        )

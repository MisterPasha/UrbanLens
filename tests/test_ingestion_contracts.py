"""Collection identity, acceptance and replay boundaries using synthetic inputs only."""

import hashlib
from datetime import UTC, datetime, timedelta, timezone
from pathlib import Path
from uuid import UUID

import pytest
from pydantic import ValidationError

from urbanlens.ingestion.contracts import (
    Artifact,
    CollectionCycle,
    Counts,
    Failure,
    FailureClass,
    Partition,
    Provider,
    Run,
    Snapshot,
    Stage,
    ValidationCheck,
    ValidationReport,
    advance_run,
    validate_replay,
    validate_retry,
)
from urbanlens.ingestion.interfaces import Layer, artifact_key

NOW = datetime(2026, 11, 4, tzinfo=UTC)


def cycle(**changes: object) -> CollectionCycle:
    return CollectionCycle.model_validate(
        {
            "cycle_id": UUID(int=1),
            "provider": Provider.FSA,
            "scope": "brighton-hove-e06000043",
            "query_version": "authority-v1",
            "query_sha256": "a" * 64,
            "window_start": NOW,
            "window_end": NOW + timedelta(days=1),
            **changes,
        }
    )


def run(**changes: object) -> Run:
    return Run.model_validate(
        {
            "run_id": UUID(int=2),
            "cycle_id": UUID(int=1),
            "pipeline_version": "test-v1",
            "started_at": NOW,
            **changes,
        }
    )


def part(**changes: object) -> Partition:
    return Partition.model_validate(
        {
            "partition_id": "full",
            "cycle_id": UUID(int=1),
            "run_id": UUID(int=2),
            "schema_version": "fsa-v1",
            "pipeline_version": "test-v1",
            "status": "complete",
            "fetched_at": NOW + timedelta(seconds=1),
            "artifacts": [
                Artifact(
                    uri="file:///synthetic/empty.json",
                    sha256="b" * 64,
                    byte_count=2,
                    media_type="application/json",
                )
            ],
            "counts": Counts(raw=0, parsed=0, accepted=0, rejected=0),
            **changes,
        }
    )


def snapshot(**changes: object) -> Snapshot:
    return Snapshot.model_validate(
        {
            "snapshot_id": UUID(int=3),
            "cycle": cycle(),
            "run": run(status=Stage.ACCEPTED, ended_at=NOW + timedelta(seconds=2)),
            "completeness": "complete",
            "partitions": [part()],
            "validation": ValidationReport(
                rule_version="test-v1",
                checks=tuple(
                    ValidationCheck(code=code, outcome="passed")
                    for code in ("schema", "scope", "completeness", "counts")
                ),
            ),
            **changes,
        }
    )


def test_complete_empty_is_explicit_and_json_round_trips() -> None:
    result = snapshot()
    assert result.successful_empty
    assert Snapshot.model_validate_json(result.model_dump_json()) == result
    with pytest.raises(ValidationError):
        result.completeness = "failed"


def test_partial_and_failed_are_not_empty_success() -> None:
    partial = snapshot(
        run=run(status=Stage.PARSED),
        completeness="partial",
        partitions=[part(status="parsed", counts=Counts(raw=5, parsed=2))],
    )
    failure = Failure(category=FailureClass.TIMEOUT, code="provider-timeout", retryable=True)
    failed = snapshot(
        run=run(status=Stage.FAILED, ended_at=NOW + timedelta(seconds=2), failure=failure),
        completeness="failed",
        partitions=[],
    )
    assert not partial.successful_empty and not failed.successful_empty
    assert partial.partitions[0].counts.accepted is None


@pytest.mark.parametrize("ids", [("a",), ("a", "a"), ("a", "b", "unexpected")])
def test_missing_duplicate_or_unexpected_parts_cannot_be_complete(ids: tuple[str, ...]) -> None:
    with pytest.raises(ValidationError):
        snapshot(
            cycle=cycle(expected_partitions=("a", "b")),
            partitions=[part(partition_id=p) for p in ids],
        )


@pytest.mark.parametrize(
    "change",
    [
        {"status": "fetched", "counts": Counts()},
        {"source_edition": "different-month"},
        {"cycle_id": UUID(int=99)},
        {"pipeline_version": "different-code"},
        {"schema_version": "different-schema"},
    ],
)
def test_incompatible_partition_cannot_complete_collection(change: dict[str, object]) -> None:
    with pytest.raises(ValidationError):
        snapshot(
            cycle=cycle(expected_partitions=("a", "b")),
            partitions=[part(partition_id="a"), part(partition_id="b", **change)],
        )


@pytest.mark.parametrize("outcome", ["failed", "not-run"])
def test_blocking_validation_prevents_acceptance(outcome: str) -> None:
    with pytest.raises(ValidationError, match="blocking"):
        snapshot(
            validation=ValidationReport.model_validate(
                {"rule_version": "test", "checks": [{"code": "scope", "outcome": outcome}]}
            )
        )


def test_no_checks_is_not_validation_success() -> None:
    assert not ValidationReport(rule_version="test").passed
    with pytest.raises(ValidationError):
        snapshot(validation=ValidationReport(rule_version="test"))


def test_omitted_required_checks_are_not_silently_passed() -> None:
    report = ValidationReport(
        rule_version="test", checks=(ValidationCheck(code="schema", outcome="passed"),)
    )
    assert not report.passed
    with pytest.raises(ValidationError):
        snapshot(validation=report)


def test_complete_nonempty_collection_can_pass_with_a_nonblocking_warning() -> None:
    base = snapshot()
    report = ValidationReport(
        rule_version="test-v1",
        checks=(
            *base.validation.checks,
            ValidationCheck(code="coordinates-missing", blocking=False, outcome="failed"),
        ),
    )
    result = snapshot(
        cycle=cycle(expected_partitions=("a", "b")),
        partitions=[
            part(partition_id="a", counts=Counts(raw=4, parsed=4, accepted=3, rejected=1)),
            part(partition_id="b", counts=Counts(raw=2, parsed=2, accepted=2, rejected=0)),
        ],
        validation=report,
    )
    assert result.completeness == "complete" and not result.successful_empty


def test_rejection_retains_raw_evidence_and_records_the_reason() -> None:
    result = snapshot(
        run=run(status=Stage.REJECTED, ended_at=NOW + timedelta(seconds=2)),
        validation=ValidationReport(
            rule_version="test", checks=(ValidationCheck(code="scope", outcome="failed"),)
        ),
    )
    assert result.partitions[0].artifacts and not result.successful_empty
    with pytest.raises(ValidationError, match="Rejection requires"):
        snapshot(run=result.run)


def test_failed_fetch_can_retain_raw_bytes_without_claiming_a_record_count() -> None:
    failure = Failure(category=FailureClass.SCHEMA, code="provider-error-body", retryable=False)
    result = snapshot(
        run=run(status=Stage.FAILED, ended_at=NOW + timedelta(seconds=2), failure=failure),
        completeness="failed",
        partitions=[part(status="failed", counts=Counts(), failure=failure)],
    )
    assert result.partitions[0].artifacts
    assert result.partitions[0].counts.raw is None and not result.successful_empty


def test_serialized_schemas_match_the_contracts() -> None:
    from scripts.ingestion_schema import DIRECTORY, schemas

    for name, expected in schemas().items():
        assert (DIRECTORY / name).read_text(encoding="utf-8") == expected


def test_recorded_examples_remain_valid() -> None:
    directory = Path(__file__).resolve().parents[1] / "reference/ingestion/examples"
    original = Snapshot.model_validate_json((directory / "complete-empty.json").read_text())
    replay = Run.model_validate_json((directory / "replay-run.json").read_text())
    validate_replay(replay, original)
    assert original.successful_empty
    raw = (directory / "empty-source.json").read_bytes()
    artifact = original.partitions[0].artifacts[0]
    assert len(raw) == artifact.byte_count
    assert hashlib.sha256(raw).hexdigest() == artifact.sha256


def test_accepted_counts_must_reconcile_and_not_be_unknown() -> None:
    with pytest.raises(ValidationError):
        Counts(raw=10, parsed=10, accepted=7, rejected=2)
    with pytest.raises(ValidationError):
        snapshot(partitions=[part(counts=Counts(raw=10, parsed=10))])
    with pytest.raises(ValidationError):
        part(counts=Counts(raw=10, parsed=9))


def test_retry_preserves_cycle_and_changes_run_and_key() -> None:
    failure = Failure(category=FailureClass.NETWORK, code="connection-lost", retryable=True)
    previous = run(status=Stage.FAILED, failure=failure, ended_at=NOW + timedelta(seconds=1))
    retry = run(
        run_id=UUID(int=4),
        attempt=2,
        retry_of_run_id=previous.run_id,
        started_at=NOW + timedelta(seconds=2),
    )
    validate_retry(retry, previous)
    assert artifact_key(Layer.BRONZE, cycle(), retry.run_id, "source.json") != artifact_key(
        Layer.BRONZE, cycle(), previous.run_id, "source.json"
    )
    with pytest.raises(ValueError):
        validate_retry(run(**{**retry.model_dump(), "cycle_id": UUID(int=9)}), previous)
    with pytest.raises(ValueError):
        validate_retry(run(**{**retry.model_dump(), "pipeline_version": "new-code"}), previous)


def test_nonretryable_failure_cannot_be_automatically_retried() -> None:
    failure = Failure(category=FailureClass.AUTHENTICATION, code="invalid-key", retryable=False)
    previous = run(status=Stage.FAILED, failure=failure, ended_at=NOW + timedelta(seconds=1))
    retry = run(
        run_id=UUID(int=4),
        attempt=2,
        retry_of_run_id=previous.run_id,
        started_at=NOW + timedelta(seconds=2),
    )
    with pytest.raises(ValueError):
        validate_retry(retry, previous)


def test_replay_references_original_and_cannot_create_fresh_snapshot() -> None:
    original = snapshot()
    replay = run(mode="replay", replay_of_snapshot_id=original.snapshot_id, run_id=UUID(int=4))
    validate_replay(replay, original)
    with pytest.raises(ValidationError, match="fresh snapshot"):
        snapshot(run=replay)
    with pytest.raises(ValueError):
        validate_replay(run(**{**replay.model_dump(), "cycle_id": UUID(int=9)}), original)
    assert original.partitions[0].fetched_at == NOW + timedelta(seconds=1)


def test_new_collection_of_identical_content_has_a_different_cycle_key() -> None:
    first, second = (
        cycle(),
        cycle(
            cycle_id=UUID(int=5),
            window_start=NOW + timedelta(days=1),
            window_end=NOW + timedelta(days=2),
        ),
    )
    assert artifact_key(Layer.BRONZE, first, UUID(int=2), "source.json") != artifact_key(
        Layer.BRONZE, second, UUID(int=6), "source.json"
    )
    # Content checksum is deliberately independent of either observation identity.
    assert snapshot().partitions[0].artifacts[0].sha256 == "b" * 64


def test_stage_transitions_cannot_skip_acceptance_or_reopen_failure() -> None:
    fetched = advance_run(run(), Stage.FETCHED)
    with pytest.raises(ValueError):
        advance_run(fetched, Stage.PUBLISHED)
    parsed = advance_run(fetched, Stage.PARSED)
    complete = advance_run(parsed, Stage.COMPLETE)
    accepted = advance_run(complete, Stage.ACCEPTED)
    modelled = advance_run(advance_run(accepted, Stage.TRANSFORMED), Stage.MODELLED)
    published = advance_run(modelled, Stage.PUBLISHED, ended_at=NOW + timedelta(seconds=2))
    with pytest.raises(ValueError):
        advance_run(published, Stage.FETCHED)
    with pytest.raises(ValidationError):
        advance_run(run(), Stage.FAILED, ended_at=NOW)


@pytest.mark.parametrize("filename", ["../source.json", "nested/source.json", "C:\\secret", ".."])
def test_artifact_keys_reject_path_escape(filename: str) -> None:
    with pytest.raises(ValueError):
        artifact_key(Layer.BRONZE, cycle(), UUID(int=2), filename)


@pytest.mark.parametrize(
    "uri",
    [
        "https://host/file?token=secret",
        "s3://bucket/a?token=x",
        "s3://user:password@bucket/a",
        "relative.json",
    ],
)
def test_artifact_metadata_cannot_store_credentials_or_signed_urls(uri: str) -> None:
    with pytest.raises(ValidationError):
        Artifact(uri=uri, sha256="b" * 64, byte_count=1, media_type="application/json")


def test_times_are_aware_utc_and_unknown_fields_are_rejected() -> None:
    with pytest.raises(ValidationError):
        run(started_at=NOW.replace(tzinfo=None))
    with pytest.raises(ValidationError):
        run(started_at=NOW.astimezone(timezone(timedelta(hours=1))))
    with pytest.raises(ValidationError):
        run(api_key="do-not-store-credentials")


@pytest.mark.parametrize("provider", list(Provider))
def test_all_five_providers_use_the_same_collection_contract(provider: Provider) -> None:
    assert cycle(provider=provider).provider == provider

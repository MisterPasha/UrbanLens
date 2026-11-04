"""Review gates bind to immutable evidence and never override structural validity."""

import json
from collections.abc import Iterator, Mapping
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any
from uuid import NAMESPACE_URL, uuid5

import pytest

from urbanlens.ingestion.acceptance import AcceptanceService
from urbanlens.ingestion.cadence import month_window
from urbanlens.ingestion.contracts import CollectionCycle, Counts, Partition, Provider, Run, Stage
from urbanlens.ingestion.engine import Collector
from urbanlens.ingestion.errors import Conflict
from urbanlens.ingestion.interfaces import ArtifactStore, Layer, artifact_key
from urbanlens.ingestion.payload import payload
from urbanlens.ingestion.registry import Registry, SQLiteStateBackend
from urbanlens.ingestion.storage import LocalArtifactStore
from urbanlens.ingestion.validation import ValidationRules, Validator

NOW = datetime(2026, 11, 4, 12, tzinfo=UTC)


def cycle(month: int) -> CollectionCycle:
    start, end = month_window(date(2026, month, 4))
    return CollectionCycle(
        cycle_id=uuid5(NAMESPACE_URL, str(start)),
        provider=Provider.FSA,
        scope="test-review",
        query_version="fixture-v1",
        query_sha256="a" * 64,
        window_start=start,
        window_end=end,
    )


class Adapter:
    provider = Provider.FSA

    def __init__(self, values: list[dict[str, Any]]) -> None:
        self.values = values

    def fetch(
        self, cycle: CollectionCycle, run: Run, partition_id: str, store: ArtifactStore
    ) -> Partition:
        artifact = store.put_immutable(
            artifact_key(Layer.BRONZE, cycle, run.run_id, "raw.json", partition_id),
            iter([json.dumps(self.values).encode()]),
            media_type="application/json",
            compression="none",
        )
        return Partition(
            partition_id=partition_id,
            cycle_id=cycle.cycle_id,
            run_id=run.run_id,
            schema_version="fixture-v1",
            pipeline_version=run.pipeline_version,
            status="fetched",
            fetched_at=NOW,
            artifacts=(artifact,),
            counts=Counts(raw=len(self.values)),
        )

    def parse(self, part: Partition, store: ArtifactStore) -> Iterator[Mapping[str, object]]:
        with payload(store, part.artifacts[0]) as stream:
            yield from json.load(stream)


def rows(count: int) -> list[dict[str, Any]]:
    return [{"id": str(i), "name": "Invented"} for i in range(count)]


def setup(tmp_path: Path) -> tuple[Collector, AcceptanceService, Registry]:
    store = LocalArtifactStore(tmp_path / "objects")
    registry = Registry(SQLiteStateBackend(tmp_path / "registry.sqlite"))
    gate = AcceptanceService(store)
    return Collector(store, registry, acceptance=gate), gate, registry


RULES = ValidationRules(
    version="fixture-v1", identity_field="id", required_fields=("name",), max_drop_fraction=1
)


def baseline(collector: Collector, gate: AcceptanceService, count: int) -> Any:
    held = collector.collect(cycle(10), Adapter(rows(count)), RULES, pipeline_version="fixture-v1")
    assert held.run.status == Stage.REJECTED
    gate.record(
        held.cycle,
        held.partitions,
        reviewer="fixture inspector",
        reviewed_at=NOW,
        decision="approve",
        reason="Inspected source structure, scope, counts and retained evidence",
    )
    return collector.collect(cycle(10), Adapter(rows(count)), RULES, pipeline_version="fixture-v1")


def test_first_baseline_requires_evidence_bound_review_without_refetch(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    collector, gate, registry = setup(tmp_path)
    held = collector.collect(cycle(11), Adapter(rows(3)), RULES, pipeline_version="fixture-v1")
    assert held.run.status == Stage.REJECTED and registry.current(held.cycle) is None
    original = held.partitions[0].artifacts[0]
    gate.record(
        held.cycle,
        held.partitions,
        reviewer="inspector",
        reviewed_at=NOW,
        decision="approve",
        reason="Three synthetic rows inspected",
    )
    monkeypatch.setattr(
        Adapter, "fetch", lambda *args: pytest.fail("Review resume must reuse retained artifacts")
    )
    accepted = collector.collect(cycle(11), Adapter(rows(3)), RULES, pipeline_version="fixture-v1")
    assert accepted.run.status == Stage.ACCEPTED and accepted.partitions[0].artifacts[0] == original
    assert accepted.partitions[0].fetched_at == NOW


@pytest.mark.parametrize(
    "before,after,held",
    [
        (100, 119, False),
        (100, 120, True),
        (100, 80, True),
        (99, 80, False),
        (200, 170, False),
        (0, 19, False),
        (0, 20, True),
    ],
)
def test_anomaly_requires_both_absolute_and_relative_thresholds(
    tmp_path: Path, before: int, after: int, held: bool
) -> None:
    collector, gate, registry = setup(tmp_path)
    previous = baseline(collector, gate, before)
    result = collector.collect(
        cycle(11), Adapter(rows(after)), RULES, pipeline_version="fixture-v1"
    )
    assert (result.run.status == Stage.REJECTED) == held
    assert registry.current(result.cycle) == (previous if held else result)


def test_reviewed_anomaly_promotes_only_identical_artifacts(tmp_path: Path) -> None:
    collector, gate, registry = setup(tmp_path)
    baseline(collector, gate, 100)
    held = collector.collect(cycle(11), Adapter(rows(120)), RULES, pipeline_version="fixture-v1")
    gate.record(
        held.cycle,
        held.partitions,
        reviewer="inspector",
        reviewed_at=NOW,
        decision="approve",
        reason="Explained twenty additional source records",
    )
    accepted = collector.collect(
        cycle(11), Adapter(rows(120)), RULES, pipeline_version="fixture-v1"
    )
    assert accepted.run.status == Stage.ACCEPTED and registry.current(accepted.cycle) == accepted


def test_approval_cannot_bypass_duplicate_source_identity(tmp_path: Path) -> None:
    collector, gate, registry = setup(tmp_path)
    previous = baseline(collector, gate, 100)
    held = collector.collect(
        cycle(11),
        Adapter([{"id": "duplicate", "name": "A"}, {"id": "duplicate", "name": "B"}]),
        RULES,
        pipeline_version="fixture-v1",
    )
    gate.record(
        held.cycle,
        held.partitions,
        reviewer="inspector",
        reviewed_at=NOW,
        decision="approve",
        reason="Attempted structural exception",
    )
    result = collector.collect(cycle(11), Adapter(rows(2)), RULES, pipeline_version="fixture-v1")
    assert result.run.status == Stage.REJECTED and registry.current(result.cycle) == previous
    assert any(c.code == "duplicates" and c.outcome == "failed" for c in result.validation.checks)


def test_review_is_immutable_and_not_valid_for_another_cycle(tmp_path: Path) -> None:
    collector, gate, _ = setup(tmp_path)
    held = collector.collect(cycle(11), Adapter(rows(1)), RULES, pipeline_version="fixture-v1")
    gate.record(
        held.cycle,
        held.partitions,
        reviewer="inspector",
        reviewed_at=NOW,
        decision="reject",
        reason="Needs correction",
    )
    assert not gate.reviewed(held.cycle, held.partitions)
    with pytest.raises(Conflict):
        gate.record(
            held.cycle,
            held.partitions,
            reviewer="other",
            reviewed_at=NOW,
            decision="approve",
            reason="Cannot rewrite original review",
        )
    assert not gate.reviewed(cycle(12), held.partitions)


def test_exact_exclusion_multiplicity_keeps_counts_without_aggregating_accepted_rows(
    tmp_path: Path,
) -> None:
    rules = ValidationRules(
        version="weights-v1",
        identity_field="id",
        required_fields=("name",),
        eligible=lambda r: r.get("included") is True,
        multiplicity_field="count",
    )
    validator = Validator(rules, tmp_path / "ids.sqlite")
    try:
        assert not validator.consume(
            {"id": "excluded", "name": "excluded", "included": False, "count": 5000000}
        )
        assert validator.consume(
            {"id": "00000001", "name": "Included", "included": True, "count": 1}
        )
        assert validator.count == 5000001 and validator.reasons["eligibility"] == 5000000
        with pytest.raises(ValueError, match="Accepted records cannot be aggregated"):
            validator.consume({"id": "bad", "name": "bad", "included": True, "count": 2})
    finally:
        validator.close()

"""Monthly identity, calendar edges and preservation of pre-policy source evidence."""

from datetime import UTC, date, datetime
from pathlib import Path
from unittest.mock import Mock

import pytest

from urbanlens.ingestion.cadence import legacy_month_snapshot, month_window
from urbanlens.ingestion.contracts import Snapshot
from urbanlens.ingestion.fsa import FsaConfig
from urbanlens.ingestion.osm import OsmConfig
from urbanlens.ingestion.registry import Registry


@pytest.mark.parametrize(
    "day,start,end",
    [
        (date(2026, 11, 30), datetime(2026, 11, 1, tzinfo=UTC), datetime(2026, 12, 1, tzinfo=UTC)),
        (date(2026, 12, 31), datetime(2026, 12, 1, tzinfo=UTC), datetime(2027, 1, 1, tzinfo=UTC)),
        (date(2028, 2, 29), datetime(2028, 2, 1, tzinfo=UTC), datetime(2028, 3, 1, tzinfo=UTC)),
    ],
)
def test_calendar_month_windows(day: date, start: datetime, end: datetime) -> None:
    assert month_window(day) == (start, end)


@pytest.mark.parametrize("provider", ["fsa", "osm"])
def test_dates_in_one_month_share_cycle_but_next_month_does_not(provider: str) -> None:
    config = (
        FsaConfig()
        if provider == "fsa"
        else OsmConfig(boundary_sha256="a" * 64, template_sha256="b" * 64)
    )
    first = config.cycle(date(2026, 11, 4))
    assert first == config.cycle(date(2026, 11, 30))
    assert first.cycle_id != config.cycle(date(2026, 12, 1)).cycle_id
    assert first.window_start == datetime(2026, 11, 1, tzinfo=UTC)
    assert first.window_end == datetime(2026, 12, 1, tzinfo=UTC)


def test_legacy_observation_reused_only_in_its_month_with_identical_configuration() -> None:
    path = Path(__file__).parents[1] / "reference/ingestion/examples/fsa-complete.json"
    snapshot = Snapshot.model_validate_json(path.read_text(encoding="utf-8"))
    original = snapshot.model_dump_json()
    registry = Mock(spec=Registry)
    registry.current.return_value = snapshot
    config = FsaConfig(min_records=0)
    assert (
        legacy_month_snapshot(
            registry, config.cycle(date(2026, 11, 20)), config, "fsa-authority-v1"
        )
        == snapshot
    )
    assert snapshot.model_dump_json() == original
    assert (
        legacy_month_snapshot(registry, config.cycle(date(2026, 12, 1)), config, "fsa-authority-v1")
        is None
    )
    changed = FsaConfig(min_records=10)
    assert (
        legacy_month_snapshot(
            registry, changed.cycle(date(2026, 11, 20)), changed, "fsa-authority-v1"
        )
        is None
    )
    registry.update.assert_not_called()

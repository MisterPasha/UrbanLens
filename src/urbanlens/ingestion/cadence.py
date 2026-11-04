"""Calendar-month collection windows and reuse of accepted pre-policy observations."""

import hashlib
from datetime import UTC, date, datetime

from urbanlens.ingestion.contracts import CollectionCycle, Contract, Snapshot
from urbanlens.ingestion.registry import Registry


def month_window(day: date) -> tuple[datetime, datetime]:
    start = datetime(day.year, day.month, 1, tzinfo=UTC)
    end = datetime(day.year + (day.month == 12), day.month % 12 + 1, 1, tzinfo=UTC)
    return start, end


def legacy_month_snapshot(
    registry: Registry, cycle: CollectionCycle, config: Contract, legacy_version: str
) -> Snapshot | None:
    """Reuse an exact prior configuration collected within this month, without rewriting it.

    Only the cadence version may differ. Changed scope, query pins or validation limits
    cannot silently reuse an incompatible observation.
    """
    current = registry.current(cycle)
    old_config = type(config).model_validate({**config.model_dump(), "version": legacy_version})
    old_digest = hashlib.sha256(old_config.model_dump_json().encode()).hexdigest()
    if (
        current is not None
        and current.cycle.query_version == legacy_version
        and current.cycle.query_sha256 == old_digest
        and cycle.window_start <= current.cycle.window_start
        and current.cycle.window_end <= cycle.window_end
    ):
        return current
    return None

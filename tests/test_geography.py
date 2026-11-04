"""Reject geography inputs that would corrupt membership or demographic denominators."""

import csv
import io
from decimal import Decimal
from typing import Any

import pytest

from scripts.geography import LAD, POP_FIELDS, SAM_FIELDS, SAM_LAND, land_rows, population_bands


def sam_csv(**changes: str) -> bytes:
    row = dict(
        zip(
            SAM_FIELDS,
            ["E01016852", "Example", "130", "125", "5", "120", LAD, "Example", ""],
            strict=True,
        )
    )
    row.update(changes)
    stream = io.StringIO()
    writer = csv.DictWriter(stream, fieldnames=list(row))
    writer.writeheader()
    writer.writerow(row)
    return stream.getvalue().encode("cp1252")


def test_official_land_excludes_inland_water() -> None:
    rows, _ = land_rows(sam_csv(), {"E01016852"})
    assert rows["E01016852"] / 100 == Decimal("1.2")


@pytest.mark.parametrize("change", [{SAM_LAND: "125"}, {"LTLA22CD": "E06000001"}, {SAM_LAND: "0"}])
def test_reject_inconsistent_land_or_authority(change: dict[str, str]) -> None:
    with pytest.raises(ValueError):
        land_rows(sam_csv(**change), {"E01016852"})


def test_reject_unreviewed_land_header_and_missing_area() -> None:
    with pytest.raises(ValueError, match="SAM schema differs"):
        land_rows(sam_csv().replace(SAM_LAND.encode(), b"Unknown land field"), {"E01016852"})
    with pytest.raises(ValueError, match="code set mismatch"):
        land_rows(sam_csv(), {"E01016852", "E01016853"})


@pytest.mark.parametrize("value", [11, None, -1, 10.5, True])
def test_reject_unreconciled_or_invalid_population(value: Any) -> None:
    row: dict[str, Any] = dict.fromkeys(POP_FIELDS[5:], 1)
    row["Total"] = value
    with pytest.raises(ValueError):
        population_bands(row)

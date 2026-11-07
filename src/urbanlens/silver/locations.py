"""Conservative location selection; coordinate usability never verifies a storefront."""

from __future__ import annotations

import math
from datetime import date
from typing import Literal

from pydantic import Field

from urbanlens.ingestion.contracts import Contract


class LocationReview(Contract):
    provider: Literal["fsa", "osm"]
    scope: str = Field(min_length=1)
    source_key: str = Field(min_length=1)
    expected_record_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    longitude: float
    latitude: float
    evidence: str = Field(min_length=1)
    reason: str = Field(min_length=1)
    reviewed_on: date


class Candidate(Contract):
    longitude: float
    latitude: float
    srid: int = 4326
    kind: Literal["source", "reviewed", "approximate"] = "source"
    evidence: str = Field(min_length=1)

    @property
    def valid(self) -> bool:
        return (
            self.srid == 4326
            and math.isfinite(self.longitude)
            and math.isfinite(self.latitude)
            and -180 <= self.longitude <= 180
            and -90 <= self.latitude <= 90
            and (self.longitude, self.latitude) != (0, 0)
        )


class Selection(Contract):
    quality: Literal[
        "source", "reviewed", "approximate", "missing", "invalid", "unresolved", "withheld"
    ]
    selected: Candidate | None = None
    alternatives: tuple[Candidate, ...] = ()
    reasons: tuple[str, ...]

    @property
    def precise(self) -> bool:
        return self.quality in {"source", "reviewed"}


def select(
    candidates: tuple[Candidate, ...], *, withheld: bool = False, reviewed_index: int | None = None
) -> Selection:
    if withheld:
        # Do not reconstruct or retain alternative coordinates for withheld premises.
        return Selection(quality="withheld", reasons=("published_address_withheld",))
    if reviewed_index is not None:
        if not 0 <= reviewed_index < len(candidates):
            raise ValueError("Review selection outside candidate list")
        chosen = candidates[reviewed_index]
        if not chosen.valid or chosen.kind != "reviewed":
            raise ValueError("Reviewed coordinate needs valid WGS84 and review evidence")
        return Selection(
            quality="reviewed",
            selected=chosen,
            alternatives=candidates,
            reasons=("explicit_evidence_bound_review",),
        )
    usable = tuple(c for c in candidates if c.valid and c.kind == "source")
    distinct = {(c.longitude, c.latitude) for c in usable}
    if len(distinct) > 1:
        return Selection(
            quality="unresolved", alternatives=candidates, reasons=("conflicting_precise_points",)
        )
    if usable:
        return Selection(
            quality="source",
            selected=usable[0],
            alternatives=candidates,
            reasons=("source_provided_not_storefront_verified",),
        )
    fallback = tuple(c for c in candidates if c.valid and c.kind == "approximate")
    if fallback:
        return Selection(
            quality="approximate",
            selected=fallback[0],
            alternatives=candidates,
            reasons=("postcode_centroid_not_premises",),
        )
    return Selection(
        quality="invalid" if candidates else "missing",
        alternatives=candidates,
        reasons=("no_valid_candidate" if candidates else "no_coordinate_evidence",),
    )

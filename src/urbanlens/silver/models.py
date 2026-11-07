"""Portable observation contracts shared by all five providers."""

from __future__ import annotations

import hashlib
import json
import re
from datetime import date, datetime
from typing import Any, Literal, Self
from uuid import UUID

from pydantic import Field, model_validator

from urbanlens.ingestion.contracts import Artifact, Contract, Digest, Provider

VERSION: Literal["silver-normalisation-v1"] = "silver-normalisation-v1"
State = Literal["published", "missing", "null", "withheld", "invalid", "unavailable"]


class Text(Contract):
    raw: str | None = None
    value: str | None = None
    state: State = "missing"


class DateValue(Contract):
    raw: str | None = None
    value: date | None = None
    state: State = "missing"


class Address(Contract):
    raw: dict[str, Any] | None = None
    value: str | None = None
    state: State = "missing"


class Location(Contract):
    longitude: float | None = Field(default=None, ge=-180, le=180, allow_inf_nan=False)
    latitude: float | None = Field(default=None, ge=-90, le=90, allow_inf_nan=False)
    srid: Literal[4326] = 4326
    quality: Literal[
        "source_point",
        "point_on_surface",
        "postcode_fallback",
        "missing",
        "invalid",
        "withheld",
        "unresolved",
    ] = "missing"
    role: Literal["premises", "registered_office", "postcode_centroid", "none"] = "none"
    # This is location quality only, not a decision that a record is an eligible venue.
    primary_spatial_eligible: bool = False

    @model_validator(mode="after")
    def coordinates_and_quality(self) -> Self:
        point = self.longitude is not None and self.latitude is not None
        if (self.longitude is None) != (self.latitude is None):
            raise ValueError("Coordinates must be a pair")
        if point != (self.quality in {"source_point", "point_on_surface", "postcode_fallback"}):
            raise ValueError("Coordinate presence and quality disagree")
        if self.primary_spatial_eligible and (
            not point
            or self.role != "premises"
            or self.quality not in {"source_point", "point_on_surface"}
        ):
            raise ValueError("Approximate/non-premises points cannot enter primary metrics")
        return self


class Rating(Contract):
    raw: str | None = None
    score: int | None = Field(default=None, ge=0, le=5)
    state: Literal[
        "rated", "awaiting_inspection", "awaiting_publication", "exempt", "unknown", "missing"
    ]
    assessed_on: DateValue


class Company(Contract):
    legal_status: Text
    company_type: Text
    sic_codes: tuple[str, ...]
    sic_state: Literal["published", "missing", "invalid"] = "published"
    incorporated_on: DateValue
    ceased_on: DateValue
    registered_office_cohort: bool = False
    profile: bool = False


class Area(Contract):
    authority_code: str
    geography_version: str
    population_edition: str
    reference_date: date
    population: int = Field(ge=0)
    age_0_15: int = Field(ge=0)
    age_16_64: int = Field(ge=0)
    age_65_plus: int = Field(ge=0)
    official_land_km2: str
    geometry: dict[str, Any]
    editions: dict[str, Any]

    @model_validator(mode="after")
    def reconciled(self) -> Self:
        if self.age_0_15 + self.age_16_64 + self.age_65_plus != self.population:
            raise ValueError("Age bands must reconcile")
        from decimal import Decimal

        if not Decimal(self.official_land_km2).is_finite() or Decimal(self.official_land_km2) <= 0:
            raise ValueError("Land area must be positive")
        return self


class Postcode(Contract):
    state: Literal[
        "invalid",
        "unmatched",
        "outside",
        "incompatible-geography",
        "resolved",
        "throttled",
        "unavailable",
    ]
    authority_code: str | None = None
    lsoa21: str | None = None
    quality: int | None = Field(default=None, ge=1, le=9)
    dataset_version: str | None = None
    edition_note: str
    expires_at: datetime | None = None


class Evidence(Contract):
    artifact: Artifact
    locator: str = Field(min_length=1)


class Observation(Contract):
    schema_version: Literal["silver-observation-v1"] = "silver-observation-v1"
    normalisation_version: Literal["silver-normalisation-v1"] = VERSION
    provider: Provider
    source_key: str
    record_kind: Literal["venue", "company", "area", "postcode"]
    scope: str
    snapshot_id: UUID | None
    cycle_id: UUID | None
    observed_at: datetime
    ingested_at: datetime | None = None
    source_published_at: datetime | None = None
    source_extracted_at: datetime | None = None
    source_extract_date: date | None = None
    source_event_at: datetime | None = None
    name: Text = Field(default_factory=Text)
    address: Address = Field(default_factory=Address)
    postcode: Text = Field(default_factory=Text)
    location: Location = Field(default_factory=Location)
    rating: Rating | None = None
    company: Company | None = None
    area: Area | None = None
    postcode_result: Postcode | None = None
    tags_raw: dict[str, str] = Field(default_factory=dict)
    tags: dict[str, tuple[str, ...]] = Field(default_factory=dict)
    source_values: dict[str, Any] = Field(default_factory=dict)
    evidence: tuple[Evidence, ...] = Field(min_length=1)
    # Paths are relative to the raw evidence locator(s), not to an undocumented join.
    field_sources: dict[str, str]
    record_hash: Digest

    @model_validator(mode="after")
    def domains_and_privacy(self) -> Self:
        patterns = {
            Provider.FSA: r"[0-9]*[1-9][0-9]*",
            Provider.OSM: r"(?:node|way|relation)/[1-9][0-9]*",
            Provider.COMPANIES_HOUSE: r"[A-Z0-9]{8}",
            Provider.ONS: r"E01[0-9]{6}",
        }
        if self.provider in patterns and not re.fullmatch(patterns[self.provider], self.source_key):
            raise ValueError("Identifier is outside its provider domain")
        expected = {
            Provider.FSA: "venue",
            Provider.OSM: "venue",
            Provider.COMPANIES_HOUSE: "company",
            Provider.ONS: "area",
            Provider.POSTCODES: "postcode",
        }[self.provider]
        if self.record_kind != expected:
            raise ValueError("Provider and record kind disagree")
        if (
            (self.company is not None) != (expected == "company")
            or (self.area is not None) != (expected == "area")
            or (self.postcode_result is not None) != (expected == "postcode")
        ):
            raise ValueError("Typed provider details are required")
        if (self.snapshot_id is None) != (self.cycle_id is None):
            raise ValueError("Snapshot and collection cycle must be supplied together")
        if self.provider != Provider.POSTCODES and self.snapshot_id is None:
            raise ValueError("Source observations require their accepted snapshot")
        if self.address.state == "withheld" and (
            self.address.value is not None
            or self.address.raw is not None
            or self.postcode.raw is not None
            or self.postcode.value is not None
            or self.location.quality != "withheld"
        ):
            raise ValueError("Withheld premises details must remain suppressed")
        if self.record_hash != semantic_hash(self.model_dump(mode="json")):
            raise ValueError("Semantic record hash mismatch")
        return self


def semantic_hash(values: dict[str, Any]) -> str:
    """Hash interpreted content, not transport, processing or observation metadata."""

    def meaning(value: Any) -> Any:
        if isinstance(value, dict):
            return {
                k: meaning(v)
                for k, v in value.items()
                if (k != "raw" or value.get("state") in {"invalid", "unknown"})
                and k != "expires_at"
            }
        if isinstance(value, (list, tuple)):
            return [meaning(v) for v in value]
        return value

    fields = (
        "provider",
        "source_key",
        "record_kind",
        "name",
        "address",
        "postcode",
        "location",
        "rating",
        "company",
        "area",
        "postcode_result",
        "tags",
        "source_values",
    )
    content = {k: meaning(values[k]) for k in fields}
    return hashlib.sha256(
        json.dumps(
            content, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False
        ).encode()
    ).hexdigest()


def observation(**values: Any) -> Observation:
    # Fill defaults through construction without validation only to compute the hash;
    # the returned object always passes the complete contract and hash checks.
    values["provider"] = Provider(values["provider"])
    complete = Observation.model_construct(**values).model_dump(mode="json")
    return Observation.model_validate({**complete, "record_hash": semantic_hash(complete)})

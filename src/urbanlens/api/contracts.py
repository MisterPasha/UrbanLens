"""Versioned public contracts and bounded query models."""

from __future__ import annotations

from datetime import date, datetime
from typing import Any, Literal, Self
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, model_validator

Category = Literal["cafe", "restaurant", "takeaway", "bakery", "pub_bar", "food_service"]
Radius = Literal[250, 500, 1000, 2000]


class Contract(BaseModel):
    model_config = ConfigDict(extra="forbid", allow_inf_nan=False)


class PinQuery(Contract):
    release_id: UUID | None = None


class ExploreQuery(PinQuery):
    mode: Literal["city", "area", "radius", "bbox"] = "city"
    categories: list[Category] | None = Field(default=None, max_length=6)
    empty_categories: bool = False
    area_code: str | None = Field(default=None, pattern=r"^E01[0-9]{6}$")
    longitude: float | None = Field(default=None, ge=-180, le=180)
    latitude: float | None = Field(default=None, ge=-90, le=90)
    radius: int | None = Field(
        default=None, json_schema_extra={"enum": [250, 500, 1000, 2000, None]}
    )
    bbox: list[float] | None = Field(default=None, min_length=4, max_length=4)
    limit: int = Field(default=50, ge=1, le=100)
    map_limit: int = Field(default=500, ge=1, le=1000)
    cursor: str | None = Field(default=None, max_length=2048)

    @model_validator(mode="after")
    def scope(self) -> Self:
        if self.radius is not None and self.radius not in (250, 500, 1000, 2000):
            raise ValueError("Choose a supported radius")
        if self.empty_categories:
            if self.categories:
                raise ValueError("Choose categories or an empty selection")
            self.categories = []
        if self.categories is not None:
            self.categories = sorted(set(self.categories))
        if self.mode == "radius":
            if self.longitude is None or self.latitude is None:
                raise ValueError("Radius mode needs a centre")
            if self.area_code is not None or self.bbox is not None:
                raise ValueError("Choose one spatial mode")
            self.radius = self.radius or 500
        elif self.longitude is not None or self.latitude is not None or self.radius is not None:
            raise ValueError("Coordinates/radius require radius mode")
        if (self.mode == "area") != (self.area_code is not None):
            raise ValueError("Area mode needs an area")
        if (self.mode == "bbox") != (self.bbox is not None):
            raise ValueError("Viewport mode needs a bbox")
        if self.bbox:
            a, b, c, d = self.bbox
            if not (-180 <= a < c <= 180 and -90 <= b < d <= 90 and c - a <= 0.1 and d - b <= 0.1):
                raise ValueError("Viewport is unordered or too large")
        return self


class Metadata(Contract):
    published_at: datetime
    geography_version: str
    population_edition: str
    population_reference_date: str
    taxonomy_version: str
    sources: list[dict[str, Any]]
    attribution: list[str]


class Envelope[T](Contract):
    api_version: Literal["v1"] = "v1"
    request_id: str = ""
    release_id: UUID
    applied_filters: dict[str, Any]
    metadata: Metadata
    coverage: list[dict[str, Any]]
    null_reasons: dict[str, str | None]
    data: T


class Meta(Contract):
    categories: list[Category]
    radii: list[Radius]
    default_radius: Radius
    city: str
    industry: str


class Area(Contract):
    area_code: str
    area_name: str
    geography_version: str
    display_geometry: dict[str, Any]


class Venue(Contract):
    establishment_id: UUID
    display_name: str | None
    name_unavailable: bool
    primary_category: Category
    secondary_categories: list[str]
    address: str | None
    postcode: str | None
    location_quality: str
    area_code: str
    observation_state: str
    last_observed_at: datetime
    source_mix: str
    longitude: float
    latitude: float


class MapPoint(Contract):
    establishment_id: UUID
    primary_category: Category
    longitude: float
    latitude: float


class Exploration(Contract):
    total: int
    items: list[Venue]
    has_more: bool
    next_cursor: str | None = None
    map_points: list[MapPoint]
    map_truncated: bool
    category_mix: dict[Category, int]
    partial_coverage: bool
    unknown_location_scope: str


class VerifiedCompany(Contract):
    company_number: str = Field(pattern=r"^[A-Z0-9]{8}$")
    relationship: Literal["operator", "franchisee"]
    reviewed_at: datetime | None
    reviewed_on: str | None
    url: str


class VenueDetail(Contract):
    establishment_id: UUID
    display_name: str | None
    name_unavailable: bool
    address: str | None
    postcode: str | None
    primary_category: Category | None
    secondary_categories: list[str]
    eligibility: str
    duplicate_status: str
    observation_state: str
    primary_spatial_eligible: bool
    location_quality: str
    longitude: float | None
    latitude: float | None
    area_code: str | None
    first_observed_at: datetime
    last_observed_at: datetime
    source_mix: str
    exclusion_reasons: list[str]
    hygiene: list[dict[str, Any]]
    null_reasons: dict[str, str | None]
    sources: list[dict[str, Any]]
    provenance: dict[str, dict[str, Any]]
    verified_companies: list[VerifiedCompany]


class Postcode(Contract):
    postcode: str
    state: Literal[
        "resolved",
        "unmatched",
        "outside",
        "incompatible_geography",
        "provider_failure",
        "throttled",
    ]
    can_define_search: bool = False
    longitude: float | None = None
    latitude: float | None = None
    area_code: str | None = None
    positional_quality: int | None = None
    location_role: Literal["search-centre"] = "search-centre"
    approximate: Literal[True] = True
    fetched_at: datetime | None = None
    expires_at: datetime | None = None
    edition_note: str = (
        "Lookup time is not a dataset edition; coordinates describe an approximate postcode centre"
    )
    cache_hit: bool = False


class ErrorInfo(Contract):
    code: str
    message: str
    request_id: str
    refresh_required: bool = False


class ErrorResponse(Contract):
    error: ErrorInfo


class SummaryQuery(PinQuery):
    categories: list[Category] | None = Field(default=None, max_length=6)
    empty_categories: bool = False

    @model_validator(mode="after")
    def selection(self) -> Self:
        if self.empty_categories and self.categories:
            raise ValueError("Choose categories or an empty selection")
        if self.empty_categories:
            self.categories = []
        elif self.categories is not None:
            self.categories = sorted(set(self.categories))
        return self


class CompareQuery(SummaryQuery):
    area_codes: list[str] = Field(min_length=2, max_length=2)

    @model_validator(mode="after")
    def distinct_areas(self) -> Self:
        import re

        if len(set(self.area_codes)) != 2 or any(
            not re.fullmatch(r"E01[0-9]{6}", code) for code in self.area_codes
        ):
            raise ValueError("Choose two distinct supported areas")
        return self


class AreaMetrics(Contract):
    area_code: str | None
    area_name: str
    geography_version: str
    population_edition: str
    population_reference_date: date
    land_area_basis: str
    land_area_edition: dict[str, Any]
    establishment_count: int
    official_land_km2: float | None
    population: int | None
    establishments_per_land_km2: float | None
    land_rate_unavailable_reason: str | None
    establishments_per_1000_residents: float | None
    resident_rate_unavailable_reason: str | None
    population_per_land_km2: float | None
    population_density_unavailable_reason: str | None
    age_0_15: int | None
    age_16_64: int | None
    age_65_plus: int | None
    age_0_15_percent: float | None
    age_16_64_percent: float | None
    age_65_plus_percent: float | None
    age_mix_unavailable_reason: str | None
    category_mix: dict[Category, int]


class AreaSummary(Contract):
    area: AreaMetrics
    city_benchmark: AreaMetrics


class Comparison(Contract):
    areas: list[AreaMetrics]
    city_benchmark: AreaMetrics


class PageQuery(PinQuery):
    limit: int = Field(default=50, ge=1, le=100)
    cursor: str | None = Field(default=None, max_length=2048)


class CompaniesQuery(PageQuery):
    cohort: Literal["registered_office", "verified_operator"]
    legal_status: str | None = Field(
        default=None, min_length=1, max_length=40, pattern=r"^[a-z-]+$"
    )
    sic_code: str | None = Field(default=None, pattern=r"^[0-9]{5}$")
    name: str | None = Field(default=None, min_length=2, max_length=80)


class Company(Contract):
    company_number: str = Field(pattern=r"^[A-Z0-9]{8}$")
    legal_name: str
    legal_status: str
    company_type: str
    sic_codes: list[str]
    incorporation_date: date | None
    cessation_date: date | None
    registered_office: dict[str, Any] | None
    registered_office_postcode: str | None
    cohorts: list[str]
    office_caveat: str
    first_observed_at: datetime
    last_observed_at: datetime
    profile_observed_at: datetime | None
    url: str


class PremisesLink(Contract):
    establishment_id: UUID
    display_name: str | None
    relationship: Literal["operator", "franchisee"]
    reviewed_at: datetime | None
    reviewed_on: date | None
    evidence_url: str | None
    primary_spatial_eligible: bool


class CompanyDetail(Company):
    verified_premises: list[PremisesLink]


class CompaniesPage(Contract):
    total: int
    items: list[Company]
    has_more: bool
    next_cursor: str | None = None
    interpretation: str


EntityType = Literal["establishment", "company", "source"]
EventType = Literal[
    "newly_observed",
    "no_longer_observed",
    "reappeared",
    "source_changed",
    "company_incorporated",
    "company_dissolved",
    "processing_correction",
]


class ActivityQuery(PageQuery):
    entity_type: EntityType | None = None
    event_type: EventType | None = None


class ActivityEvent(Contract):
    event_id: UUID
    entity_type: EntityType
    establishment_id: UUID | None
    company_number: str | None
    event_type: EventType
    observed_at: datetime
    source_event_date: date | None
    before_value: dict[str, Any] | None
    after_value: dict[str, Any] | None
    evidence: dict[str, Any]
    rule_version: str
    baseline_established_at: datetime | None
    interpretation: str


class ActivityBaseline(Contract):
    entity_type: Literal["establishment", "company"]
    established_at: datetime | None
    retained_event_count: int | None
    state: Literal["baseline_unavailable", "no_observed_changes", "observed_changes"]
    retention_boundary: datetime
    baseline_source_scopes: int


class ActivityPage(Contract):
    total: int
    items: list[ActivityEvent]
    has_more: bool
    next_cursor: str | None = None
    baselines: list[ActivityBaseline]
    interpretation: str
    context_note: str

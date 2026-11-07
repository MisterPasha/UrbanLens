"""Evidence-based eligibility and taxonomy, independent of location and identity matching."""

from __future__ import annotations

import hashlib
import json
from datetime import date
from typing import Any, Literal, Self

from pydantic import Field, model_validator

from urbanlens.ingestion.contracts import Contract, Digest, Provider
from urbanlens.silver.models import Evidence, Observation

Category = Literal["cafe", "restaurant", "takeaway", "bakery", "pub_bar", "food_service"]
Eligibility = Literal["eligible", "excluded", "unresolved"]


def digest(value: Any) -> str:
    return hashlib.sha256(
        json.dumps(
            value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False
        ).encode()
    ).hexdigest()


class Selection(Contract):
    eligibility: Eligibility
    primary_category: Category | None = None
    category_state: Literal["specific", "broad", "ambiguous", "unresolved", "not_applicable"]
    candidate_categories: tuple[Category, ...] = ()
    rules: tuple[str, ...] = Field(min_length=1)
    reasons: tuple[str, ...] = Field(min_length=1)
    conflicts: tuple[str, ...] = ()
    review_required: bool = False

    @model_validator(mode="after")
    def consistent(self) -> Self:
        if (self.eligibility == "eligible") != (self.primary_category is not None):
            raise ValueError("Only eligible evidence can select a primary category")
        if self.primary_category == "food_service" and self.category_state not in {
            "broad",
            "ambiguous",
        }:
            raise ValueError("Broad classification must remain visibly broad/ambiguous")
        if (
            self.primary_category not in {None, "food_service"}
            and self.category_state != "specific"
        ):
            raise ValueError("Fine category must have specific evidence")
        if self.eligibility == "unresolved" and not self.review_required:
            raise ValueError("Unresolved eligibility requires review")
        return self


class FsaRule(Contract):
    eligibility: Eligibility
    primary_category: Category | None = None
    reason: str = Field(min_length=1)
    candidates: tuple[Category, ...] = ()


class Taxonomy(Contract):
    version: str
    categories: tuple[Category, ...]
    osm_categories: dict[str, dict[str, Category]]
    osm_broad: tuple[str, ...]
    osm_institutions: tuple[str, ...]
    osm_retail: tuple[str, ...]
    osm_accommodation: tuple[str, ...]
    osm_production: dict[str, tuple[str, ...]]
    private_access: tuple[str, ...]
    public_access: tuple[str, ...]
    lifecycle_prefixes: tuple[str, ...]
    fsa_rules: dict[str, FsaRule]

    @model_validator(mode="after")
    def complete_and_safe(self) -> Self:
        if (
            set(self.categories)
            != {"cafe", "restaurant", "takeaway", "bakery", "pub_bar", "food_service"}
            or len(self.categories) != 6
        ):
            raise ValueError("Taxonomy must declare the six supported categories exactly once")
        if set(self.private_access) & set(self.public_access):
            raise ValueError("Public/private access rules conflict")
        for rule in self.fsa_rules.values():
            if (rule.eligibility == "eligible") != (rule.primary_category is not None):
                raise ValueError("Eligible FSA rules require a supported category")
        return self


class Review(Contract):
    review_id: str = Field(min_length=1)
    provider: Provider
    scope: str
    source_key: str
    expected_record_hash: Digest
    reviewer: str = Field(min_length=1)
    reviewed_on: date
    eligibility: Eligibility
    primary_category: Category | None = None
    public_venue_confirmed: bool = False
    reason: str = Field(min_length=1)
    evidence: str = Field(min_length=1)

    @model_validator(mode="after")
    def explicit_evidence(self) -> Self:
        if self.provider not in {Provider.FSA, Provider.OSM}:
            raise ValueError("Company/area/postcode records cannot be reviewed into venues")
        if (self.eligibility == "eligible") != (self.primary_category is not None):
            raise ValueError("Reviewed eligibility/category disagree")
        if self.eligibility == "eligible" and not self.public_venue_confirmed:
            raise ValueError("Eligible overrides require explicit public-venue confirmation")
        if not self.reason.strip() or not self.evidence.strip() or not self.reviewer.strip():
            raise ValueError("Review needs an author, reason and evidence")
        return self


class Reviews(Contract):
    version: str
    reviews: tuple[Review, ...] = ()

    @model_validator(mode="after")
    def unique_targets(self) -> Self:
        keys = [(r.provider, r.scope, r.source_key) for r in self.reviews]
        if len(keys) != len(set(keys)) or len({r.review_id for r in self.reviews}) != len(keys):
            raise ValueError("Duplicate/contradictory review targets")
        return self


class Decision(Contract):
    engine_version: Literal["eligibility-engine-v1"] = "eligibility-engine-v1"
    schema_version: Literal["eligibility-decision-v1"] = "eligibility-decision-v1"
    provider: Provider
    scope: str
    source_key: str
    record_hash: Digest
    applicable: bool
    taxonomy_version: str
    taxonomy_sha256: Digest
    reviews_version: str
    reviews_sha256: Digest
    selection: Selection
    automatic_selection: Selection
    review_status: Literal["none", "applied", "stale"] = "none"
    review: Review | None = None
    secondary_tags: dict[str, tuple[str, ...]]
    source_evidence: tuple[Evidence, ...]
    decision_hash: Digest

    @model_validator(mode="after")
    def verified(self) -> Self:
        if self.decision_hash != digest(self.model_dump(mode="json", exclude={"decision_hash"})):
            raise ValueError("Classification hash mismatch")
        if (self.review is None) != (self.review_status == "none"):
            raise ValueError("Review evidence/status disagree")
        if not self.applicable and self.selection.eligibility != "excluded":
            raise ValueError("Non-venue evidence cannot qualify as a venue")
        return self


def selected(
    eligibility: Eligibility,
    rule: str,
    reason: str,
    *,
    categories: set[Category] | None = None,
    conflicts: tuple[str, ...] = (),
) -> Selection:
    fine = sorted((categories or set()) - {"food_service"})
    primary: Category | None = None
    state: str = "not_applicable" if eligibility == "excluded" else "unresolved"
    if eligibility == "eligible":
        primary = fine[0] if len(fine) == 1 else "food_service"
        state = "specific" if len(fine) == 1 else "ambiguous" if fine else "broad"
    return Selection.model_validate(
        dict(
            eligibility=eligibility,
            primary_category=primary,
            category_state=state,
            candidate_categories=fine,
            rules=(rule,),
            reasons=(reason,),
            conflicts=conflicts,
            review_required=eligibility == "unresolved" or bool(conflicts) or len(fine) > 1,
        )
    )


def automatic(record: Observation, config: Taxonomy) -> Selection:
    if record.record_kind != "venue":
        return selected(
            "excluded", "not-venue", "legal_company_or_reference_record_not_venue_evidence"
        )
    if record.provider == Provider.FSA:
        code = str(record.source_values.get("BusinessTypeID") or "").strip()
        rule = config.fsa_rules.get(code)
        if rule is None:
            return selected("unresolved", "fsa-unknown-type", "unknown_fsa_type_requires_review")
        result = selected(
            rule.eligibility,
            "fsa-type-" + code,
            rule.reason,
            categories=set(rule.candidates)
            if rule.primary_category is None
            else {rule.primary_category},
        )
        return result
    tags = {k: set(v.casefold() for v in values) for k, values in record.tags.items()}

    def get(key: str) -> set[str]:
        return tags.get(key, set())

    fine: set[Category] = {
        category
        for key, mapping in config.osm_categories.items()
        for value, category in mapping.items()
        if value in get(key)
    }
    broad = bool(get("amenity") & set(config.osm_broad))
    active = bool(fine) or broad
    institutional = bool(get("amenity") & set(config.osm_institutions))
    host = bool(
        get("tourism") & set(config.osm_accommodation)
        or get("building") & set(config.osm_institutions)
    )
    retail = bool(get("shop") & set(config.osm_retail))
    lifecycle = any(
        k.startswith(config.lifecycle_prefixes) and k.split(":", 1)[-1] in {"amenity", "shop"}
        for k in tags
    )
    if lifecycle or get("disused") & {"yes"} or get("abandoned") & {"yes"}:
        return selected(
            "unresolved",
            "osm-lifecycle",
            "lifecycle_tags_do_not_prove_current_venue_or_verified_closure",
            categories=fine,
            conflicts=("current_and_lifecycle_tags",) if active else (),
        )
    if get("access") & set(config.private_access) and get("access") & set(config.public_access):
        return selected(
            "unresolved",
            "osm-conflicting-access",
            "conflicting_public_private_access_tags",
            categories=fine,
            conflicts=("public_private_access_conflict",),
        )
    if get("access") & set(config.private_access) or get("membership") & {"yes"}:
        return selected("excluded", "osm-private-access", "private_staff_or_members_only_access")
    public = get("access") & set(config.public_access)
    if active and get("access") and not public:
        return selected(
            "unresolved",
            "osm-unknown-access",
            "access_restriction_requires_review",
            categories=fine,
        )
    if active and (institutional or retail):
        return selected(
            "unresolved",
            "osm-mixed-feature",
            "institution_or_general_retail_and_hospitality_share_one_feature",
            categories=fine,
            conflicts=("mixed_feature_grain",),
        )
    if active and (host or "canteen" in get("amenity")) and not public:
        return selected(
            "unresolved",
            "osm-hosted-venue",
            "hosted_venue_or_canteen_needs_explicit_public_access",
            categories=fine,
        )
    if active:
        return selected(
            "eligible",
            "osm-explicit-venue",
            "specific_hospitality_tag_with_no_conflicting_scope_evidence",
            categories=fine,
            conflicts=("multiple_primary_categories",) if len(fine) > 1 else (),
        )
    if institutional:
        return selected(
            "excluded",
            "osm-institution-only",
            "institution_without_distinct_public_hospitality_evidence",
        )
    if retail:
        return selected(
            "excluded",
            "osm-retail-only",
            "general_food_or_other_retail_without_hospitality_evidence",
        )
    if host:
        return selected(
            "excluded",
            "osm-accommodation-only",
            "accommodation_or_institution_only_without_venue_evidence",
        )
    if any(get(k) & set(values) for k, values in config.osm_production.items()):
        return selected(
            "excluded", "osm-production-only", "production_without_public_shop_or_venue_evidence"
        )
    return selected(
        "unresolved", "osm-insufficient-evidence", "no_supported_public_hospitality_tag"
    )


def classify(record: Observation, config: Taxonomy, reviews: Reviews) -> Decision:
    base = automatic(record, config)
    choice = base
    applicable = record.record_kind == "venue"
    review = next(
        (
            r
            for r in reviews.reviews
            if (r.provider, r.scope, r.source_key)
            == (record.provider, record.scope, record.source_key)
        ),
        None,
    )
    status = "none"
    if review is not None:
        if review.expected_record_hash != record.record_hash:
            status = "stale"
            choice = selected(
                "unresolved",
                "stale-review",
                "source_changed_review_requires_reconfirmation",
                categories=set(base.candidate_categories),
                conflicts=("stale_owner_review",),
            )
        else:
            status = "applied"
            category = review.primary_category
            choice = selected(
                review.eligibility,
                "reviewed-override",
                review.reason,
                categories={category} if category not in {None, "food_service"} else set(),
            )
    secondary = {"osm." + k: v for k, v in record.tags.items()}
    if record.provider == Provider.FSA:
        secondary = {
            "fsa." + k: (str(record.source_values[k]),)
            for k in ("BusinessTypeID", "BusinessType")
            if record.source_values.get(k) is not None
        }
    data: dict[str, Any] = dict(
        provider=record.provider,
        scope=record.scope,
        source_key=record.source_key,
        record_hash=record.record_hash,
        applicable=applicable,
        taxonomy_version=config.version,
        taxonomy_sha256=digest(config.model_dump(mode="json")),
        reviews_version=reviews.version,
        reviews_sha256=digest(reviews.model_dump(mode="json")),
        selection=choice,
        automatic_selection=base,
        review_status=status,
        review=review,
        secondary_tags=secondary,
        source_evidence=record.evidence,
    )
    complete = Decision.model_construct(**data).model_dump(mode="json")
    return Decision.model_validate({**complete, "decision_hash": digest(complete)})


def resolve_linked(decisions: tuple[Decision, ...]) -> Selection:
    """Only call after links are accepted (step 23); this function never matches records."""
    applicable = [d.selection for d in decisions if d.applicable]
    if not applicable:
        return selected("unresolved", "linked-no-venue-evidence", "no_accepted_venue_evidence")
    eligible = [s for s in applicable if s.eligibility == "eligible"]
    excluded = [s for s in applicable if s.eligibility == "excluded"]
    conflicts = tuple(
        sorted({v for s in applicable for v in s.conflicts if v != "multiple_primary_categories"})
    )
    if conflicts or (eligible and excluded):
        return selected(
            "unresolved",
            "linked-conflict",
            "accepted_source_evidence_disagrees_on_scope",
            conflicts=conflicts or ("source_eligibility_conflict",),
        )
    if eligible:
        fine = {
            s.primary_category for s in eligible if s.primary_category not in {None, "food_service"}
        }
        # Keep explicit conflicting categories; broad evidence contributes no fine category.
        candidates = set(fine) | {c for s in eligible for c in s.candidate_categories}
        return selected(
            "eligible",
            "linked-supported",
            "accepted_source_evidence_supports_public_venue",
            categories=candidates,
            conflicts=("multiple_primary_categories",) if len(candidates) > 1 else (),
        )
    if len(excluded) == len(applicable):
        return selected(
            "excluded", "linked-excluded", "all_accepted_venue_evidence_is_out_of_scope"
        )
    return selected(
        "unresolved", "linked-insufficient", "accepted_sources_do_not_establish_public_eligibility"
    )

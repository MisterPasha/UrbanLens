"""Source-bound owner decisions, field overrides and explicit processing corrections."""

from __future__ import annotations

from collections import defaultdict
from datetime import date
from typing import Any, Literal

from pydantic import Field, model_validator

from urbanlens.ingestion.contracts import Artifact, Contract, Digest
from urbanlens.silver.eligibility import digest
from urbanlens.silver.identity import Entry, PairReview, Ref, Reviews

REVIEW_ENGINE_VERSION = "review-application-v1"


class DecisionMapping(Contract):
    review_id: str
    left: Ref
    right: Ref
    decision: Literal["same place", "different places", "unsure"]
    notes: str
    evidence_urls: tuple[str, ...]
    reviewed_on: date | None
    confirmed_on: date
    pair_id: Digest

    @model_validator(mode="after")
    def bound_pair(self) -> DecisionMapping:
        if self.reviewed_on is not None and self.reviewed_on > self.confirmed_on:
            raise ValueError("Review date is later than its confirmation")
        if self.left.key == self.right.key or self.pair_id != digest(
            sorted([self.left.key, self.right.key])
        ):
            raise ValueError("Review pair ID does not bind its two distinct source identities")
        return self


class DecisionBatch(Contract):
    version: str
    confirmed_on: date
    reviewer: str = Field(min_length=1)
    status: str
    review_file: str
    review_file_sha256: Digest
    resolution_manifest: Artifact
    hospitality_scope_policy: dict[str, Any]
    name_selection_policy: dict[str, Any]
    reviews: tuple[DecisionMapping, ...]
    replacement_reports: tuple[str, ...]
    unresolved_pairs: tuple[str, ...]


class OldLabel(Contract):
    source: Ref
    published_name: str | None
    status: Literal["owner_reports_not_current_at_this_address"]


class Replacement(Contract):
    name: str = Field(min_length=1)
    primary_category: Literal["restaurant"]
    service_style: str
    owner_original_tag_description: str
    address_from_reviewed_records: str
    postcode_from_reviewed_records: str
    longitude: None = None
    latitude: None = None
    identity_continuity: str
    opening_date: None = None


class Overrides(Contract):
    postcode: str | None = Field(default=None, pattern=r"^[A-Z0-9 ]+$")
    address: str | None = None
    preferred_postcode_source: Literal["left", "right"] | None = None
    preferred_address_source: Literal["left", "right"] | None = None
    replacement_candidate: Replacement | None = None


class Correction(Contract):
    review_id: str
    kind: Literal[
        "current_name_or_source_label",
        "superseded_source_label",
        "replacement_venue_report",
        "postcode_correction",
        "address_correction",
    ]
    owner_statement: str = Field(min_length=1)
    current_name: str | None
    current_source: Ref | None
    superseded_source_labels: tuple[OldLabel, ...]
    field_overrides: Overrides
    evidence_basis: str
    identity_decision: Literal["same place", "different places", "unsure"] | None


class CorrectionBatch(Contract):
    version: str
    received_on: date
    reviewer: str
    status: str
    source_review_manifest: Artifact
    name_selection_policy: dict[str, Any]
    corrections: tuple[Correction, ...]
    provenance_policy: str


class Application:
    def __init__(self, original: Reviews, decisions: DecisionBatch, corrections: CorrectionBatch):
        self.original, self.decisions, self.corrections = original, decisions, corrections
        self.version = REVIEW_ENGINE_VERSION + "/" + decisions.version + "/" + corrections.version
        self.definition = dict(
            engine_version=REVIEW_ENGINE_VERSION,
            original=original.model_dump(mode="json"),
            decisions=decisions.model_dump(mode="json"),
            corrections=corrections.model_dump(mode="json"),
        )
        self.sha256 = digest(self.definition)
        self.artifact: Artifact | None = None
        self.inactive: dict[str, str] = {}
        self.overrides: dict[str, dict[str, tuple[Any, str]]] = defaultdict(dict)
        self.review_ids: dict[str, Any] = {}
        self.registry_replacements: list[dict[str, Any]] = []
        if len({r.review_id for r in corrections.corrections}) != len(corrections.corrections):
            raise ValueError("Duplicate field correction identifiers")
        if decisions.resolution_manifest != corrections.source_review_manifest:
            raise ValueError("Review batches disagree on their source resolution manifest")

    def bind(
        self, entries: list[Entry], rows: dict[str, dict[str, Any]], artifact: Artifact
    ) -> Reviews:
        self.artifact = artifact
        actual = {e.key: e for e in entries}

        def check(ref: Ref) -> None:
            if ref.key not in actual or actual[ref.key].record_hash != ref.record_hash:
                raise ValueError("Stale review: source hash, scope or identity generation changed")

        all_reviews = list(self.original.reviews)
        for original_review in self.original.reviews:
            check(original_review.left)
            check(original_review.right)
        existing = {r.review_id for r in all_reviews}
        for r in self.decisions.reviews:
            check(r.left)
            check(r.right)
            if r.review_id in existing:
                raise ValueError("Duplicate review identifier")
            existing.add(r.review_id)
            all_reviews.append(
                PairReview(
                    review_id=r.review_id,
                    left=r.left,
                    right=r.right,
                    decision=r.decision,
                    reviewer=self.decisions.reviewer,
                    reviewed_on=r.reviewed_on or r.confirmed_on,
                    reason=r.notes or "Owner confirmed pair as " + r.decision,
                    evidence=artifact.uri + "#pair/" + r.review_id,
                )
            )
        scope = self.decisions.hospitality_scope_policy
        self.inactive = {}
        self.overrides = defaultdict(dict)
        for e in entries:
            source_type = rows[e.key]["source_fields"].get("source_values", {}).get("BusinessType")
            amenities = set(e.tags.get("amenity", ()))
            shops = set(e.tags.get("shop", ()))
            if "asda" in (e.name or "").casefold():
                self.inactive[e.key] = "Owner excludes Asda retail records"
            elif e.provider == "fsa" and source_type in scope.get("exclude_fsa_types", []):
                self.inactive[e.key] = "Owner excludes FSA type " + str(source_type)
            elif amenities & set(scope.get("exclude_osm_amenities", [])) or "chemist" in shops:
                self.inactive[e.key] = (
                    "Owner excludes healthcare, education or caring/retail premises"
                )
            elif shops and not (
                amenities & set(scope.get("keep_osm_amenities", [])) or "bakery" in shops
            ):
                self.inactive[e.key] = "Retail without explicit public food-service evidence"
            elif amenities and not (
                amenities & set(scope.get("keep_osm_amenities", [])) or "bakery" in shops
            ):
                self.inactive[e.key] = "Non-food-service amenity without hospitality evidence"
            elif e.tags.get("tourism") and not (
                amenities & set(scope.get("keep_osm_amenities", [])) or "bakery" in shops
            ):
                self.inactive[e.key] = (
                    "Accommodation or visitor attraction without hospitality evidence"
                )
            elif rows[e.key]["classification"]["selection"]["eligibility"] == "excluded":
                self.inactive[e.key] = "Existing source hospitality exclusion"
        for c in self.corrections.corrections:
            if c.current_source:
                check(c.current_source)
            for old in c.superseded_source_labels:
                check(old.source)
                self.inactive[old.source.key] = c.owner_statement
            if c.current_source and c.current_name:
                prior = self.overrides[c.current_source.key].get("display_name")
                if prior and prior[0] != c.current_name:
                    raise ValueError("Conflicting owner name corrections")
                self.overrides[c.current_source.key]["display_name"] = (c.current_name, c.review_id)
                self.inactive.pop(c.current_source.key, None)
            for field in ("postcode", "address"):
                value = getattr(c.field_overrides, field)
                side = getattr(c.field_overrides, "preferred_" + field + "_source")
                if value is not None:
                    pair = next(
                        (r for r in self.decisions.reviews if r.review_id == c.review_id), None
                    )
                    if pair is None or side is None:
                        raise ValueError("Field correction needs a bound reviewed source")
                    target = getattr(pair, side)
                    self.overrides[target.key][field] = (value, c.review_id)
        # Earlier, already approved pilot corrections retain their original evidence.
        for rid, side, value, old_side in (
            ("M007", "left", "Big Sams Kitchen At The Signalman", "right"),
            ("M027", "right", "The Chimney House", "left"),
            ("M028", "right", "Eat Central", None),
            ("M030", "right", "Pizza King", None),
        ):
            pilot_review = next((r for r in self.original.reviews if r.review_id == rid), None)
            if pilot_review:
                target = getattr(pilot_review, side)
                self.overrides[target.key]["display_name"] = (value, rid)
                self.inactive.pop(target.key, None)
                if old_side:
                    self.inactive[getattr(pilot_review, old_side).key] = pilot_review.reason
        return Reviews(
            version=self.version,
            reviews=tuple(all_reviews),
            held_source_keys=(),
            preferred_name_keys=self.original.preferred_name_keys,
        )

    def adjust_flags(
        self, pairs: list[Any], groups: list[list[str]], flags: dict[str, list[str]]
    ) -> None:
        group = {k: i for i, members in enumerate(groups) for k in members}
        uncertain = {
            k
            for p in pairs
            if p.decision == "pending"
            and group[p.left] != group[p.right]
            and p.left not in self.inactive
            and p.right not in self.inactive
            for k in (p.left, p.right)
        }
        for key in list(flags):
            flags[key] = [
                f for f in flags[key] if f != "unresolved_duplicate_candidate" or key in uncertain
            ]

    def apply_fields(self, body: dict[str, Any], members: list[str]) -> None:
        body["field_reviews"] = {}
        for field in ("display_name", "address", "postcode"):
            choices = [
                (k, self.overrides[k][field]) for k in members if field in self.overrides.get(k, {})
            ]
            if len({str(value[0]) for _, value in choices}) > 1:
                raise ValueError("Conflicting owner field overrides in accepted identity")
            if choices:
                key, (value, rid) = choices[0]
                if field in {"address", "postcode"} and body["location_quality"] == "withheld":
                    raise ValueError("Owner correction cannot reconstruct withheld premises")
                body[field] = value
                body["field_sources"]["name" if field == "display_name" else field] = key
                body["field_reviews"][field] = (
                    "correction/" + rid if rid.startswith("U") else "pair/" + rid
                )
        body["review_application_sha256"] = self.sha256
        body["inactive_sources"] = {k: self.inactive[k] for k in members if k in self.inactive}

"""Conservative identity graph: references and reviews accept; similarity discovers."""

from __future__ import annotations

import math
import re
import unicodedata
from collections import defaultdict
from datetime import date
from difflib import SequenceMatcher
from typing import Literal

from pydantic import Field
from pyproj import Geod
from shapely import STRtree, box
from shapely.geometry import Point

from urbanlens.ingestion.contracts import Contract, Digest
from urbanlens.silver.eligibility import digest

GEOD = Geod(ellps="WGS84")


class Rules(Contract):
    version: str
    candidate_radius_metres: float = Field(gt=0, le=2000)
    candidate_name_similarity: float = Field(ge=0, le=1)
    postcode_name_similarity: float = Field(ge=0, le=1)
    reference_radius_metres: float = Field(gt=0, le=500)
    reference_name_similarity: float = Field(ge=0, le=1)
    osm_duplicate_radius_metres: float = Field(gt=0, le=100)
    location_agreement_metres: float = Field(gt=0, le=500)
    reuse_distance_metres: float = Field(gt=0)
    reuse_name_similarity: float = Field(ge=0, le=1)
    accept_guarded_references: bool


class Ref(Contract):
    provider: Literal["fsa", "osm"]
    scope: str
    source_key: str
    generation: int = Field(ge=1)
    record_hash: Digest

    @property
    def key(self) -> str:
        return f"{self.provider}|{self.scope}|{self.source_key}|{self.generation}"


class Entry(Ref):
    name: str | None = None
    address: str | None = None
    postcode: str | None = None
    longitude: float | None = None
    latitude: float | None = None
    tags: dict[str, tuple[str, ...]] = {}
    location_hash: Digest
    classification_hash: Digest
    primary_category: str | None = None
    withheld: bool = False


class PairReview(Contract):
    review_id: str
    left: Ref
    right: Ref
    decision: Literal["same place", "different places", "unsure"]
    reviewer: str = Field(min_length=1)
    reviewed_on: date
    evidence: str = Field(min_length=1)
    reason: str = Field(min_length=1)


class Reviews(Contract):
    version: str
    reviews: tuple[PairReview, ...] = ()
    # Prior owner statements about stale/renamed businesses require field review.
    held_source_keys: tuple[str, ...] = ()
    preferred_name_keys: tuple[str, ...] = ()


class Pair(Contract):
    pair_id: Digest
    left: str
    right: str
    left_hash: Digest
    right_hash: Digest
    decision: Literal["accepted", "rejected", "pending"]
    method: str
    reasons: tuple[str, ...]
    distance_metres: float | None
    name_similarity: float
    review_id: str | None = None


def normalise(value: str | None) -> str:
    text = unicodedata.normalize("NFKD", (value or "").casefold().replace("&", " and "))
    return " ".join(re.findall(r"[a-z0-9]+", text))


def similarity(left: str | None, right: str | None) -> float:
    a, b = normalise(left), normalise(right)
    return SequenceMatcher(None, a, b, autojunk=False).ratio() if a and b else 0.0


def references(values: tuple[str, ...]) -> tuple[tuple[str, ...], tuple[str, ...]]:
    tokens = [t.strip() for value in values for t in value.split(";") if t.strip()]
    good = {str(int(t)) for t in tokens if re.fullmatch(r"[0-9]+", t) and int(t) > 0}
    bad = {t for t in tokens if not re.fullmatch(r"[0-9]+", t) or int(t) == 0}
    return tuple(sorted(good)), tuple(sorted(bad))


def distance(a: Entry, b: Entry) -> float | None:
    if a.longitude is None or a.latitude is None or b.longitude is None or b.latitude is None:
        return None
    return round(float(GEOD.inv(a.longitude, a.latitude, b.longitude, b.latitude)[2]), 6)


def house_number(entry: Entry) -> str | None:
    if entry.provider == "osm":
        values = entry.tags.get("addr:housenumber", ())
        return normalise(values[0]) if len(values) == 1 else None
    match = re.match(r"^\s*(\d+[a-zA-Z]?(?:\s*[-/]\s*\d+[a-zA-Z]?)?)\b", entry.address or "")
    return normalise(match[1]) if match else None


def contradictions(a: Entry, b: Entry, rules: Rules) -> list[str]:
    reasons = []
    metres = distance(a, b)
    if metres is None:
        reasons.append("precise_coordinate_missing")
    elif metres > rules.reference_radius_metres:
        reasons.append("reference_points_too_far_apart")
    if similarity(a.name, b.name) < rules.reference_name_similarity:
        reasons.append("source_names_disagree")
    if a.postcode and b.postcode and a.postcode != b.postcode:
        reasons.append("published_postcodes_disagree")
    na, nb = house_number(a), house_number(b)
    if na and nb and na != nb:
        reasons.append("published_house_numbers_disagree")
    if a.primary_category and b.primary_category and a.primary_category != b.primary_category:
        reasons.append("specific_categories_disagree")
    if a.withheld or b.withheld:
        reasons.append("withheld_premises")
    return reasons


class Graph:
    def __init__(self, keys: list[str]) -> None:
        self.parent = {k: k for k in keys}

    def root(self, key: str) -> str:
        if self.parent[key] != key:
            self.parent[key] = self.root(self.parent[key])
        return self.parent[key]

    def join(self, a: str, b: str) -> None:
        x, y = sorted((self.root(a), self.root(b)))
        self.parent[y] = x


def resolve(
    entries: list[Entry],
    rules: Rules,
    reviews: Reviews,
    previous: dict[str, Entry] | None = None,
) -> tuple[list[Pair], list[list[str]], dict[str, list[str]]]:
    by_key = {e.key: e for e in entries}
    if len(by_key) != len(entries):
        raise ValueError("Duplicate resolver input identity")
    graph = Graph(sorted(by_key))
    flags: dict[str, list[str]] = defaultdict(list)
    pairs: dict[tuple[str, str], Pair] = {}
    for e in entries:
        old = (previous or {}).get(e.key)
        if old and old.record_hash != e.record_hash:
            moved = distance(old, e)
            if (
                moved is not None
                and moved > rules.reuse_distance_metres
                and (
                    similarity(old.name, e.name) < rules.reuse_name_similarity
                    or (old.postcode and e.postcode and old.postcode != e.postcode)
                )
            ):
                flags[e.key].append("possible_source_id_reuse_new_generation_review_required")
        if e.key in reviews.held_source_keys:
            flags[e.key].append("owner_reported_stale_or_corrected_source_requires_field_review")

    def put(
        a: Entry,
        b: Entry,
        decision: str,
        method: str,
        reasons: list[str],
        review_id: str | None = None,
    ) -> None:
        left, right = sorted((a.key, b.key))
        if left == right:
            raise ValueError("A source cannot be matched to itself")
        item = Pair.model_validate(
            dict(
                pair_id=digest([left, right]),
                left=left,
                right=right,
                left_hash=by_key[left].record_hash,
                right_hash=by_key[right].record_hash,
                decision=decision,
                method=method,
                reasons=reasons,
                distance_metres=distance(a, b),
                name_similarity=round(similarity(a.name, b.name), 6),
                review_id=review_id,
            )
        )
        old = pairs.get((left, right))
        if old and old.method == "owner_review":
            if method == "owner_review" and old != item:
                raise ValueError("Conflicting pair reviews")
            return
        pairs[left, right] = item

    for review in reviews.reviews:
        a, b = by_key.get(review.left.key), by_key.get(review.right.key)
        if a is None or b is None:
            raise ValueError("Pair review identity/generation is not in current input")
        stale = (a.record_hash, b.record_hash) != (
            review.left.record_hash,
            review.right.record_hash,
        )
        decision = (
            "pending"
            if stale or review.decision == "unsure"
            else ("accepted" if review.decision == "same place" else "rejected")
        )
        put(
            a,
            b,
            decision,
            "owner_review",
            ["stale_review" if stale else review.reason],
            review.review_id,
        )
        if decision == "pending":
            reason = "stale_owner_identity_review" if stale else "owner_identity_review_pending"
            flags[a.key].append(reason)
            flags[b.key].append(reason)
    for pair in pairs.values():
        if pair.decision == "accepted":
            graph.join(pair.left, pair.right)
    for pair in pairs.values():
        if pair.decision == "rejected" and graph.root(pair.left) == graph.root(pair.right):
            raise ValueError("Accepted reviews transitively contradict a different-place review")

    def blocked(a: Entry, b: Entry) -> bool:
        ra, rb = graph.root(a.key), graph.root(b.key)
        return any(
            (p.decision == "rejected" or (p.method == "owner_review" and p.decision == "pending"))
            and {graph.root(p.left), graph.root(p.right)} == {ra, rb}
            for p in pairs.values()
        )

    fsa = {str(int(e.source_key)): e for e in entries if e.provider == "fsa"}
    if len(fsa) != sum(e.provider == "fsa" for e in entries):
        raise ValueError("Ambiguous FSA identifiers after FHRS reference normalisation")
    osm = [e for e in entries if e.provider == "osm"]
    refs = {e.key: references(e.tags.get("fhrs:id", ())) for e in osm}
    referenced: dict[str, list[Entry]] = defaultdict(list)
    for e in osm:
        good, bad = refs[e.key]
        if bad:
            flags[e.key].append("invalid_fhrs_reference_tokens")
        for ref in good:
            referenced[ref].append(e)
            if ref not in fsa:
                flags[e.key].append("fhrs_reference_target_not_in_supported_input")
    # Resolve node/area representations before considering repeated FHRS references.
    for ref, sources in sorted(referenced.items()):
        for index, a in enumerate(sources):
            for b in sources[index + 1 :]:
                reasons = contradictions(a, b, rules)
                metres = distance(a, b)
                types = {a.source_key.split("/")[0], b.source_key.split("/")[0]}
                safe = (
                    rules.accept_guarded_references
                    and not reasons
                    and normalise(a.name) == normalise(b.name)
                    and len(normalise(a.name)) >= 5
                    and refs[a.key] == ((ref,), ())
                    and refs[b.key] == ((ref,), ())
                    and "node" in types
                    and bool(types & {"way", "relation"})
                    and metres is not None
                    and metres <= rules.osm_duplicate_radius_metres
                    and not blocked(a, b)
                    and not flags.get(a.key)
                    and not flags.get(b.key)
                )
                put(
                    a,
                    b,
                    "accepted" if safe else "pending",
                    "repeated_reference",
                    ["guarded_node_area_same_reference"]
                    if safe
                    else reasons or ["repeated_reference_requires_review"],
                )
                pair_key = (min(a.key, b.key), max(a.key, b.key))
                if pairs[pair_key].decision == "accepted":
                    graph.join(a.key, b.key)
    component_references: dict[str, set[str]] = defaultdict(set)
    for source in osm:
        component_references[graph.root(source.key)].update(refs[source.key][0])
    refs_for_source = {source.key: component_references[graph.root(source.key)] for source in osm}
    for e in osm:
        good, bad = refs[e.key]
        for ref in good:
            target = fsa.get(ref)
            if target is None:
                continue
            reasons = contradictions(e, target, rules)
            if len(good) > 1:
                reasons.append("multiple_fhrs_references")
            component_refs = refs_for_source[e.key]
            if len(component_refs) > 1:
                reasons.append("osm_component_has_multiple_fhrs_references")
            if bad:
                reasons.append("invalid_fhrs_reference_tokens")
            if len({graph.root(s.key) for s in referenced[ref]}) > 1:
                reasons.append("unresolved_repeated_fhrs_reference")
            if blocked(e, target):
                reasons.append("would_conflict_with_reviewed_distinct_businesses")
            if flags.get(e.key) or flags.get(target.key):
                reasons.append("source_identity_requires_review")
            safe = rules.accept_guarded_references and not reasons
            put(
                e,
                target,
                "accepted" if safe else "pending",
                "fhrs_reference",
                reasons or ["unique_reference_passed_name_address_distance_checks"],
            )
            pair_key = (min(e.key, target.key), max(e.key, target.key))
            if pairs[pair_key].decision == "accepted":
                graph.join(e.key, target.key)

    located = [e for e in entries if e.longitude is not None and e.latitude is not None]
    tree = STRtree([Point(e.longitude, e.latitude) for e in located])
    for a in located:
        assert a.longitude is not None and a.latitude is not None
        dy = rules.candidate_radius_metres / 110000
        dx = dy / max(0.01, math.cos(math.radians(a.latitude)))
        for index in tree.query(
            box(a.longitude - dx, a.latitude - dy, a.longitude + dx, a.latitude + dy)
        ):
            b = located[int(index)]
            if a.key >= b.key or (a.key, b.key) in pairs:
                continue
            metres = distance(a, b)
            score = similarity(a.name, b.name)
            same_pc = bool(a.postcode and a.postcode == b.postcode)
            if (
                metres is not None
                and metres <= rules.candidate_radius_metres
                and (
                    score >= rules.candidate_name_similarity
                    or (same_pc and score >= rules.postcode_name_similarity)
                )
            ):
                put(
                    a,
                    b,
                    "pending",
                    "similarity_candidate",
                    ["name_address_proximity_are_not_identity_proof"],
                )
    addresses: dict[tuple[str, str], list[Entry]] = defaultdict(list)
    for e in entries:
        if normalise(e.name) and normalise(e.address):
            addresses[normalise(e.name), normalise(e.address)].append(e)
    for members in addresses.values():
        for i, a in enumerate(members):
            for b in members[i + 1 :]:
                if tuple(sorted((a.key, b.key))) not in pairs:
                    put(
                        a,
                        b,
                        "pending",
                        "same_name_address",
                        ["exact_text_is_candidate_evidence_only"],
                    )
    # A newly discovered possible continuation cannot silently create a new venue.
    if previous:
        old_keys = set(previous)
        for pair in pairs.values():
            if pair.decision == "pending" and ((pair.left in old_keys) != (pair.right in old_keys)):
                flags[pair.left].append("possible_changed_source_id_continuity")
                flags[pair.right].append("possible_changed_source_id_continuity")
    for pair_key, pair in list(pairs.items()):
        if (
            pair.decision == "pending"
            and pair.method != "owner_review"
            and (graph.root(pair.left) == graph.root(pair.right))
        ):
            pairs[pair_key] = pair.model_copy(
                update={
                    "decision": "accepted",
                    "method": "accepted_transitive_path",
                    "reasons": (*pair.reasons, "accepted_identity_path_resolves_pair"),
                }
            )
    for pair in pairs.values():
        if pair.decision == "pending" and graph.root(pair.left) != graph.root(pair.right):
            flags[pair.left].append("unresolved_duplicate_candidate")
            flags[pair.right].append("unresolved_duplicate_candidate")
    groups: dict[str, list[str]] = defaultdict(list)
    for key in sorted(by_key):
        groups[graph.root(key)].append(key)
    return sorted(pairs.values(), key=lambda p: p.pair_id), list(groups.values()), dict(flags)

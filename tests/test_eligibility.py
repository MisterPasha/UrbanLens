"""Conservative public-venue scope, evidence-bound reviews and honest category coverage."""

from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from uuid import uuid4

import pytest
from pydantic import ValidationError

from urbanlens.ingestion.contracts import Artifact
from urbanlens.silver import normalise as n
from urbanlens.silver.eligibility import (
    Decision,
    Reviews,
    Taxonomy,
    classify,
    resolve_linked,
)
from urbanlens.silver.models import Evidence, Observation

ROOT = Path(__file__).parents[1]


@pytest.fixture
def taxonomy() -> Taxonomy:
    return Taxonomy.model_validate_json((ROOT / "config/taxonomy.json").read_text())


def context() -> dict[str, Any]:
    return dict(
        scope="synthetic",
        snapshot_id=uuid4(),
        cycle_id=uuid4(),
        observed_at=datetime(2026, 11, 1, tzinfo=UTC),
        evidence=(
            Evidence(
                artifact=Artifact(
                    uri="s3://synthetic/bronze/source",
                    sha256="a" * 64,
                    byte_count=1,
                    media_type="application/json",
                ),
                locator="row/1",
            ),
        ),
    )


def fsa(code: str, name: str = "A Bakery Cafe Pub Restaurant") -> Observation:
    return n.fsa(dict(FHRSID="000123", BusinessName=name, BusinessTypeID=code), context())


def osm(tags: dict[str, str], identity: int = 123) -> Observation:
    return n.osm(
        dict(
            element_type="node",
            osm_id=identity,
            tags=tags,
            geometry_state="source-node",
            longitude=-0.14,
            latitude=50.82,
            geometry=None,
            original_geometry={},
            fhrs_ids=[],
            invalid_fhrs_tokens=[],
            in_city=True,
            crosses_city_boundary=False,
        ),
        context(),
    )


def result(record: Observation, taxonomy: Taxonomy) -> Decision:
    return classify(record, taxonomy, Reviews(version="fixture"))


@pytest.mark.parametrize(
    "code,eligibility,category",
    [
        ("1", "unresolved", None),
        ("4613", "unresolved", None),
        ("7843", "unresolved", None),
        ("7844", "eligible", "takeaway"),
        ("7841", "unresolved", None),
        ("7846", "unresolved", None),
        ("5", "excluded", None),
        ("7845", "excluded", None),
        ("7840", "excluded", None),
        ("7842", "excluded", None),
        ("7839", "excluded", None),
        ("99999", "unresolved", None),
    ],
)
def test_fsa_coarse_and_nonvenue_types_do_not_become_fine_categories(
    taxonomy: Taxonomy, code: str, eligibility: str, category: str | None
) -> None:
    selection = result(fsa(code), taxonomy).selection
    assert selection.eligibility == eligibility and selection.primary_category == category
    assert selection.rules and selection.reasons
    if eligibility == "unresolved":
        assert selection.review_required


@pytest.mark.parametrize(
    "tags,eligibility,category",
    [
        ({"amenity": "cafe"}, "eligible", "cafe"),
        ({"amenity": "restaurant"}, "eligible", "restaurant"),
        ({"amenity": "fast_food"}, "eligible", "takeaway"),
        ({"amenity": "pub"}, "eligible", "pub_bar"),
        ({"amenity": "bar"}, "eligible", "pub_bar"),
        ({"shop": "bakery"}, "eligible", "bakery"),
        ({"amenity": "ice_cream"}, "eligible", "food_service"),
        ({"amenity": "cafe;restaurant"}, "eligible", "food_service"),
        ({"shop": "bakery", "amenity": "cafe"}, "eligible", "food_service"),
        ({"amenity": "canteen"}, "unresolved", None),
        ({"amenity": "canteen", "access": "customers"}, "eligible", "food_service"),
        ({"amenity": "canteen", "access": "private"}, "excluded", None),
        ({"amenity": "cafe", "access": "no"}, "excluded", None),
        ({"amenity": "cafe", "access": "yes;private"}, "unresolved", None),
        ({"amenity": "cafe", "access": "customers"}, "eligible", "cafe"),
        ({"amenity": "cafe", "membership": "yes"}, "excluded", None),
        ({"tourism": "hotel"}, "excluded", None),
        ({"tourism": "hotel", "amenity": "restaurant"}, "unresolved", None),
        ({"tourism": "hotel", "amenity": "restaurant", "access": "yes"}, "eligible", "restaurant"),
        ({"building": "school", "amenity": "cafe"}, "unresolved", None),
        ({"building": "school", "amenity": "cafe", "access": "yes"}, "eligible", "cafe"),
        ({"amenity": "school;cafe", "access": "yes"}, "unresolved", None),
        ({"amenity": "hospital"}, "excluded", None),
        ({"shop": "convenience", "amenity": "cafe"}, "unresolved", None),
        ({"shop": "newsagent"}, "excluded", None),
        ({"shop": "confectionery"}, "excluded", None),
        ({"craft": "bakery"}, "excluded", None),
        ({"craft": "bakery", "shop": "bakery"}, "eligible", "bakery"),
        ({"disused:amenity": "pub"}, "unresolved", None),
        ({"disused:amenity": "pub", "amenity": "pub"}, "unresolved", None),
        ({"name": "A Bakery Cafe Pub Restaurant"}, "unresolved", None),
    ],
)
def test_osm_scope_requires_supported_public_venue_evidence(
    taxonomy: Taxonomy, tags: dict[str, str], eligibility: str, category: str | None
) -> None:
    decision = result(osm(tags), taxonomy)
    assert decision.selection.eligibility == eligibility
    assert decision.selection.primary_category == category
    assert decision.source_evidence and decision.taxonomy_sha256


def test_multiple_categories_are_one_broad_observation_and_tags_are_preserved(
    taxonomy: Taxonomy,
) -> None:
    decision = result(osm({"amenity": "cafe;restaurant", "cuisine": "vegan;italian"}), taxonomy)
    assert decision.selection.category_state == "ambiguous" and decision.selection.review_required
    assert set(decision.selection.candidate_categories) == {"cafe", "restaurant"}
    assert decision.secondary_tags["osm.cuisine"] == ("italian", "vegan")


def test_owner_review_is_scoped_hash_bound_and_requires_public_confirmation(
    taxonomy: Taxonomy,
) -> None:
    record = fsa("1")
    review = dict(
        review_id="fixture-review",
        provider="fsa",
        scope=record.scope,
        source_key=record.source_key,
        expected_record_hash=record.record_hash,
        reviewer="fixture-owner",
        reviewed_on="2026-11-02",
        eligibility="eligible",
        primary_category="food_service",
        public_venue_confirmed=True,
        reason="Confirmed public canteen",
        evidence="Synthetic documented owner confirmation",
    )
    reviews = Reviews.model_validate(dict(version="fixture", reviews=[review]))
    decision = classify(record, taxonomy, reviews)
    assert (
        decision.review_status == "applied"
        and decision.selection.primary_category == "food_service"
    )
    assert decision.automatic_selection.eligibility == "unresolved"
    changed = classify(fsa("1", "Different business"), taxonomy, reviews)
    assert changed.review_status == "stale" and changed.selection.eligibility == "unresolved"
    assert classify(osm({"amenity": "cafe"}), taxonomy, reviews).review_status == "none"
    with pytest.raises(ValidationError, match="public-venue"):
        Reviews.model_validate(
            dict(version="fixture", reviews=[{**review, "public_venue_confirmed": False}])
        )
    with pytest.raises(ValidationError, match="Duplicate"):
        Reviews.model_validate(dict(version="fixture", reviews=[review, review]))


def test_company_sic_and_registered_office_never_establish_venue_eligibility(
    taxonomy: Taxonomy,
) -> None:
    record = n.company(
        dict(
            company_number="SC000001",
            company_name="Example Bakery",
            company_status="active",
            type="ltd",
            sic_codes=["56101", "10710"],
        ),
        context(),
        profile=True,
    )
    decision = result(record, taxonomy)
    assert not decision.applicable and decision.selection.eligibility == "excluded"


def test_accepted_links_can_resolve_coarse_evidence_but_conflicts_stay_explicit(
    taxonomy: Taxonomy,
) -> None:
    cafe = result(osm({"amenity": "cafe"}), taxonomy)
    assert resolve_linked((result(fsa("1"), taxonomy), cafe)).primary_category == "cafe"
    conflicting = resolve_linked((result(fsa("7845"), taxonomy), cafe))
    assert conflicting.eligibility == "unresolved" and conflicting.conflicts
    ambiguous = result(osm({"amenity": "cafe;restaurant"}), taxonomy)
    assert resolve_linked((cafe, ambiguous)).primary_category == "food_service"


def test_accepted_link_resolves_eligibility_without_erasing_duplicate_or_location_gates(
    taxonomy: Taxonomy,
) -> None:
    from urbanlens.silver.identity import Entry, Rules
    from urbanlens.silver.identity import Reviews as IdentityReviews
    from urbanlens.silver.resolution import canonical

    rules = Rules.model_validate_json((ROOT / "config/identity-rules.json").read_text())
    records = [
        n.fsa(
            dict(
                FHRSID="123",
                BusinessName="Example Cafe",
                BusinessTypeID="1",
                AddressLine1="1 Example Street",
                PostCode="BN1 1AA",
                Longitude="-0.14",
                Latitude="50.82",
            ),
            context(),
        ),
        osm({"name": "Example Cafe", "amenity": "cafe"}),
    ]
    entries = {}
    rows = {}
    for record in records:
        decision = result(record, taxonomy)
        e = Entry(
            provider="fsa" if record.provider.value == "fsa" else "osm",
            scope=record.scope,
            source_key=record.source_key,
            generation=1,
            record_hash=record.record_hash,
            name=record.name.value,
            address=record.address.value,
            longitude=-0.14,
            latitude=50.82,
            location_hash="a" * 64,
            classification_hash=decision.decision_hash,
        )
        entries[e.key] = e
        rows[e.key] = dict(
            classification=decision.model_dump(mode="json"),
            spatial_usable=True,
            location_quality="source_point",
            source_state="observed",
            first_observed_at=record.observed_at,
            last_observed_at=record.observed_at,
            location_decision=dict(
                quality="source",
                longitude=-0.14,
                latitude=50.82,
                spatial_usable=True,
                area_code="E01000001",
                geography_version="LSOA-2021",
            ),
        )
    reviews = IdentityReviews(version="test")
    members = list(entries)
    resolved = canonical(members, entries, rows, {}, rules, reviews)
    assert resolved["eligibility"] == "eligible" and resolved["primary_category"] == "cafe"
    assert resolved["primary_spatial_eligible"]
    assert canonical([members[1]], entries, rows, {}, rules, reviews)["primary_spatial_eligible"]
    blocked = canonical(
        members, entries, rows, {members[0]: ["unresolved_duplicate_candidate"]}, rules, reviews
    )
    assert blocked["eligibility"] == "eligible" and not blocked["primary_spatial_eligible"]
    entries[members[1]] = entries[members[1]].model_copy(update={"longitude": -0.15})
    conflict = canonical(members, entries, rows, {}, rules, reviews)
    assert conflict["duplicate_status"] == "resolved" and not conflict["primary_spatial_eligible"]
    assert conflict["location_quality"] == "unresolved"


def test_decision_replay_and_tampering(taxonomy: Taxonomy) -> None:
    record = osm({"amenity": "cafe"})
    first = result(record, taxonomy)
    assert result(record, taxonomy) == first
    data = first.model_dump(mode="json")
    data["selection"]["reasons"] = ["tampered"]
    with pytest.raises(ValidationError, match="hash mismatch"):
        Decision.model_validate(data)


def test_coverage_includes_broad_eligible_records_and_excludes_unresolved_and_nonvenues(
    tmp_path: Path, taxonomy: Taxonomy
) -> None:
    from urbanlens.ingestion.storage import LocalArtifactStore
    from urbanlens.silver.classification import build, check, save_json
    from urbanlens.silver.pipeline import Bundle, write_batch

    store = LocalArtifactStore(tmp_path / "objects")
    observations = [
        osm({"amenity": "cafe"}, 1),
        osm({"amenity": "cafe;restaurant"}, 2),
        osm({"amenity": "canteen"}, 3),
        osm({"shop": "newsagent"}, 4),
        n.company(
            dict(
                company_number="SC000001",
                company_name="Cafe Ltd",
                company_status="active",
                type="ltd",
                sic_codes=["56101"],
            ),
            context(),
            profile=True,
        ),
    ]
    raw = save_json(store, "bronze/fixture", {"synthetic": True})
    batches = tuple(
        write_batch(
            store,
            f"silver/input/{i}.jsonl.gz",
            iter([r]),
            provider=r.provider,
            scope=r.scope,
            partition=str(i),
            snapshot=None,
            source_manifest=raw,
        )
        for i, r in enumerate(observations)
    )
    source = Bundle(batches=batches)
    manifest = save_json(store, "manifests/source", source.model_dump(mode="json"))
    reviews = Reviews(version="fixture")
    bundle, output, summary, queue = build(store, source, manifest, taxonomy, reviews)
    assert check(store, bundle) == {
        "verified_decisions": 5,
        "all_recomputed_from_pinned_sources": True,
    }
    assert summary["fine_category_coverage"]["numerator"] == 1
    assert summary["fine_category_coverage"]["denominator"] == 2
    assert summary["fine_category_coverage"]["fraction"] == 0.5
    assert summary["counts"]["not_applicable"] == 1 and len(queue) == 2
    assert build(store, source, manifest, taxonomy, reviews) == (bundle, output, summary, queue)

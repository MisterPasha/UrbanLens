"""Identity evidence never turns name/proximity or co-location into proof."""

from pathlib import Path

import pytest

from urbanlens.silver.identity import Entry, PairReview, Ref, Reviews, Rules, references, resolve


@pytest.fixture
def rules() -> Rules:
    return Rules.model_validate_json(
        (Path(__file__).parents[1] / "config/identity-rules.json").read_text()
    )


def entry(provider: str = "fsa", source_key: str = "1", **changes: object) -> Entry:
    return Entry.model_validate(
        dict(
            provider=provider,
            scope="test",
            source_key=source_key,
            generation=1,
            record_hash="a" * 64,
            name="Example Cafe",
            address="1 Example Street",
            postcode="BN11AA",
            longitude=-0.14,
            latitude=50.82,
            location_hash="b" * 64,
            classification_hash="c" * 64,
        )
        | changes
    )


def review(a: Entry, b: Entry, decision: str = "same place", rid: str = "R1") -> PairReview:
    def ref(e: Entry) -> Ref:
        return Ref(
            provider=e.provider,
            scope=e.scope,
            source_key=e.source_key,
            generation=e.generation,
            record_hash=e.record_hash,
        )

    return PairReview.model_validate(
        dict(
            review_id=rid,
            left=ref(a),
            right=ref(b),
            decision=decision,
            reviewer="owner",
            reviewed_on="2026-11-02",
            evidence="https://example.test/review",
            reason="fixture",
        )
    )


def test_reference_values_expand_canonicalise_and_reject_invalid() -> None:
    assert references(("001; 2;2;garbage;0;١٢; -1",)) == (("1", "2"), ("-1", "0", "garbage", "١٢"))


def test_unique_reference_with_corroborating_evidence_accepts(rules: Rules) -> None:
    a, b = entry(), entry("osm", "node/1", tags={"fhrs:id": ("1",)})
    pairs, groups, flags = resolve([a, b], rules, Reviews(version="test"))
    assert pairs[0].decision == "accepted" and len(groups) == 1 and not flags


@pytest.mark.parametrize(
    "change",
    [
        dict(longitude=-0.2),
        dict(name="Unrelated Pub"),
        dict(postcode="BN22AA"),
        dict(tags={"fhrs:id": ("1",), "addr:housenumber": ("99",)}),
        dict(withheld=True),
    ],
)
def test_reference_contradictions_remain_pending(rules: Rules, change: dict[str, object]) -> None:
    a = entry()
    b = entry("osm", "node/1", **(dict(tags={"fhrs:id": ("1",)}) | change))
    pairs, groups, flags = resolve([a, b], rules, Reviews(version="test"))
    assert pairs[0].decision == "pending" and len(groups) == 2 and len(flags) == 2


def test_fuzzy_similarity_does_not_accept(rules: Rules) -> None:
    a, b = entry(), entry("osm", "node/1")
    pairs, groups, flags = resolve([a, b], rules, Reviews(version="test"))
    assert pairs[0].method == "similarity_candidate" and pairs[0].decision == "pending"
    assert len(groups) == 2 and len(flags) == 2


def test_distinct_colocated_review_overrides_reference(rules: Rules) -> None:
    a, b = entry(), entry("osm", "node/1", tags={"fhrs:id": ("1",)})
    pairs, groups, flags = resolve(
        [a, b], rules, Reviews(version="test", reviews=(review(a, b, "different places"),))
    )
    assert pairs[0].decision == "rejected" and len(groups) == 2 and not flags


def test_stale_review_does_not_apply(rules: Rules) -> None:
    a, b = entry(), entry("osm", "node/1", tags={"fhrs:id": ("1",)})
    old = review(a, b)
    b = b.model_copy(update={"record_hash": "d" * 64})
    pairs, groups, flags = resolve([a, b], rules, Reviews(version="test", reviews=(old,)))
    assert pairs[0].decision == "pending" and pairs[0].reasons == ("stale_review",)
    assert len(groups) == 2 and len(flags) == 2


def test_transitive_review_contradiction_fails(rules: Rules) -> None:
    a, b, c = entry(), entry("osm", "node/1"), entry("osm", "node/2")
    reviews = Reviews(
        version="test",
        reviews=(
            review(a, b, rid="R1"),
            review(b, c, rid="R2"),
            review(a, c, "different places", rid="R3"),
        ),
    )
    with pytest.raises(ValueError, match="transitively contradict"):
        resolve([a, b, c], rules, reviews)


def test_guarded_node_area_duplicates_resolve_repeated_reference(rules: Rules) -> None:
    entries = [
        entry(),
        entry("osm", "node/1", tags={"fhrs:id": ("1",)}),
        entry("osm", "way/2", tags={"fhrs:id": ("1",)}),
    ]
    pairs, groups, flags = resolve(entries, rules, Reviews(version="test"))
    assert len(groups) == 1 and len(pairs) == 3 and not flags
    assert all(p.decision == "accepted" for p in pairs)


def test_repeated_reference_two_nodes_requires_review(rules: Rules) -> None:
    entries = [
        entry(),
        entry("osm", "node/1", tags={"fhrs:id": ("1",)}),
        entry("osm", "node/2", tags={"fhrs:id": ("1",)}),
    ]
    pairs, groups, flags = resolve(entries, rules, Reviews(version="test"))
    assert len(groups) == 3 and len(flags) == 3
    assert all(p.decision == "pending" for p in pairs)


def test_accepted_paths_resolve_redundant_pair_uncertainty(rules: Rules) -> None:
    a, b, c = (
        entry(),
        entry("osm", "node/1", tags={"fhrs:id": ("1",)}),
        entry("osm", "node/2", tags={"fhrs:id": ("1",)}),
    )
    pairs, groups, flags = resolve(
        [a, b, c],
        rules,
        Reviews(
            version="test",
            reviews=(
                review(a, b, rid="R1"),
                review(a, c, rid="R2"),
            ),
        ),
    )
    assert len(groups) == 1 and not flags
    assert all(p.decision == "accepted" for p in pairs)
    assert any(p.method == "accepted_transitive_path" for p in pairs)


def test_multiple_references_are_not_an_automatic_fsa_merge(rules: Rules) -> None:
    entries = [entry(), entry(source_key="2"), entry("osm", "node/1", tags={"fhrs:id": ("1;2",)})]
    pairs, groups, _ = resolve(entries, rules, Reviews(version="test"))
    assert len(groups) == 3 and not any(p.decision == "accepted" for p in pairs)


def test_reviewed_osm_duplicate_with_two_fsa_ids_does_not_merge_fsa_automatically(
    rules: Rules,
) -> None:
    a, b = (
        entry("osm", "node/1", tags={"fhrs:id": ("1",)}),
        entry("osm", "way/2", tags={"fhrs:id": ("2",)}),
    )
    pairs, groups, _ = resolve(
        [entry(), entry(source_key="2"), a, b],
        rules,
        Reviews(version="test", reviews=(review(a, b),)),
    )
    assert len(groups) == 3
    assert sum(p.decision == "accepted" for p in pairs) == 1


def test_ambiguous_fsa_reference_normalisation_fails(rules: Rules) -> None:
    with pytest.raises(ValueError, match="Ambiguous FSA identifiers"):
        resolve([entry(), entry(source_key="01")], rules, Reviews(version="test"))


def test_automatic_reference_acceptance_can_be_disabled(rules: Rules) -> None:
    pairs, groups, _ = resolve(
        [entry(), entry("osm", "node/1", tags={"fhrs:id": ("1",)})],
        rules.model_copy(update={"accept_guarded_references": False}),
        Reviews(version="test"),
    )
    assert len(groups) == 2 and pairs[0].decision == "pending"


def test_source_only_branches_stay_separate(rules: Rules) -> None:
    a, b = (
        entry("osm", "node/1"),
        entry("osm", "node/2", longitude=-0.15, address="100 Example Street"),
    )
    pairs, groups, flags = resolve([a, b], rules, Reviews(version="test"))
    assert not pairs and len(groups) == 2 and not flags


def test_missing_point_exact_name_and_address_is_only_candidate(rules: Rules) -> None:
    a, b = (
        entry(longitude=None, latitude=None),
        entry("osm", "node/1", longitude=None, latitude=None),
    )
    pairs, groups, _ = resolve([a, b], rules, Reviews(version="test"))
    assert pairs[0].method == "same_name_address" and len(groups) == 2


def test_source_id_reuse_is_held_for_new_generation_review(rules: Rules) -> None:
    old = entry()
    changed = entry(
        name="Another Business", longitude=-0.2, postcode="BN22AA", record_hash="d" * 64
    )
    _, _, flags = resolve([changed], rules, Reviews(version="test"), {old.key: old})
    assert "possible_source_id_reuse_new_generation_review_required" in flags[old.key]


def test_changed_identifier_continuity_is_not_assumed(rules: Rules) -> None:
    old, new = entry(), entry(source_key="2")
    pairs, groups, flags = resolve([old, new], rules, Reviews(version="test"), {old.key: old})
    assert pairs[0].decision == "pending" and len(groups) == 2
    assert all("possible_changed_source_id_continuity" in flags[k] for k in (old.key, new.key))


def test_review_cannot_cross_source_generations(rules: Rules) -> None:
    a, b = entry(), entry("osm", "node/1")
    r = review(a, b)
    with pytest.raises(ValueError, match="identity/generation"):
        resolve(
            [a, b.model_copy(update={"generation": 2})],
            rules,
            Reviews(version="test", reviews=(r,)),
        )

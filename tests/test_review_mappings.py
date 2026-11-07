"""Strict, source-bound review definitions and date-only approval semantics."""

import json
from pathlib import Path
from typing import Any

import pytest
from pydantic import ValidationError

from urbanlens.silver.identity import Entry, Reviews
from urbanlens.silver.review_mappings import Application, CorrectionBatch, DecisionBatch

ROOT = Path(__file__).parents[1]


def application() -> Application:
    return Application(
        Reviews.model_validate_json((ROOT / "config/identity-reviews.json").read_text()),
        DecisionBatch.model_validate_json(
            (ROOT / "config/venue-review-decisions.json").read_text(encoding="utf-8")
        ),
        CorrectionBatch.model_validate_json(
            (ROOT / "config/venue-review-corrections.json").read_text(encoding="utf-8")
        ),
    )


def test_real_mapping_inventory_and_date_only_confirmations() -> None:
    app = application()
    assert len(app.decisions.reviews) == 421 and len(app.corrections.corrections) == 14
    assert app.decisions.confirmed_on.isoformat() == "2026-11-05"
    assert all(
        r.reviewed_on is None or r.reviewed_on <= r.confirmed_on for r in app.decisions.reviews
    )
    replacement = next(c for c in app.corrections.corrections if c.review_id == "U544")
    assert replacement.field_overrides.replacement_candidate is not None
    assert replacement.field_overrides.replacement_candidate.longitude is None


def test_pair_tampering_and_unknown_mapping_fields_fail() -> None:
    raw = json.loads((ROOT / "config/venue-review-decisions.json").read_text(encoding="utf-8"))
    raw["reviews"][0]["left"]["source_key"] = "999999"
    with pytest.raises(ValidationError, match="pair ID"):
        DecisionBatch.model_validate(raw)
    raw["unknown_override"] = True
    with pytest.raises(ValidationError, match="Extra inputs"):
        DecisionBatch.model_validate(raw)


def test_wrong_generation_or_meaning_cannot_receive_a_review() -> None:
    app = application()
    with pytest.raises(ValueError, match="Stale review"):
        app.bind([], {}, app.decisions.resolution_manifest)


def test_conflicting_review_batch_provenance_fails() -> None:
    app = application()
    corrections = app.corrections.model_copy(
        update={
            "source_review_manifest": app.corrections.source_review_manifest.model_copy(
                update={"sha256": "0" * 64}
            )
        }
    )
    with pytest.raises(ValueError, match="disagree"):
        Application(app.original, app.decisions, corrections)


def test_duplicate_correction_ids_fail() -> None:
    app = application()
    corrections = app.corrections.model_copy(
        update={"corrections": (app.corrections.corrections[0], app.corrections.corrections[0])}
    )
    with pytest.raises(ValueError, match="Duplicate field correction"):
        Application(app.original, app.decisions, corrections)


def test_conflicting_field_overrides_do_not_select_arbitrarily() -> None:
    app = application()
    app.overrides["a"] = {"display_name": ("First", "U099")}
    app.overrides["b"] = {"display_name": ("Second", "U111")}
    with pytest.raises(ValueError, match="Conflicting owner field overrides"):
        app.apply_fields({"field_sources": {}, "location_quality": "missing"}, ["a", "b"])


def test_withheld_address_cannot_be_reconstructed_by_a_review() -> None:
    app = application()
    app.overrides["a"] = {"address": ("Private address", "U701")}
    with pytest.raises(ValueError, match="withheld"):
        app.apply_fields({"field_sources": {}, "location_quality": "withheld"}, ["a"])


def test_scope_only_candidates_do_not_block_a_hospitality_record() -> None:
    from urbanlens.silver.identity import Pair

    app = application()
    app.inactive = {"b": "retail excluded"}
    pair = Pair(
        pair_id="a" * 64,
        left="a",
        right="b",
        left_hash="b" * 64,
        right_hash="c" * 64,
        decision="pending",
        method="candidate",
        reasons=("candidate",),
        distance_metres=1,
        name_similarity=1,
    )
    flags = {"a": ["unresolved_duplicate_candidate"], "b": ["unresolved_duplicate_candidate"]}
    app.adjust_flags([pair], [["a"], ["b"]], flags)
    assert flags["a"] == []


def test_name_override_records_its_own_approval_reference() -> None:
    app = application()
    app.overrides["source"] = {"display_name": ("Current name", "U099")}
    body: dict[str, Any] = {
        "display_name": "Old name",
        "field_sources": {},
        "location_quality": "source_point",
    }
    app.apply_fields(body, ["source"])
    assert body["display_name"] == "Current name"
    assert body["field_reviews"]["display_name"] == "correction/U099"


def test_finite_replacement_schema_rejects_invented_coordinates() -> None:
    app = application()
    raw = app.corrections.model_dump(mode="json")
    correction = next(c for c in raw["corrections"] if c["review_id"] == "U544")
    correction["field_overrides"]["replacement_candidate"]["longitude"] = -0.14
    with pytest.raises(ValidationError):
        CorrectionBatch.model_validate(raw)


@pytest.mark.parametrize(
    "tags,excluded",
    [
        ({"amenity": ("pharmacy",)}, True),
        ({"shop": ("chemist",)}, True),
        ({"amenity": ("school",)}, True),
        ({"amenity": ("post_office",)}, True),
        ({"amenity": ("fuel",)}, True),
        ({"amenity": ("studio",)}, True),
        ({"tourism": ("hotel",)}, True),
        ({"amenity": ("pub",), "tourism": ("hotel",)}, False),
        ({"amenity": ("bar",)}, False),
        ({"amenity": ("nightclub",)}, False),
        ({"shop": ("bakery",)}, False),
        ({"amenity": ("cafe",), "shop": ("supermarket",)}, False),
    ],
)
def test_scope_policy_excludes_unrelated_types_but_keeps_food_service(
    tags: dict[str, tuple[str, ...]],
    excluded: bool,
) -> None:
    base = application()
    app = Application(
        Reviews(version="test"),
        base.decisions.model_copy(update={"reviews": ()}),
        base.corrections.model_copy(update={"corrections": ()}),
    )
    e = Entry(
        provider="osm",
        scope="test",
        source_key="node/1",
        generation=1,
        record_hash="a" * 64,
        location_hash="b" * 64,
        classification_hash="c" * 64,
        tags=tags,
    )
    rows = {
        e.key: dict(source_fields={}, classification={"selection": {"eligibility": "unresolved"}})
    }
    app.bind([e], rows, app.decisions.resolution_manifest)
    assert (e.key in app.inactive) == excluded

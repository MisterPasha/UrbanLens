"""Location selection preserves uncertainty, privacy, and independent precision gates."""

import pytest

from urbanlens.silver.locations import Candidate, select


def point(lon: float = -0.14, lat: float = 50.82, **kwargs: object) -> Candidate:
    return Candidate.model_validate(
        dict(longitude=lon, latitude=lat, evidence="source:1", **kwargs)
    )


@pytest.mark.parametrize(
    "lon,lat,srid",
    [
        (0, 0, 4326),
        (181, 50, 4326),
        (-0.14, 91, 4326),
        (float("nan"), 50, 4326),
        (float("inf"), 50, 4326),
        (530000, 104000, 27700),
    ],
)
def test_invalid_coordinates_are_quarantined(lon: float, lat: float, srid: int) -> None:
    result = select((point(lon, lat, srid=srid),))
    assert result.quality == "invalid" and result.selected is None and not result.precise


def test_missing_and_approximate_are_not_precise() -> None:
    assert select(()).quality == "missing"
    result = select((point(kind="approximate"),))
    assert result.quality == "approximate" and not result.precise


def test_source_beats_centroid_without_erasing_alternative() -> None:
    source, centroid = point(), point(-0.15, kind="approximate")
    result = select((source, centroid))
    assert result.selected == source and result.precise and len(result.alternatives) == 2


def test_conflicting_points_need_explicit_review() -> None:
    source, other = point(), point(-0.15)
    unresolved = select((source, other))
    assert unresolved.quality == "unresolved" and unresolved.selected is None
    reviewed = point(-0.15, kind="reviewed")
    chosen = select((source, other, reviewed), reviewed_index=2)
    assert chosen.quality == "reviewed" and chosen.selected == reviewed
    assert len(chosen.alternatives) == 3
    with pytest.raises(ValueError, match="Review selection"):
        select((source,), reviewed_index=2)
    with pytest.raises(ValueError, match="Reviewed coordinate"):
        select((source,), reviewed_index=0)


def test_withheld_cannot_be_reconstructed() -> None:
    result = select((point(), point(kind="approximate")), withheld=True)
    assert result.quality == "withheld" and result.selected is None and not result.alternatives


def test_same_point_is_not_a_conflict() -> None:
    assert select((point(), point())).precise

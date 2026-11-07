"""Consequential legal/cohort semantics independent of live providers."""

from datetime import UTC, datetime
from typing import Any
from uuid import uuid4

from urbanlens.silver.companies import context


def test_newer_outside_profile_cannot_keep_old_local_office_membership() -> None:
    def row(*, profile: bool, postcode: str, month: int) -> dict[str, Any]:
        return dict(
            observation_id=uuid4(),
            source_key="00000001",
            record_hash="a" * 64,
            scope_key="profile" if profile else "bulk",
            name_normalised="Example Ltd",
            address_normalised="Registered office",
            postcode_normalised=postcode,
            address_state="published",
            first_observed_at=datetime(2026, 10, 1, tzinfo=UTC),
            last_observed_at=datetime(2026, month, 1, tzinfo=UTC),
            source_fields=dict(
                company=dict(
                    profile=profile,
                    registered_office_cohort=not profile,
                    legal_status={"value": "dissolved"},
                    company_type={"value": "ltd"},
                    sic_codes=["56101"],
                    sic_state="published",
                    incorporated_on={"value": "2001-01-01", "state": "published"},
                    ceased_on={"value": None, "state": "missing"},
                ),
                silver_artifact={"uri": "s3://synthetic/retained"},
                field_sources={},
            ),
        )

    bulk = row(profile=False, postcode="BN1 1AA", month=10)
    newer = row(profile=True, postcode="SW1A 1AA", month=11)
    assert context([bulk])["registered_office_cohort"]
    result = context([bulk, newer])
    assert not result["registered_office_cohort"]
    assert result["registered_office"]["postcode"] == "SW1A 1AA"
    assert result["cessation_interpretation"] == "date_unavailable"
    assert result["incorporation"]["value"] == "2001-01-01"
    assert len(result["sources"]) == 2 and result["warnings"]

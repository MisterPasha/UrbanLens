"""Publication boundaries: withheld data, exact machine-readable copies and tampering."""

import io
import json
import zipfile

import pytest

from urbanlens.reuse import archive, encoded, inspect, venue, verify


def test_withheld_location_and_private_source_notes_cannot_escape() -> None:
    row = dict(
        establishment_id="venue",
        location_quality="withheld",
        address="PRIVATE ADDRESS",
        postcode="PRIVATE POSTCODE",
        area_code="PRIVATE AREA",
        longitude=1.2345,
        latitude=5.4321,
        resolution_artifact_id="private",
        sources=[
            dict(
                provider="fsa",
                key="00123",
                tags={"phone": "PRIVATE PHONE"},
                normalised_artifact="s3://private/object",
            )
        ],
        provenance={"address": dict(reason="PRIVATE ADDRESS", detail={"value": "PRIVATE ADDRESS"})},
        verified_operators=[],
    )
    public = venue(row)
    body = encoded(public)
    assert b"PRIVATE" not in body and b"s3://" not in body and b"1.2345" not in body
    assert public["sources"][0]["source_key"] == "00123"
    assert public["provenance"]["address"]["reason"] == "Source withheld"
    inspect(public)


def test_publication_refuses_private_evidence_contacts_and_ni_geography() -> None:
    for value in (
        {"evidence_uri": "s3://bucket/private"},
        {"nested": {"email": "private"}},
        {"postcode": "BT1 1AA"},
        {"reason": "file:///private/review"},
    ):
        with pytest.raises(ValueError):
            inspect(value)


def test_archive_preserves_identifiers_nested_values_nulls_and_detects_tampering() -> None:
    tables = {
        "companies": [
            dict(number="00001234", name="Café, Ltd", missing=None, flags=["a", "b"], valid=False)
        ]
    }
    first = archive(tables, {"release_id": "test"}, {"NOTICES.txt": b"source notices"})
    assert first == archive(tables, {"release_id": "test"}, {"NOTICES.txt": b"source notices"})
    assert verify(first)["release_id"] == "test"
    with zipfile.ZipFile(io.BytesIO(first)) as zipped:
        record = json.loads(zipped.read("companies.jsonl"))
        assert record["number"] == "00001234" and record["missing"] is None
        files = {n: zipped.read(n) for n in zipped.namelist()}
    files["companies.csv"] = b"modified,contents\n"
    output = io.BytesIO()
    with zipfile.ZipFile(output, "w") as zipped:
        for name, data in files.items():
            zipped.writestr(name, data)
    with pytest.raises(ValueError, match="checksum"):
        verify(output.getvalue())

"""Synthetic source contracts; no credentials or real company records."""

import io
import json
import urllib.error
from datetime import UTC, datetime, timedelta
from email.message import Message
from pathlib import Path
from typing import Any
from unittest.mock import patch

import pytest

from scripts import company_pilot
from scripts.company_pilot import (
    batch_values,
    cache_entry,
    cache_fresh,
    parse_batch,
    parts_from_listing,
    request_postcodes,
    sic_codes,
)


def listing(parts: list[int], edition: str = "2026-11-01") -> str:
    return "".join(f'href="BasicCompanyData-{edition}-part{p}_2.zip"' for p in parts)


@pytest.mark.parametrize(
    "html", [listing([1]), listing([1, 1]), listing([1]) + listing([2], "2026-10-01")]
)
def test_reject_incomplete_duplicate_or_mixed_parts(html: str) -> None:
    with pytest.raises(ValueError):
        parts_from_listing(html)


def test_order_parts_and_preserve_number_text() -> None:
    assert [p[2] for p in parts_from_listing(listing([2, 1]))] == [1, 2]
    import csv

    row = next(csv.DictReader(io.StringIO(" CompanyNumber,SICCode.SicText_1\n00778921,56101\n")))
    assert {k.strip(): v for k, v in row.items()}["CompanyNumber"] == "00778921"


def test_all_four_sics_and_no_numeric_coercion() -> None:
    row = {
        f"SICCode.SicText_{i}": value
        for i, value in enumerate(
            ["99999 - Other", "56101 - Restaurant", "10710 - Bakery", "56302 - Pub"], 1
        )
    }
    assert sic_codes(row) == ["99999", "56101", "10710", "56302"]


def test_deduplicate_normalise_and_chunk_at_100() -> None:
    batches = batch_values(["bn1 1je", "BN11JE"] + [str(i) for i in range(200)])
    assert [len(b) for b in batches] == [100, 100, 1]
    assert sum("BN11JE" in b for b in batches) == 1


def test_positive_and_negative_cache_expiry_boundaries() -> None:
    now = datetime(2026, 11, 2, tzinfo=UTC)
    cases: list[tuple[dict[str, Any] | None, int]] = [({}, 30), (None, 1)]
    for result, days in cases:
        entry = cache_entry(result, now)
        assert cache_fresh(entry, now + timedelta(days=days, microseconds=-1))
        assert not cache_fresh(entry, now + timedelta(days=days))


def test_null_is_distinct_from_a_resolved_outside_postcode() -> None:
    outside = {"postcode": "SW1A 1AA", "codes": {"admin_district": "E09000033"}, "quality": 1}
    payload = {
        "status": 200,
        "result": [{"query": "SW1A1AA", "result": outside}, {"query": "UNKNOWN", "result": None}],
    }
    result = parse_batch(payload, ["SW1A1AA", "UNKNOWN"])
    assert result["SW1A1AA"] == outside
    assert result["UNKNOWN"] is None


@pytest.mark.parametrize(
    "items", [[], [{"query": "OTHER", "result": None}], [{"query": "UNKNOWN", "result": None}] * 2]
)
def test_reject_partial_unexpected_or_duplicate_results(items: list[Any]) -> None:
    with pytest.raises(ValueError):
        parse_batch({"status": 200, "result": items}, ["UNKNOWN"])


def test_reject_oversized_batch_before_accepting_response() -> None:
    with pytest.raises(ValueError, match="100"):
        parse_batch({"status": 200, "result": []}, [str(i) for i in range(101)])


def test_unavailable_has_two_attempts_and_never_returns_null() -> None:
    with patch("urllib.request.urlopen", side_effect=urllib.error.URLError("offline")) as call:
        with patch("time.sleep") as sleep:
            with pytest.raises(RuntimeError, match="unavailable"):
                request_postcodes(b"{}")
    assert call.call_count == 2
    sleep.assert_called_once_with(2.0)


def test_retry_after_is_honoured_then_success() -> None:
    headers = Message()
    headers["Retry-After"] = "4"
    error = urllib.error.HTTPError("https://example.invalid", 429, "throttle", headers, None)
    with patch("urllib.request.urlopen", side_effect=[error, io.BytesIO(b'{"status":200}')]):
        with patch("time.sleep") as sleep:
            assert json.loads(request_postcodes(b"{}"))["status"] == 200
    sleep.assert_called_once_with(4.0)


def test_long_retry_after_stops_without_retrying_early() -> None:
    headers = Message()
    headers["Retry-After"] = "120"
    error = urllib.error.HTTPError("https://example.invalid", 429, "throttle", headers, None)
    with patch("urllib.request.urlopen", side_effect=error) as call, patch("time.sleep") as sleep:
        with pytest.raises(ValueError, match="wait budget"):
            request_postcodes(b"{}")
    assert call.call_count == 1
    sleep.assert_not_called()


def test_permanent_http_failure_is_not_retried() -> None:
    error = urllib.error.HTTPError("https://example.invalid", 400, "bad", Message(), None)
    with patch("urllib.request.urlopen", side_effect=error) as call:
        with pytest.raises(RuntimeError, match="400"):
            request_postcodes(b"{}")
    assert call.call_count == 1


def test_expired_response_refreshes_and_retains_old_evidence(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    import hashlib

    monkeypatch.setattr(company_pilot, "RAW", tmp_path)
    monkeypatch.setattr(company_pilot, "ROOT", tmp_path)
    monkeypatch.setattr(company_pilot, "local_directory", lambda: {})
    target = tmp_path / "reference/geography"
    target.mkdir(parents=True)
    (target / "target-areas.csv").write_text("LSOA21CD\nE01000001\n", encoding="utf-8")
    (tmp_path / "postcode-candidates.csv").write_text(
        "company_number,company_name,postcode\n00000001,Example,BN11JE\n", encoding="utf-8"
    )
    old = datetime.now(UTC) - timedelta(days=31)
    cache_path = tmp_path / "postcode-cache.json"
    company_pilot.write_json(cache_path, {"BN11JE": cache_entry(None, old)})
    body = json.dumps({"postcodes": ["BN11JE"]}, separators=(",", ":")).encode()
    saved = tmp_path / ("postcodes-" + hashlib.sha256(body).hexdigest()[:16] + ".json")
    company_pilot.write_json(
        saved, {"status": 200, "result": [{"query": "BN11JE", "result": None}]}
    )
    company_pilot.write_json(
        saved.with_suffix(".meta.json"),
        {"fetched_at": old.isoformat(), "sha256": company_pilot.sha(saved)},
    )
    value = {
        "postcode": "BN1 1JE",
        "quality": 1,
        "codes": {"admin_district": company_pilot.LAD, "lsoa21": "E01000001"},
    }
    response = json.dumps({"status": 200, "result": [{"query": "BN11JE", "result": value}]})
    with patch.object(company_pilot, "request_postcodes", return_value=response.encode()) as call:
        company_pilot.enrich()
        assert call.call_count == 1
        company_pilot.enrich()
        assert call.call_count == 1  # Fresh cache replay needs no network.
    assert company_pilot.read_json(cache_path)["BN11JE"]["result"] == value
    assert len(list(tmp_path.glob(saved.stem + "-*.json"))) == 2  # Body and metadata retained.

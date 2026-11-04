"""Postcode input, provider failures, vintage reconciliation and bounded durable caching."""

import json
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import httpx
import pytest

from urbanlens.ingestion.contracts import Provider
from urbanlens.ingestion.http import HttpDownloader, HttpPolicy
from urbanlens.ingestion.postcodes import PostcodeService
from urbanlens.ingestion.storage import LocalArtifactStore

NOW = datetime(2026, 11, 4, 12, tzinfo=UTC)
AREA = "E01016849"


def value(key: str, **updates: Any) -> dict[str, Any]:
    return {
        "postcode": key,
        "quality": 1,
        "longitude": -0.14,
        "latitude": 50.83,
        "codes": {"admin_district": "E06000043", "lsoa21": AREA},
        **updates,
    }


def service(tmp_path: Path, handler: Any) -> PostcodeService:
    client = HttpDownloader(
        Provider.POSTCODES,
        HttpPolicy(max_requests=20, max_retries=0),
        transport=httpx.MockTransport(handler),
    )
    return PostcodeService(
        tmp_path / "cache.sqlite", LocalArtifactStore(tmp_path / "objects"), {AREA}, client
    )


def response(items: list[dict[str, Any]]) -> httpx.Response:
    return httpx.Response(
        200, stream=httpx.ByteStream(json.dumps({"status": 200, "result": items}).encode())
    )


@pytest.fixture(autouse=True)
def fast(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("urbanlens.ingestion.http.RATES", {p: 100_000.0 for p in Provider})


def test_unique_inputs_preserve_raw_values_and_reopen_cache_without_network(tmp_path: Path) -> None:
    calls = []

    def handler(request: httpx.Request) -> httpx.Response:
        batch = json.loads(request.content)["postcodes"]
        calls.append(batch)
        return response([{"query": k, "result": value(k)} for k in batch])

    original = service(tmp_path, handler)
    first = original.bulk(["bn1 1je", "BN11JE"], now=NOW)
    assert calls == [["BN11JE"]] and [r.raw_input for r in first] == ["bn1 1je", "BN11JE"]
    assert all(
        r.state == "resolved" and r.approximate and not r.primary_spatial_eligible for r in first
    )
    reopened = service(tmp_path, lambda _: pytest.fail("Cache must prevent requests"))
    cached = reopened.lookup("BN1 1JE", now=NOW + timedelta(days=29))
    assert (
        cached.cache_hit
        and cached.fetched_at == NOW
        and cached.expires_at == NOW + timedelta(days=30)
    )
    assert cached.can_define_search and not cached.primary_spatial_eligible
    assert cached.source_edition is None and cached.artifact == first[0].artifact


def test_invalid_input_does_not_call_provider_and_null_is_not_an_outage(tmp_path: Path) -> None:
    client = service(
        tmp_path,
        lambda request: response(
            [{"query": k, "result": None} for k in json.loads(request.content)["postcodes"]]
        ),
    )
    rows = client.bulk(["junk", "ZZ99 9ZZ", "BN99ZZ"], now=NOW)
    assert [r.state for r in rows] == ["invalid", "invalid", "unmatched"]
    assert client.downloader.requests == 1
    assert client.lookup("BN99ZZ", now=NOW + timedelta(hours=23)).cache_hit
    assert not client.lookup("BN99ZZ", now=NOW + timedelta(days=1)).cache_hit


@pytest.mark.parametrize(
    "codes,state",
    [
        ({"admin_district": "E09000033", "lsoa21": "E01000001"}, "outside"),
        ({"admin_district": "E06000043", "lsoa": AREA}, "incompatible-geography"),
        ({"admin_district": "E06000043", "lsoa21": "E01000000"}, "incompatible-geography"),
    ],
)
def test_district_and_explicit_2021_membership_are_independent(
    tmp_path: Path, codes: dict[str, str], state: str
) -> None:
    client = service(
        tmp_path, lambda _: response([{"query": "BN11JE", "result": value("BN11JE", codes=codes)}])
    )
    row = client.lookup("BN11JE", now=NOW)
    assert row.state == state and not row.can_define_search


@pytest.mark.parametrize(
    "status,state", [(429, "throttled"), (503, "unavailable"), (401, "unavailable")]
)
def test_provider_failure_is_not_negative_cached(tmp_path: Path, status: int, state: str) -> None:
    client = service(
        tmp_path, lambda _: httpx.Response(status, stream=httpx.ByteStream(b"private error body"))
    )
    row = client.lookup("BN11JE", now=NOW)
    assert row.state == state and client.cached("BN11JE", NOW) is None and row.artifact is None


@pytest.mark.parametrize(
    "kind",
    ["missing", "duplicate", "wrong-query", "wrong-identity", "bad-coordinates", "non-object"],
)
def test_incomplete_or_malformed_batch_never_partially_enters_cache(
    tmp_path: Path, kind: str
) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        items = [{"query": k, "result": value(k)} for k in json.loads(request.content)["postcodes"]]
        if kind == "missing":
            items.pop()
        if kind == "duplicate":
            items.append(items[0])
        if kind == "wrong-query":
            items[0]["query"] = "BN99ZZ"
        if kind == "wrong-identity":
            items[0]["result"]["postcode"] = "BN99ZZ"
        if kind == "bad-coordinates":
            items[0]["result"]["longitude"] = float("nan")
        if kind == "non-object":
            return httpx.Response(200, stream=httpx.ByteStream(b"[]"))
        return response(items)

    client = service(tmp_path, handler)
    rows = client.bulk(["BN11JE", "BN12AA"], now=NOW)
    assert all(r.state == "unavailable" for r in rows)
    assert client.cached("BN11JE", NOW) is None and client.cached("BN12AA", NOW) is None
    assert list((tmp_path / "objects").rglob("response.json.gz"))


def test_bulk_partitions_at_one_hundred_and_expires_positive_at_boundary(tmp_path: Path) -> None:
    batches = []

    def handler(request: httpx.Request) -> httpx.Response:
        batch = json.loads(request.content)["postcodes"]
        batches.append(batch)
        return response([{"query": k, "result": value(k)} for k in batch])

    client = service(tmp_path, handler)
    letters = "ABDEFGHJLNPQRSTUWXYZ"
    keys = [f"BN1{i % 10}{letters[i // 10]}A" for i in range(101)]
    rows = client.bulk(keys, now=NOW)
    assert [len(b) for b in batches] == [100, 1] and all(r.state == "resolved" for r in rows)
    assert client.lookup(keys[0], now=NOW + timedelta(days=30)).cache_hit is False

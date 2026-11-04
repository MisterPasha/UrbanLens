"""Retry/throttle/budget behavior with no provider requests."""

import asyncio
import io
from typing import Any

import httpx
import pytest

from urbanlens.ingestion.contracts import Provider
from urbanlens.ingestion.errors import IngestionError
from urbanlens.ingestion.http import HttpDownloader, HttpPolicy


@pytest.fixture(autouse=True)
def fast_throttle(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("urbanlens.ingestion.http.RATES", {p: 100_000.0 for p in Provider})


def response(
    status: int, content: bytes = b"", *, headers: dict[str, str] | None = None
) -> httpx.Response:
    return httpx.Response(status, stream=httpx.ByteStream(content), headers=headers)


def test_transient_error_retries_but_never_appends_partial_or_error_body() -> None:
    calls = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(request)
        return response(503, b"secret error") if len(calls) == 1 else response(200, b"valid")

    client = HttpDownloader(
        Provider.FSA, HttpPolicy(backoff_seconds=0), transport=httpx.MockTransport(handler)
    )
    target = io.BytesIO()
    client.download("https://example.test/data", target)
    assert target.read() == b"valid" and client.requests == 2
    assert client.bytes == len(b"secret errorvalid")


def test_request_ceiling_includes_every_retry_and_redacts_error_details() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        raise httpx.ReadTimeout("password=secret https://private/?key=secret")

    client = HttpDownloader(
        Provider.FSA,
        HttpPolicy(max_requests=2, max_retries=5, backoff_seconds=0),
        transport=httpx.MockTransport(handler),
    )
    with pytest.raises(IngestionError, match="http-request-budget") as failure:
        client.download("https://example.test/?key=secret", io.BytesIO())
    assert "secret" not in str(failure.value) and client.requests == 2


@pytest.mark.parametrize("status", [401, 403, 404, 302])
def test_permanent_errors_and_redirects_do_not_retry(status: int) -> None:
    client = HttpDownloader(Provider.FSA, transport=httpx.MockTransport(lambda _: response(status)))
    with pytest.raises(IngestionError):
        client.download("https://example.test/data", io.BytesIO())
    assert client.requests == 1


def test_server_retry_after_is_not_shortened_to_our_wait_cap() -> None:
    client = HttpDownloader(
        Provider.OSM,
        HttpPolicy(max_retry_wait=0.1),
        transport=httpx.MockTransport(lambda _: response(429, headers={"retry-after": "60"})),
    )
    with pytest.raises(IngestionError, match="provider-http-429"):
        client.download("https://example.test/data", io.BytesIO())
    assert client.requests == 1


def test_byte_budget_counts_failed_attempt_bodies_too() -> None:
    client = HttpDownloader(
        Provider.FSA,
        HttpPolicy(max_bytes=3, backoff_seconds=0),
        transport=httpx.MockTransport(lambda _: response(503, b"oversized")),
    )
    with pytest.raises(IngestionError, match="http-byte-budget"):
        client.download("https://example.test/data", io.BytesIO())
    assert client.requests == 1


def test_truncated_content_length_retries_instead_of_succeeding() -> None:
    calls = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(request)
        return response(200, b"short", headers={"content-length": "100"})

    client = HttpDownloader(
        Provider.FSA,
        HttpPolicy(max_requests=2, max_retries=1, backoff_seconds=0),
        transport=httpx.MockTransport(handler),
    )
    with pytest.raises(IngestionError, match="provider-network-error"):
        client.download("https://example.test/data", io.BytesIO())
    assert len(calls) == 2


def test_global_runtime_timeout_cancels_a_stalled_stream() -> None:
    class Slow(httpx.AsyncByteStream):
        async def __aiter__(self) -> Any:
            await asyncio.sleep(1)
            yield b"late"

    client = HttpDownloader(
        Provider.FSA,
        HttpPolicy(max_seconds=0.05),
        transport=httpx.MockTransport(lambda _: httpx.Response(200, stream=Slow())),
    )
    with pytest.raises(IngestionError, match="http-runtime-budget"):
        client.download("https://example.test/data", io.BytesIO())


def test_request_throttle_applies_between_retries(monkeypatch: pytest.MonkeyPatch) -> None:
    import time

    monkeypatch.setattr("urbanlens.ingestion.http.RATES", {p: 20.0 for p in Provider})
    starts = []

    def handler(request: httpx.Request) -> httpx.Response:
        starts.append(time.monotonic())
        return response(503) if len(starts) == 1 else response(200)

    client = HttpDownloader(
        Provider.FSA, HttpPolicy(backoff_seconds=0), transport=httpx.MockTransport(handler)
    )
    client.download("https://example.test/data", io.BytesIO())
    assert starts[1] - starts[0] >= 0.045


def test_opt_in_relative_redirect_uses_request_budget_without_consuming_retries() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return (
            response(307, headers={"location": "/new"})
            if request.url.path == "/old"
            else response(200, b"valid")
        )

    client = HttpDownloader(
        Provider.FSA,
        HttpPolicy(max_redirects=1, max_retries=0),
        transport=httpx.MockTransport(handler),
    )
    target = io.BytesIO()
    client.download("https://example.test/old", target)
    assert target.read() == b"valid" and client.requests == 2


@pytest.mark.parametrize(
    "location",
    [
        "https://other.test/data",
        "http://example.test/data",
        "https://example.test:444/data",
        "https://user:secret@example.test/data",
    ],
)
def test_redirect_cannot_forward_headers_to_another_origin(location: str) -> None:
    client = HttpDownloader(
        Provider.FSA,
        HttpPolicy(max_redirects=2),
        transport=httpx.MockTransport(lambda _: response(307, headers={"location": location})),
    )
    with pytest.raises(IngestionError, match="unsafe-http-redirect"):
        client.download("https://example.test/data", io.BytesIO())
    assert client.requests == 1


@pytest.mark.parametrize(
    "policy,code,requests",
    [
        (HttpPolicy(max_redirects=1), "http-redirect-budget", 2),
        (HttpPolicy(max_redirects=5, max_requests=1), "http-request-budget", 1),
    ],
)
def test_redirect_loops_are_bounded(policy: HttpPolicy, code: str, requests: int) -> None:
    client = HttpDownloader(
        Provider.FSA,
        policy,
        transport=httpx.MockTransport(lambda _: response(307, headers={"location": "/loop"})),
    )
    with pytest.raises(IngestionError, match=code):
        client.download("https://example.test/data", io.BytesIO())
    assert client.requests == requests


def test_post_retries_preserve_query_body_and_request_budget() -> None:
    requests = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return response(503) if len(requests) == 1 else response(200, b"valid")

    client = HttpDownloader(
        Provider.OSM, HttpPolicy(backoff_seconds=0), transport=httpx.MockTransport(handler)
    )
    client.download(
        "https://example.test/interpreter",
        io.BytesIO(),
        method="POST",
        content=b"data=synthetic-query",
    )
    assert len(requests) == 2
    assert all(r.method == "POST" and r.content == b"data=synthetic-query" for r in requests)

"""Bounded HTTPS downloads. Retry bytes, requests, throttles and waits share one budget."""

from __future__ import annotations

import asyncio
import random
import ssl
import time
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from email.utils import parsedate_to_datetime
from typing import Literal, Protocol
from urllib.parse import urljoin, urlsplit

import httpx

from urbanlens.ingestion.contracts import FailureClass, Provider
from urbanlens.ingestion.errors import IngestionError

RATES = {
    Provider.FSA: 2.0,
    Provider.OSM: 0.5,
    Provider.COMPANIES_HOUSE: 1.5,
    Provider.POSTCODES: 2.0,
    Provider.ONS: 1.0,
}


class DownloadTarget(Protocol):
    def write(self, data: bytes, /) -> int: ...
    def seek(self, offset: int, /) -> int: ...
    def truncate(self, size: int | None = None, /) -> int: ...


@dataclass(frozen=True)
class HttpPolicy:
    max_requests: int = 3
    max_bytes: int = 100 * 1024 * 1024
    max_seconds: float = 120
    timeout_seconds: float = 20
    max_retries: int = 2
    backoff_seconds: float = 0.5
    max_retry_wait: float = 30
    max_redirects: int = 0

    def __post_init__(self) -> None:
        if min(self.max_requests, self.max_bytes, self.max_seconds, self.timeout_seconds) <= 0:
            raise ValueError("HTTP budgets must be positive")
        if min(self.max_retries, self.backoff_seconds, self.max_retry_wait, self.max_redirects) < 0:
            raise ValueError("Retry policy cannot be negative")


class HttpDownloader:
    def __init__(
        self,
        provider: Provider,
        policy: HttpPolicy | None = None,
        *,
        transport: httpx.AsyncBaseTransport | None = None,
        headers: Mapping[str, str] | None = None,
        before_request: Callable[[], None] | None = None,
    ) -> None:
        self.provider, self.policy, self.transport = provider, policy or HttpPolicy(), transport
        self.headers = dict(headers or {})
        self.before_request = before_request
        self.requests, self.bytes = 0, 0
        self.started = time.monotonic()
        self.next_request_at = 0.0

    def download(
        self,
        url: str,
        target: DownloadTarget,
        *,
        method: Literal["GET", "POST"] = "GET",
        content: bytes | None = None,
    ) -> None:
        parsed = urlsplit(url)
        if parsed.scheme != "https" or not parsed.hostname or parsed.username or parsed.password:
            raise ValueError("Source URL must be HTTPS without embedded credentials")
        remaining = self.policy.max_seconds - (time.monotonic() - self.started)
        if remaining <= 0:
            raise IngestionError(FailureClass.BUDGET, "http-runtime-budget")
        try:
            asyncio.run(self._bounded(url, target, remaining, method, content))
        except TimeoutError:
            raise IngestionError(FailureClass.BUDGET, "http-runtime-budget") from None

    async def _bounded(
        self, url: str, target: DownloadTarget, remaining: float, method: str, content: bytes | None
    ) -> None:
        async with asyncio.timeout(remaining):
            async with httpx.AsyncClient(
                transport=self.transport,
                verify=ssl.create_default_context(),
                follow_redirects=False,
                timeout=min(self.policy.timeout_seconds, remaining),
                headers={
                    "Accept-Encoding": "identity",
                    "User-Agent": "UrbanLens/0.1 ingestion",
                    **self.headers,
                },
            ) as client:
                attempt, redirects, current_url = 0, 0, url
                origin = urlsplit(url)
                while True:
                    await asyncio.sleep(max(0, self.next_request_at - time.monotonic()))
                    if self.requests >= self.policy.max_requests:
                        raise IngestionError(FailureClass.BUDGET, "http-request-budget")
                    if self.before_request is not None:
                        self.before_request()
                    self.requests += 1
                    self.next_request_at = time.monotonic() + 1 / RATES[self.provider]
                    target.seek(0)
                    target.truncate()
                    wait = self.policy.backoff_seconds * 2**attempt * random.uniform(0.5, 1.5)
                    failure: IngestionError
                    try:
                        async with client.stream(method, current_url, content=content) as response:
                            if (
                                response.status_code in {301, 302, 303, 307, 308}
                                and self.policy.max_redirects
                                and method == "GET"
                            ):
                                location = response.headers.get("location")
                                destination = urlsplit(urljoin(current_url, location or ""))
                                if (
                                    not location
                                    or destination.scheme != "https"
                                    or destination.hostname != origin.hostname
                                    or (destination.port or 443) != (origin.port or 443)
                                    or destination.username
                                    or destination.password
                                    or destination.fragment
                                ):
                                    raise IngestionError(FailureClass.SCOPE, "unsafe-http-redirect")
                                if redirects >= self.policy.max_redirects:
                                    raise IngestionError(
                                        FailureClass.BUDGET, "http-redirect-budget"
                                    )
                                redirects += 1
                                current_url = destination.geturl()
                                continue
                            if response.status_code == 429 or 500 <= response.status_code <= 599:
                                category = (
                                    FailureClass.RATE_LIMIT
                                    if response.status_code == 429
                                    else FailureClass.NETWORK
                                )
                                failure = IngestionError(
                                    category,
                                    f"provider-http-{response.status_code}",
                                    retryable=True,
                                )
                                retry_after = response.headers.get("retry-after")
                                if retry_after:
                                    try:
                                        seconds = float(retry_after)
                                    except ValueError:
                                        try:
                                            date = parsedate_to_datetime(retry_after)
                                            seconds = (date - datetime.now(UTC)).total_seconds()
                                        except (ValueError, TypeError, OverflowError):
                                            seconds = 0
                                    wait = max(wait, seconds)
                                # Drain only within the same total byte budget; never log a body.
                                async for block in response.aiter_raw():
                                    self.consume(len(block))
                            elif not 200 <= response.status_code < 300:
                                category = (
                                    FailureClass.AUTHENTICATION
                                    if response.status_code in {401, 403}
                                    else FailureClass.SCHEMA
                                )
                                raise IngestionError(
                                    category, f"provider-http-{response.status_code}"
                                )
                            else:
                                if (
                                    response.headers.get("content-encoding", "identity").lower()
                                    != "identity"
                                ):
                                    raise IngestionError(
                                        FailureClass.SCHEMA, "unexpected-http-encoding"
                                    )
                                received = 0
                                expected_length = response.headers.get("content-length")
                                async for block in response.aiter_raw():
                                    self.consume(len(block))
                                    target.write(block)
                                    received += len(block)
                                if expected_length is not None:
                                    try:
                                        if int(expected_length) != received:
                                            raise httpx.RemoteProtocolError("Truncated response")
                                    except ValueError:
                                        raise IngestionError(
                                            FailureClass.SCHEMA, "invalid-content-length"
                                        ) from None
                                target.seek(0)
                                return
                    except httpx.TimeoutException:
                        failure = IngestionError(
                            FailureClass.TIMEOUT, "provider-timeout", retryable=True
                        )
                    except httpx.TransportError:
                        failure = IngestionError(
                            FailureClass.NETWORK, "provider-network-error", retryable=True
                        )
                    if attempt == self.policy.max_retries or wait > self.policy.max_retry_wait:
                        raise failure
                    await asyncio.sleep(wait)
                    attempt += 1

    def consume(self, size: int) -> None:
        self.bytes += size
        if self.bytes > self.policy.max_bytes:
            raise IngestionError(FailureClass.BUDGET, "http-byte-budget")

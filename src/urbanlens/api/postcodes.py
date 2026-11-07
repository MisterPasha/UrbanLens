"""Bounded interactive cache; never grants write access to the analytical API login."""

from __future__ import annotations

import asyncio
import json
import sqlite3
import ssl
import threading
import time
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import httpx

from urbanlens.api.repository import ApiError
from urbanlens.ingestion.postcodes import POSTCODE, normalize, validate_result


class Postcodes:
    def __init__(self, path: Path, transport: httpx.AsyncBaseTransport | None = None) -> None:
        self.path, self.transport = path, transport
        self.lock = threading.Lock()
        self.calls: list[float] = []

    async def fetch(self, key: str) -> tuple[int, dict[str, Any]]:
        async with (
            asyncio.timeout(4),
            httpx.AsyncClient(
                transport=self.transport,
                timeout=3,
                verify=ssl.create_default_context(),
                follow_redirects=False,
            ) as client,
        ):
            async with client.stream(
                "GET",
                "https://api.postcodes.io/postcodes/" + key,
                headers={"Accept": "application/json"},
            ) as response:
                if response.status_code == 429:
                    return 429, {}
                if response.status_code not in (200, 404):
                    raise ValueError("Provider response failed")
                chunks = bytearray()
                async for chunk in response.aiter_bytes():
                    chunks.extend(chunk)
                    if len(chunks) > 32768:
                        raise ValueError("Provider response exceeded budget")
                document = json.loads(chunks)
                if not isinstance(document, dict):
                    raise ValueError("Invalid provider document")
                return response.status_code, document

    def lookup(self, postcode: str) -> dict[str, Any]:
        key = normalize(postcode)
        if not POSTCODE.fullmatch(key):
            raise ApiError(422, "invalid_postcode", "Enter a valid full UK postcode")
        now = datetime.now(UTC)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        if not self.lock.acquire(blocking=False):
            return dict(postcode=key, state="throttled", cache_hit=False)
        try:
            return self.cached_lookup(key, now)
        finally:
            self.lock.release()

    def cached_lookup(self, key: str, now: datetime) -> dict[str, Any]:
        with sqlite3.connect(self.path, timeout=2) as c:
            c.execute(
                "CREATE TABLE IF NOT EXISTS cache(postcode TEXT PRIMARY"
                " KEY,result TEXT,expires_at TEXT NOT NULL)"
            )
            row = c.execute(
                "SELECT result FROM cache WHERE postcode=? AND expires_at>?", (key, now.isoformat())
            ).fetchone()
            if row:
                return dict(json.loads(row[0]), cache_hit=True)
            stamp = time.monotonic()
            self.calls = [t for t in self.calls if stamp - t < 60]
            if len(self.calls) >= 30:
                return dict(postcode=key, state="throttled", cache_hit=False)
            self.calls.append(stamp)
            try:
                status, document = asyncio.run(self.fetch(key))
                if status == 429:
                    return dict(postcode=key, state="throttled", cache_hit=False)
                if status == 404:
                    if document.get("status") != 404:
                        raise ValueError("Invalid negative response")
                    value = None
                else:
                    if document.get("status") != 200:
                        raise ValueError("Invalid provider response")
                    value = document.get("result")
                    if value is None:
                        raise ValueError("Missing provider result")
                    validate_result(value, key)
                expires = now + timedelta(days=30 if value else 1)
                result = dict(
                    postcode=key,
                    state="resolved" if value else "unmatched",
                    fetched_at=now.isoformat(),
                    expires_at=expires.isoformat(),
                    cache_hit=False,
                )
                if value:
                    result.update(
                        longitude=value["longitude"],
                        latitude=value["latitude"],
                        positional_quality=value["quality"],
                        authority=value["codes"].get("admin_district"),
                        area_code=value["codes"].get("lsoa21"),
                    )
                c.execute(
                    (
                        "INSERT INTO cache VALUES (?,?,?) ON CONFLICT(postcode) DO UPDATE"
                        " SET result=excluded.result,expires_at=excluded.expires_at"
                    ),
                    (key, json.dumps(result), expires.isoformat()),
                )
                c.execute("DELETE FROM cache WHERE expires_at<=?", (now.isoformat(),))
                c.execute(
                    "DELETE FROM cache WHERE postcode IN (SELECT postcode FROM cache"
                    " ORDER BY expires_at DESC LIMIT -1 OFFSET 1000)"
                )
                return result
            except (httpx.HTTPError, ValueError, KeyError, TypeError, TimeoutError):
                return dict(postcode=key, state="provider_failure", cache_hit=False)

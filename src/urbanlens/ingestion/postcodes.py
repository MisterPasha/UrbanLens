"""Backend postcode lookup: bounded provider work, durable cache, explicit geography states."""

from __future__ import annotations

import hashlib
import json
import math
import re
import sqlite3
import tempfile
from contextlib import closing
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, Literal
from uuid import NAMESPACE_URL, uuid4, uuid5

from urbanlens.ingestion.cadence import month_window
from urbanlens.ingestion.contracts import Artifact, CollectionCycle, Contract, Provider
from urbanlens.ingestion.errors import IngestionError
from urbanlens.ingestion.http import HttpDownloader, HttpPolicy
from urbanlens.ingestion.interfaces import ArtifactStore, Layer, artifact_key
from urbanlens.ingestion.storage import chunks, gzip_chunks

POSTCODE = re.compile(
    r"(?:GIR0AA|(?:[A-PR-UWYZ][0-9][0-9A-HJKPSTUW]?|[A-PR-UWYZ][A-HK-Y][0-9][0-9ABEHMNPRVWXY]?)[0-9][ABD-HJLNP-UW-Z]{2})"
)
EDITION = "Provider response omits dataset edition; do not infer vintage from lookup time"
State = Literal[
    "invalid",
    "unmatched",
    "outside",
    "incompatible-geography",
    "resolved",
    "throttled",
    "unavailable",
]


def normalize(value: str) -> str:
    return "".join(value.upper().split())


class CacheEntry(Contract):
    postcode: str
    result: dict[str, Any] | None
    fetched_at: datetime
    expires_at: datetime
    artifact: Artifact
    edition_note: str = EDITION


class Lookup(Contract):
    raw_input: str
    postcode: str
    state: State
    cache_hit: bool = False
    fetched_at: datetime | None = None
    expires_at: datetime | None = None
    source_edition: str | None = None
    source_edition_note: str = EDITION
    artifact: Artifact | None = None
    longitude: float | None = None
    latitude: float | None = None
    quality: int | None = None
    authority: str | None = None
    lsoa21: str | None = None
    provider_result: dict[str, Any] | None = None
    location_role: Literal["search-centre", "establishment-fallback"] = "search-centre"
    approximate: bool = True
    primary_spatial_eligible: bool = False
    can_define_search: bool = False


def validate_result(value: Any, key: str) -> None:
    if value is None:
        return
    if not isinstance(value, dict) or normalize(str(value.get("postcode", ""))) != key:
        raise ValueError("Postcode/query mismatch")
    if type(value.get("quality")) is not int or not 1 <= value["quality"] <= 9:
        raise ValueError("Unknown quality")
    for field, limit in (("longitude", 180), ("latitude", 90)):
        item = value.get(field)
        if (
            not isinstance(item, (int, float))
            or isinstance(item, bool)
            or not math.isfinite(item)
            or not -limit <= item <= limit
        ):
            raise ValueError("Invalid coordinate")
    if not isinstance(value.get("codes"), dict):
        raise ValueError("Missing geography codes")
    if any(
        value["codes"].get(k) is not None and not isinstance(value["codes"][k], str)
        for k in ("admin_district", "lsoa21")
    ):
        raise ValueError("Invalid geography code type")
    if value.get("dataset_version") is not None and not isinstance(value["dataset_version"], str):
        raise ValueError("Invalid dataset version")


class PostcodeService:
    def __init__(
        self,
        path: Path,
        store: ArtifactStore,
        area_codes: set[str],
        downloader: HttpDownloader | None = None,
    ) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        self.path, self.store, self.area_codes = path, store, frozenset(area_codes)
        self.injected_downloader = downloader is not None
        self.downloader = downloader or HttpDownloader(
            Provider.POSTCODES,
            HttpPolicy(max_requests=50, max_retries=1, max_bytes=10 * 1024 * 1024, max_seconds=240),
            headers={"Content-Type": "application/json", "Accept": "application/json"},
        )
        with closing(sqlite3.connect(path)) as connection:
            connection.execute(
                "CREATE TABLE IF NOT EXISTS postcode_cache "
                "(postcode TEXT PRIMARY KEY, entry TEXT NOT NULL)"
            )
            connection.commit()

    def cached(self, key: str, now: datetime) -> CacheEntry | None:
        with closing(sqlite3.connect(self.path)) as connection:
            row = connection.execute(
                "SELECT entry FROM postcode_cache WHERE postcode=?", (key,)
            ).fetchone()
        if row is None:
            return None
        entry = CacheEntry.model_validate_json(row[0])
        return entry if entry.expires_at > now else None

    def save(self, entry: CacheEntry) -> None:
        with closing(sqlite3.connect(self.path)) as connection:
            connection.execute(
                "INSERT INTO postcode_cache VALUES (?,?) ON CONFLICT(postcode) "
                "DO UPDATE SET entry=excluded.entry",
                (entry.postcode, entry.model_dump_json()),
            )
            connection.commit()

    def lookup(
        self,
        value: str,
        *,
        role: Literal["search-centre", "establishment-fallback"] = "search-centre",
        now: datetime | None = None,
    ) -> Lookup:
        return self.bulk([value], role=role, now=now)[0]

    def bulk(
        self,
        values: list[str],
        *,
        role: Literal["search-centre", "establishment-fallback"] = "establishment-fallback",
        now: datetime | None = None,
    ) -> list[Lookup]:
        now = now or datetime.now(UTC)
        if now.utcoffset() != timedelta(0):
            raise ValueError("Lookup time must be UTC")
        if len(values) > 10000:
            raise ValueError("Lookup work exceeds batch budget")
        keys = {normalize(v) for v in values if POSTCODE.fullmatch(normalize(v))}
        entries = {key: self.cached(key, now) for key in keys}
        hits = {key for key, entry in entries.items() if entry is not None}
        missing = sorted(keys - hits)
        # A long-lived API/cache service gets a fresh bounded job for each miss batch.
        if missing and not self.injected_downloader:
            self.downloader = HttpDownloader(
                Provider.POSTCODES,
                HttpPolicy(
                    max_requests=50, max_retries=1, max_bytes=10 * 1024 * 1024, max_seconds=240
                ),
                headers={"Content-Type": "application/json", "Accept": "application/json"},
            )
        failures: dict[str, State] = {}
        for offset in range(0, len(missing), 100):
            batch = missing[offset : offset + 100]
            body = json.dumps({"postcodes": batch}, separators=(",", ":")).encode()
            start, end = month_window(now.date())
            digest = hashlib.sha256(body).hexdigest()
            cycle = CollectionCycle(
                cycle_id=uuid5(NAMESPACE_URL, f"postcodes/{start}/{digest}"),
                provider=Provider.POSTCODES,
                scope="brighton-hove-e06000043",
                query_version="postcodes-v1",
                query_sha256=digest,
                window_start=start,
                window_end=end,
            )
            try:
                with tempfile.TemporaryFile() as stream:
                    self.downloader.download(
                        "https://api.postcodes.io/postcodes", stream, method="POST", content=body
                    )
                    artifact = self.store.put_immutable(
                        artifact_key(Layer.BRONZE, cycle, uuid4(), "response.json.gz"),
                        gzip_chunks(chunks(stream)),
                        media_type="application/json",
                        compression="gzip",
                    )
                    stream.seek(0)
                    document = json.load(stream)
                if (
                    not isinstance(document, dict)
                    or document.get("status") != 200
                    or not isinstance(document.get("result"), list)
                ):
                    raise ValueError("Invalid postcode envelope")
                parsed: dict[str, Any] = {}
                for item in document["result"]:
                    if not isinstance(item, dict) or not isinstance(item.get("query"), str):
                        raise ValueError("Invalid response item")
                    key = normalize(item["query"])
                    if key not in batch or key in parsed:
                        raise ValueError("Unexpected/duplicate response")
                    validate_result(item["result"], key)
                    parsed[key] = item["result"]
                if set(parsed) != set(batch):
                    raise ValueError("Incomplete postcode batch")
                # Commit only a reconciled response; transport errors are never negative results.
                for key, result in parsed.items():
                    entry = CacheEntry(
                        postcode=key,
                        result=result,
                        fetched_at=now,
                        expires_at=now + timedelta(days=30 if result is not None else 1),
                        artifact=artifact,
                    )
                    self.save(entry)
                    entries[key] = entry
            except IngestionError as error:
                state: State = (
                    "throttled" if error.failure.category.value == "rate-limit" else "unavailable"
                )
                failures.update({key: state for key in batch})
            except (ValueError, TypeError, KeyError):
                failures.update({key: "unavailable" for key in batch})
        output = []
        for raw in values:
            key = normalize(raw)
            common: dict[str, Any] = {"raw_input": raw, "postcode": key, "location_role": role}
            if not POSTCODE.fullmatch(key):
                output.append(Lookup(**common, state="invalid"))
                continue
            if key in failures:
                output.append(Lookup(**common, state=failures[key]))
                continue
            saved = entries[key]
            assert saved is not None
            result = saved.result
            codes = {} if result is None else result["codes"]
            authority, lsoa = codes.get("admin_district"), codes.get("lsoa21")
            state = (
                "unmatched"
                if result is None
                else "outside"
                if authority and authority != "E06000043"
                else "resolved"
                if authority == "E06000043" and lsoa in self.area_codes
                else "incompatible-geography"
            )
            output.append(
                Lookup(
                    **common,
                    state=state,
                    cache_hit=key in hits,
                    fetched_at=saved.fetched_at,
                    expires_at=saved.expires_at,
                    artifact=saved.artifact,
                    can_define_search=state == "resolved" and role == "search-centre",
                    provider_result=result,
                    authority=authority,
                    lsoa21=lsoa,
                    longitude=None if result is None else result["longitude"],
                    latitude=None if result is None else result["latitude"],
                    quality=None if result is None else result["quality"],
                    source_edition=None if result is None else result.get("dataset_version"),
                )
            )
        return output

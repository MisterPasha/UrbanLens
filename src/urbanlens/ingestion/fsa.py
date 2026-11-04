"""Full-authority FSA XML ingestion; no venue eligibility or identity merging."""

from __future__ import annotations

import hashlib
import json
import math
import re
import tempfile
import unicodedata
import xml.etree.ElementTree as ET
from collections.abc import Iterator, Mapping
from datetime import UTC, date, datetime, timedelta
from typing import IO
from urllib.parse import urlsplit
from uuid import NAMESPACE_URL, uuid5

from pydantic import Field

from urbanlens.ingestion.cadence import month_window
from urbanlens.ingestion.contracts import (
    Artifact,
    CollectionCycle,
    Contract,
    Counts,
    Failure,
    FailureClass,
    Partition,
    Provider,
    Run,
)
from urbanlens.ingestion.errors import IngestionError
from urbanlens.ingestion.http import HttpDownloader, HttpPolicy
from urbanlens.ingestion.interfaces import ArtifactStore, Layer, artifact_key
from urbanlens.ingestion.payload import payload
from urbanlens.ingestion.storage import chunks, gzip_chunks
from urbanlens.ingestion.validation import Record, ValidationRules

SCHEMA = "fsa-authority-xml-v1"
PIPELINE = "fsa-source-v1"


class FsaConfig(Contract):
    version: str = "fsa-authority-monthly-v2"
    scope: str = "brighton-hove-e06000043"
    authority_id: int = 286
    authority_code: str = "875"
    authority_name: str = "Brighton and Hove"
    ons_code: str = "E06000043"
    min_records: int = Field(default=1000, ge=0)
    max_records: int = Field(default=20000, ge=1)
    max_drop_fraction: float = Field(default=0.2, ge=0, le=1)

    def cycle(self, day: date) -> CollectionCycle:
        digest = hashlib.sha256(self.model_dump_json().encode()).hexdigest()
        start, end = month_window(day)
        return CollectionCycle(
            cycle_id=uuid5(NAMESPACE_URL, f"fsa/{self.scope}/{start.date()}/{digest}"),
            provider=Provider.FSA,
            scope=self.scope,
            query_version=self.version,
            query_sha256=digest,
            window_start=start,
            window_end=end,
        )

    def rules(self) -> ValidationRules:
        return ValidationRules(
            version=self.version,
            identity_field="id",
            required_fields=(
                "fhrs_id",
                "business_type_id",
                "rating_value_raw",
                "source_extract_date",
            ),
            schema=lambda row: row.get("schema_valid") is True,
            scope=lambda row: row.get("local_authority_code") == self.authority_code,
            rejection_reasons=record_reasons,
            min_records=self.min_records,
            max_records=self.max_records,
            max_drop_fraction=self.max_drop_fraction,
        )


def record_reasons(row: Record) -> tuple[str, ...]:
    value = row.get("rejection_reasons", ())
    return tuple(str(item) for item in value) if isinstance(value, (tuple, list)) else ()


def header(stream: IO[bytes]) -> tuple[int, date]:
    stream.seek(0)
    for event, element in ET.iterparse(stream, events=("start", "end")):
        if event == "start" and element.tag != "FHRSEstablishment":
            raise IngestionError(FailureClass.SCHEMA, "fsa-unexpected-root")
        break
    else:
        raise IngestionError(FailureClass.SCHEMA, "fsa-empty-document")
    stream.seek(0)
    for _, element in ET.iterparse(stream, events=("end",)):
        if element.tag == "Header":
            try:
                if any(
                    len(element.findall(key)) != 1
                    for key in ("ReturnCode", "ItemCount", "ExtractDate")
                ):
                    raise ValueError()
                if element.findtext("ReturnCode") != "Success":
                    raise ValueError()
                count = int(element.findtext("ItemCount") or "")
                extracted = date.fromisoformat(element.findtext("ExtractDate") or "")
                if count < 0 or extracted > datetime.now(UTC).date() + timedelta(days=1):
                    raise ValueError()
                return count, extracted
            except ValueError:
                raise IngestionError(
                    FailureClass.SCHEMA, "fsa-invalid-publication-header"
                ) from None
    raise IngestionError(FailureClass.SCHEMA, "fsa-missing-publication-header")


class FsaAdapter:
    provider = Provider.FSA

    def __init__(self, config: FsaConfig, downloader: HttpDownloader | None = None) -> None:
        self.config = config
        self.downloader = downloader or HttpDownloader(
            Provider.FSA,
            HttpPolicy(max_requests=6, max_bytes=20 * 1024 * 1024, max_redirects=2),
            headers={
                "x-api-version": "2",
                "Accept": "application/json",
                "Accept-Language": "en-GB",
            },
        )

    def authority(self, value: object) -> str:
        if not isinstance(value, dict) or (
            value.get("LocalAuthorityId") != self.config.authority_id
            or value.get("LocalAuthorityIdCode") != self.config.authority_code
            or value.get("Name") != self.config.authority_name
        ):
            raise IngestionError(FailureClass.SCOPE, "fsa-authority-mapping-mismatch")
        url = value.get("FileName")
        if not isinstance(url, str):
            raise IngestionError(FailureClass.SCHEMA, "fsa-missing-extract-url")
        parsed = urlsplit(url)
        if (
            parsed.scheme != "https"
            or not parsed.hostname
            or parsed.username
            or parsed.password
            or parsed.query
            or parsed.fragment
        ):
            raise IngestionError(FailureClass.SCOPE, "fsa-unsafe-extract-url")
        return url

    def fetch(
        self, cycle: CollectionCycle, run: Run, partition_id: str, store: ArtifactStore
    ) -> Partition:
        if cycle != self.config.cycle(cycle.window_start.date()) or partition_id != "full":
            raise IngestionError(FailureClass.SCOPE, "fsa-query-definition-mismatch")
        artifacts: list[Artifact] = []

        def download(url: str, name: str, media: str) -> Artifact:
            with tempfile.TemporaryFile() as target:
                self.downloader.download(url, target)
                result = store.put_immutable(
                    artifact_key(Layer.BRONZE, cycle, run.run_id, name, partition_id),
                    gzip_chunks(chunks(target)),
                    media_type=media,
                    compression="gzip",
                )
                return result

        authority_artifact = download(
            f"https://api.ratings.food.gov.uk/Authorities/{self.config.authority_id}",
            "authority.json.gz",
            "application/json",
        )
        artifacts.append(authority_artifact)
        count, extracted = None, None
        failure: Failure | None = None
        try:
            with payload(store, authority_artifact) as stream:
                url = self.authority(json.load(stream))
            xml = download(url, "establishments.xml.gz", "application/xml")
            artifacts.insert(0, xml)
            try:
                with payload(store, xml) as stream:
                    count, extracted = header(stream)
            except (IngestionError, ET.ParseError):
                pass  # Retain fetched bytes; full parsing will fail with their artifact reference.
        except IngestionError as error:
            failure = error.failure
        except ValueError:
            failure = IngestionError(
                FailureClass.SCHEMA, "fsa-malformed-authority-metadata"
            ).failure
        return Partition(
            partition_id=partition_id,
            cycle_id=cycle.cycle_id,
            run_id=run.run_id,
            schema_version=SCHEMA,
            pipeline_version=run.pipeline_version,
            source_edition=cycle.source_edition,
            source_extract_date=extracted,
            status="failed" if failure else "fetched",
            failure=failure,
            fetched_at=datetime.now(UTC),
            artifacts=tuple(artifacts),
            counts=Counts(raw=count),
        )

    def parse(self, partition: Partition, store: ArtifactStore) -> Iterator[Mapping[str, object]]:
        if partition.schema_version != SCHEMA or len(partition.artifacts) != 2:
            raise IngestionError(FailureClass.SCHEMA, "fsa-missing-raw-inputs")
        with payload(store, partition.artifacts[1]) as authority:
            self.authority(json.load(authority))
        with payload(store, partition.artifacts[0]) as stream:
            try:
                expected, extracted = header(stream)
                if (
                    partition.source_extract_date is not None
                    and extracted != partition.source_extract_date
                ):
                    raise IngestionError(FailureClass.INTEGRITY, "fsa-extract-date-mismatch")
                stream.seek(0)
                stack: list[str] = []
                count, headers, collections = 0, 0, 0
                for event, element in ET.iterparse(stream, events=("start", "end")):
                    if event == "start":
                        stack.append(element.tag)
                        if len(stack) == 2 and element.tag not in {
                            "Header",
                            "EstablishmentCollection",
                        }:
                            raise IngestionError(
                                FailureClass.SCHEMA, "fsa-unexpected-document-section"
                            )
                        if (
                            len(stack) == 3
                            and stack[1] == "EstablishmentCollection"
                            and element.tag != "EstablishmentDetail"
                        ):
                            raise IngestionError(
                                FailureClass.SCHEMA, "fsa-unexpected-collection-member"
                            )
                        if stack == ["FHRSEstablishment", "Header"]:
                            headers += 1
                        if stack == ["FHRSEstablishment", "EstablishmentCollection"]:
                            collections += 1
                    else:
                        if element.tag == "EstablishmentDetail":
                            if stack != [
                                "FHRSEstablishment",
                                "EstablishmentCollection",
                                "EstablishmentDetail",
                            ]:
                                raise IngestionError(FailureClass.SCHEMA, "fsa-invalid-record-path")
                            count += 1
                            yield normalize(element, extracted, count, self.config.authority_code)
                            element.clear()
                        stack.pop()
                if count != expected or headers != 1 or collections != 1:
                    raise IngestionError(
                        FailureClass.INCOMPLETE, "fsa-declared-count-or-structure-mismatch"
                    )
            except ET.ParseError:
                raise IngestionError(FailureClass.SCHEMA, "fsa-malformed-xml") from None


def normalize(
    row: ET.Element, extracted: date, ordinal: int, authority_code: str = "875"
) -> dict[str, object]:
    def text(key: str) -> str:
        return (row.findtext(key) or "").strip()

    reasons: list[str] = []
    for key in (
        "FHRSID",
        "BusinessName",
        "BusinessTypeID",
        "RatingValue",
        "SchemeType",
        "LocalAuthorityCode",
    ):
        if len(row.findall(key)) != 1:
            reasons.append("missing-or-duplicate-" + key.lower())
    fhrs_id, type_id = text("FHRSID"), text("BusinessTypeID")
    if not re.fullmatch(r"[0-9]+", fhrs_id) or not fhrs_id.strip("0"):
        reasons.append("invalid-fhrs-id")
    if not type_id.isdecimal():
        reasons.append("missing-business-type-id")
    if not text("BusinessName"):
        reasons.append("missing-business-name")
    if not text("RatingValue") or text("SchemeType") != "FHRS":
        reasons.append("invalid-rating-structure")
    if text("LocalAuthorityCode") != authority_code:
        reasons.append("wrong-authority")
    address = {f"line{i}": text(f"AddressLine{i}") or None for i in range(1, 5)}
    address["postcode"] = text("PostCode") or None
    joined = " ".join(v or "" for v in address.values()).casefold()
    held = any(
        marker in joined
        for marker in (
            "private address",
            "address not disclosed",
            "address withheld",
            "address suppressed",
        )
    )
    held |= any(
        text(key).casefold() == "true"
        for key in ("IsAddressPrivate", "AddressIsPrivate", "AddressWithheld", "PrivacySuppressed")
    )
    held |= not any(address.values())
    lon_raw, lat_raw = text("Geocode/Longitude"), text("Geocode/Latitude")
    lon, lat = None, None
    location_state = "withheld-or-unavailable" if held else "missing"
    if not held and (lon_raw or lat_raw):
        try:
            lon, lat = float(lon_raw), float(lat_raw)
            if not (
                math.isfinite(lon)
                and math.isfinite(lat)
                and -180 <= lon <= 180
                and -90 <= lat <= 90
            ) or (lon == 0 and lat == 0):
                raise ValueError()
            location_state = "source-point"
        except ValueError:
            lon, lat, location_state = None, None, "invalid"
    value = text("RatingValue")
    rating_score = int(value) if value in {"0", "1", "2", "3", "4", "5"} else None
    rating_state = (
        "rated"
        if rating_score is not None
        else {
            "AwaitingInspection": "awaiting-inspection",
            "AwaitingPublication": "awaiting-publication",
            "Exempt": "exempt",
        }.get(value, "unknown")
    )
    rating_date, date_state = None, "missing"
    if text("RatingDate"):
        try:
            rating_date = date.fromisoformat(text("RatingDate")).isoformat()
            date_state = "published"
        except ValueError:
            date_state = "invalid"
    return {
        "id": "fsa/" + fhrs_id if fhrs_id else "",
        "fhrs_id": fhrs_id,
        "fhrs_id_raw": row.findtext("FHRSID"),
        "local_authority_business_id": text("LocalAuthorityBusinessID") or None,
        "business_name_raw": row.findtext("BusinessName"),
        "business_name": " ".join(unicodedata.normalize("NFKC", text("BusinessName")).split()),
        "business_type_id": type_id,
        "business_type_raw": text("BusinessType"),
        "published_address": None if held else address,
        "address_state": "withheld-or-unavailable" if held else "published",
        "longitude": lon,
        "latitude": lat,
        "location_state": location_state,
        "source_coordinate_raw": None
        if held
        else {"longitude": lon_raw or None, "latitude": lat_raw or None},
        "rating_value_raw": value,
        "rating_key_raw": text("RatingKey"),
        "rating_score": rating_score,
        "rating_state": rating_state,
        "rating_date": rating_date,
        "rating_date_raw": text("RatingDate") or None,
        "rating_date_state": date_state,
        "scheme_type": text("SchemeType"),
        "new_rating_pending_raw": text("NewRatingPending"),
        "new_rating_pending": {"true": True, "false": False}.get(
            text("NewRatingPending").casefold()
        ),
        "scores_raw": {c.tag: c.text for c in row.findall("Scores/*")},
        "local_authority_code": text("LocalAuthorityCode"),
        "local_authority_name_raw": text("LocalAuthorityName"),
        "source_extract_date": extracted.isoformat(),
        "schema_valid": not reasons,
        "rejection_reasons": reasons,
        "source_record_locator": f"EstablishmentCollection/EstablishmentDetail[{ordinal}]",
        "source_record_sha256": hashlib.sha256(ET.tostring(row, encoding="utf-8")).hexdigest(),
    }

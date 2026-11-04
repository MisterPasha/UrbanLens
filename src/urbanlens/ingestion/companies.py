"""Complete monthly company ZIP partitions and bounded tracked profiles."""

from __future__ import annotations

import base64
import csv
import hashlib
import io
import json
import re
import sqlite3
import tempfile
import zipfile
from collections import Counter
from collections.abc import Callable, Iterator, Mapping
from contextlib import closing
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any
from uuid import NAMESPACE_URL, uuid5

from pydantic import Field

from urbanlens.ingestion.cadence import month_window
from urbanlens.ingestion.contracts import (
    CollectionCycle,
    Contract,
    Counts,
    FailureClass,
    Partition,
    Provider,
    Run,
)
from urbanlens.ingestion.errors import IngestionError
from urbanlens.ingestion.http import HttpDownloader, HttpPolicy
from urbanlens.ingestion.interfaces import ArtifactStore, Layer, artifact_key
from urbanlens.ingestion.payload import payload
from urbanlens.ingestion.postcodes import PostcodeService
from urbanlens.ingestion.postcodes import normalize as postcode
from urbanlens.ingestion.storage import chunks, gzip_chunks
from urbanlens.ingestion.validation import ValidationRules

SICS = frozenset({"56101", "56102", "56103", "56302", "10710", "47240"})
NUMBER = re.compile(r"[A-Z0-9]{8}")
BULK_SCHEMA, PROFILE_SCHEMA = "ch-bulk-v1", "ch-profile-v1"
PIPELINE = "company-source-v1"
FIELDS = (
    "CompanyNumber",
    "CompanyName",
    "CompanyStatus",
    "CompanyCategory",
    "RegAddress.PostCode",
    "IncorporationDate",
    "DissolutionDate",
    *(f"SICCode.SicText_{i}" for i in range(1, 5)),
)


def require(value: bool, code: str) -> None:
    if not value:
        raise IngestionError(FailureClass.SCHEMA, code)


def listing_parts(html: str) -> tuple[str, dict[str, str]]:
    matches = re.findall(r'href="(BasicCompanyData-([0-9-]+)-part(\d+)_(\d+)\.zip)"', html)
    require(bool(matches), "ch-missing-part-listing")
    require(
        len({m[1] for m in matches}) == len({m[3] for m in matches}) == 1, "ch-mixed-part-editions"
    )
    total = int(matches[0][3])
    require(
        0 < total <= 30 and sorted(int(m[2]) for m in matches) == list(range(1, total + 1)),
        "ch-incomplete-part-listing",
    )
    date.fromisoformat(matches[0][1])
    return matches[0][1], {f"part-{int(index):02}": name for name, _, index, _ in matches}


class Operator(Contract):
    company_number: str = Field(pattern=r"^[A-Z0-9]{8}$")
    reviewer: str = Field(min_length=1)
    reviewed_at: datetime
    evidence_uri: str = Field(min_length=1)
    venue_id: str = Field(min_length=1)
    status: str = "verified"


class CompanyConfig(Contract):
    version: str = "companies-monthly-v1"
    scope: str = "brighton-hove-registered-offices"
    min_national_rows: int = 1_000_000
    max_national_rows: int = 10_000_000
    max_profiles: int = Field(default=50, ge=1, le=100)
    tracked_numbers: tuple[str, ...] = ()
    verified_operators: tuple[Operator, ...] = ()

    def cycle(
        self,
        day: date,
        edition: str,
        listing_sha256: str,
        parts: tuple[str, ...],
        *,
        profiles: bool = False,
    ) -> CollectionCycle:
        start, end = month_window(day)
        digest = hashlib.sha256(
            (self.model_dump_json() + edition + listing_sha256 + str(profiles)).encode()
        ).hexdigest()
        scope = self.scope + ("-profiles" if profiles else "")
        return CollectionCycle(
            cycle_id=uuid5(NAMESPACE_URL, f"ch/{scope}/{start}/{digest}"),
            provider=Provider.COMPANIES_HOUSE,
            scope=scope,
            query_version=self.version,
            query_sha256=digest,
            window_start=start,
            window_end=end,
            expected_partitions=tuple(p.lower() for p in parts),
            source_edition=edition,
        )

    def rules(self, *, profiles: bool = False) -> ValidationRules:
        return ValidationRules(
            version=self.version,
            identity_field="company_number",
            required_fields=("company_name", "status", "company_type", "sic_codes"),
            eligible=lambda row: profiles or row.get("cohort_included") is True,
            min_records=0 if profiles else self.min_national_rows,
            max_records=self.max_profiles if profiles else self.max_national_rows,
            max_drop_fraction=1,
            multiplicity_field="source_record_count",
            rejection_reasons=lambda row: (str(row.get("cohort_reason", "excluded")),),
        )


def csv_rows(stream: Any) -> Iterator[dict[str, str]]:
    try:
        with zipfile.ZipFile(stream) as archive:
            members = archive.infolist()
            require(
                len(members) == 1
                and members[0].filename.lower().endswith(".csv")
                and members[0].file_size <= 2_500_000_000,
                "ch-invalid-zip-members",
            )
            with (
                archive.open(members[0]) as raw,
                io.TextIOWrapper(raw, encoding="utf-8-sig", newline="") as text,
            ):
                reader = csv.DictReader(text, strict=True)
                original = reader.fieldnames
                require(original is not None, "ch-missing-csv-header")
                assert original is not None
                fields = [v.strip() for v in original]
                require(
                    len(set(fields)) == len(fields) and set(FIELDS) <= set(fields),
                    "ch-invalid-csv-header",
                )
                reader.fieldnames = fields
                for row in reader:
                    require(
                        None not in row and all(v is not None for v in row.values()),
                        "ch-malformed-csv-row",
                    )
                    require(
                        bool(NUMBER.fullmatch(row["CompanyNumber"].strip()))
                        and bool(row["CompanyName"].strip()),
                        "ch-invalid-company-identity",
                    )
                    yield row

    except (zipfile.BadZipFile, UnicodeError, csv.Error):
        raise IngestionError(FailureClass.SCHEMA, "ch-malformed-zip-or-csv") from None


def sic_codes(row: dict[str, str]) -> list[str]:
    return sorted(
        {
            m[1]
            for i in range(1, 5)
            if (m := re.match(r"^(\d{5})(?:\s|$)", row[f"SICCode.SicText_{i}"].strip()))
        }
    )


def bulk_date(value: str) -> str | None:
    if not value.strip():
        return None
    try:
        return datetime.strptime(value.strip(), "%d/%m/%Y").date().isoformat()
    except ValueError:
        return None  # Original source value remains available, never fabricate a date.


class ProfileRequestBudget:
    """Count every attempted request/retry across workers in one UTC calendar month."""

    def __init__(self, path: Path, limit: int, *, initial_used: int = 0) -> None:
        self.path, self.limit = path, limit
        self.month = datetime.now(UTC).strftime("%Y-%m")
        path.parent.mkdir(parents=True, exist_ok=True)
        with closing(sqlite3.connect(path)) as connection:
            connection.execute(
                "CREATE TABLE IF NOT EXISTS profile_requests "
                "(month TEXT PRIMARY KEY, used INTEGER NOT NULL)"
            )
            connection.execute(
                "INSERT OR IGNORE INTO profile_requests VALUES (?,?)", (self.month, initial_used)
            )
            connection.commit()

    def claim(self) -> None:
        with closing(sqlite3.connect(self.path, timeout=10)) as connection:
            connection.execute("BEGIN IMMEDIATE")
            used = connection.execute(
                "SELECT used FROM profile_requests WHERE month=?", (self.month,)
            ).fetchone()[0]
            if used >= self.limit:
                connection.rollback()
                raise IngestionError(FailureClass.BUDGET, "ch-monthly-profile-request-budget")
            connection.execute(
                "UPDATE profile_requests SET used=used+1 WHERE month=?", (self.month,)
            )
            connection.commit()


class CompanyAdapter:
    provider = Provider.COMPANIES_HOUSE

    def __init__(
        self,
        config: CompanyConfig,
        listing: bytes,
        postcodes: PostcodeService,
        outcodes: set[str],
        *,
        bootstrap: dict[str, tuple[Path, str, datetime]] | None = None,
        listing_fetched_at: datetime | None = None,
    ) -> None:
        self.config, self.listing, self.postcodes, self.outcodes = (
            config,
            listing,
            postcodes,
            frozenset(outcodes),
        )
        self.edition, self.parts = listing_parts(listing.decode())
        self.bootstrap, self.listing_fetched_at = (
            bootstrap or {},
            listing_fetched_at or datetime.now(UTC),
        )
        self.downloader = HttpDownloader(
            Provider.COMPANIES_HOUSE,
            HttpPolicy(
                max_requests=20,
                max_retries=1,
                max_bytes=1_000_000_000,
                max_seconds=1800,
                timeout_seconds=90,
            ),
        )
        self.operators = {
            o.company_number for o in config.verified_operators if o.status == "verified"
        }
        self.headers: list[str] | None = None

    def fetch(
        self, cycle: CollectionCycle, run: Run, partition_id: str, store: ArtifactStore
    ) -> Partition:
        require(
            cycle.source_edition == self.edition
            and cycle.expected_partitions == tuple(sorted(self.parts))
            and partition_id in self.parts,
            "ch-pinned-part-set-mismatch",
        )
        name = self.parts[partition_id]
        require(not self.bootstrap or name in self.bootstrap, "ch-bootstrap-missing-part")
        listing = store.put_immutable(
            artifact_key(Layer.BRONZE, cycle, run.run_id, "listing.html", partition_id),
            iter([self.listing]),
            media_type="text/html",
            compression="none",
        )
        with tempfile.TemporaryFile() as stream:
            fetched_at = datetime.now(UTC)
            if name in self.bootstrap:
                path, expected, fetched_at = self.bootstrap[name]
                digest = hashlib.sha256()
                with path.open("rb") as old:
                    for block in chunks(old):
                        digest.update(block)
                        stream.write(block)
                require(digest.hexdigest() == expected, "ch-bootstrap-checksum")
                stream.seek(0)
            else:
                self.downloader.download("https://download.companieshouse.gov.uk/" + name, stream)
            raw = store.put_immutable(
                artifact_key(Layer.BRONZE, cycle, run.run_id, name.lower(), partition_id),
                chunks(stream),
                media_type="application/zip",
                compression="none",
            )
            stream.seek(0)
            count = 0
            candidates: set[str] = set()
            headers: list[str] | None = None
            for row in csv_rows(stream):
                count += 1
                headers = list(row)
                number, pc = row["CompanyNumber"].strip(), postcode(row["RegAddress.PostCode"])
                if number in self.operators or (
                    set(sic_codes(row)) & SICS and pc and pc[:-3] in self.outcodes
                ):
                    candidates.add(row["RegAddress.PostCode"])
        resolved = self.postcodes.bulk(sorted(candidates))
        if any(r.state in {"throttled", "unavailable"} for r in resolved):
            raise IngestionError(
                FailureClass.NETWORK, "ch-postcode-enrichment-unavailable", retryable=True
            )
        context = {
            "edition": self.edition,
            "filename": name,
            "rows": count,
            "headers": headers,
            "listing_fetched_at": self.listing_fetched_at.isoformat(),
            "bootstrap": bool(self.bootstrap),
            "outcodes": sorted(self.outcodes),
            "verified_operators": sorted(self.operators),
            "postcodes": {r.postcode: r.model_dump(mode="json") for r in resolved},
        }
        descriptor = store.put_immutable(
            artifact_key(Layer.BRONZE, cycle, run.run_id, "context.json.gz", partition_id),
            gzip_chunks(iter([json.dumps(context, sort_keys=True).encode()])),
            media_type="application/json",
            compression="gzip",
        )
        return Partition(
            partition_id=partition_id,
            cycle_id=cycle.cycle_id,
            run_id=run.run_id,
            source_edition=self.edition,
            schema_version=BULK_SCHEMA,
            pipeline_version=run.pipeline_version,
            status="fetched",
            fetched_at=fetched_at,
            source_extract_date=date.fromisoformat(self.edition),
            artifacts=(raw, listing, descriptor),
            counts=Counts(raw=count),
        )

    def parse(self, partition: Partition, store: ArtifactStore) -> Iterator[Mapping[str, object]]:
        require(
            partition.schema_version == BULK_SCHEMA and len(partition.artifacts) == 3,
            "ch-missing-bulk-inputs",
        )
        with payload(store, partition.artifacts[1]) as stream:
            edition, parts = listing_parts(stream.read().decode())
        with payload(store, partition.artifacts[2]) as stream:
            context = json.load(stream)
        require(
            edition == partition.source_edition == context["edition"]
            and parts[partition.partition_id] == context["filename"],
            "ch-edition-or-part-mismatch",
        )
        stats: Counter[str] = Counter()
        excluded: Counter[str] = Counter()
        with payload(store, partition.artifacts[0], max_bytes=128 * 1024 * 1024) as stream:
            for row in csv_rows(stream):
                stats["scanned"] += 1
                if stats["scanned"] == 1:
                    require(list(row) == context["headers"], "ch-part-header-changed")
                    if self.headers is None:
                        self.headers = list(row)
                    require(list(row) == self.headers, "ch-inconsistent-part-headers")
                number, pc, codes = (
                    row["CompanyNumber"].strip(),
                    postcode(row["RegAddress.PostCode"]),
                    sic_codes(row),
                )
                lookup = context["postcodes"].get(pc)
                operator = number in context["verified_operators"]
                relevant = bool(set(codes) & SICS)
                reason = (
                    "verified-operator"
                    if operator
                    else "not-hospitality-sic"
                    if not relevant
                    else "missing-postcode"
                    if not pc
                    else "outside-postal-prefilter"
                    if pc[:-3] not in context["outcodes"]
                    else "postcode-" + (lookup["state"] if lookup else "unclassified")
                )
                included = operator or (
                    relevant and lookup is not None and lookup["state"] == "resolved"
                )
                if included and not operator:
                    reason = "registered-office-cohort"
                stats[reason] += 1
                if not included:
                    excluded[reason] += 1
                    continue
                yield {
                    "company_number": number,
                    "company_name": row["CompanyName"].strip(),
                    "status": row["CompanyStatus"].strip(),
                    "company_type": row["CompanyCategory"].strip(),
                    "sic_codes": codes,
                    "postcode_raw": row["RegAddress.PostCode"],
                    "postcode": pc,
                    "creation_date": bulk_date(row["IncorporationDate"]),
                    "creation_date_raw": row["IncorporationDate"],
                    "cessation_date": bulk_date(row["DissolutionDate"]),
                    "cessation_date_raw": row["DissolutionDate"],
                    "registered_office": {
                        k: v for k, v in row.items() if k.startswith("RegAddress.")
                    },
                    "cohort_included": included,
                    "cohort_reason": reason,
                    "verified_operator": operator,
                    "postcode_evidence": lookup if included else None,
                    "source_edition": edition,
                    "source_part": partition.partition_id,
                    "source_record": stats["scanned"],
                    "observed_at": partition.fetched_at.isoformat()
                    if partition.fetched_at
                    else None,
                    "absence_means_dissolved": False,
                    "source_record_count": 1,
                }
        require(
            stats["scanned"] == context["rows"] == partition.counts.raw, "ch-raw-row-count-mismatch"
        )
        # Every excluded row was scanned and validated above. Exact counts replace
        # millions of irrelevant duplicate-index inserts; retained ZIPs preserve all IDs.
        for reason, count in sorted(excluded.items()):
            yield {
                "company_number": f"excluded-{partition.partition_id}-{reason}",
                "company_name": "Excluded national-source rows",
                "status": "not-published",
                "company_type": "not-published",
                "sic_codes": [],
                "record_kind": "excluded-summary",
                "cohort_included": False,
                "cohort_reason": reason,
                "source_record_count": count,
            }


class ProfileAdapter:
    provider = Provider.COMPANIES_HOUSE

    def __init__(
        self,
        config: CompanyConfig,
        key: str | None = None,
        *,
        downloader: HttpDownloader | None = None,
        request_budget: Callable[[], None] | None = None,
    ) -> None:
        self.config = config
        self.numbers = tuple(
            sorted(
                set(config.tracked_numbers)
                | {o.company_number for o in config.verified_operators if o.status == "verified"}
            )
        )
        require(
            0 < len(self.numbers) <= config.max_profiles
            and all(NUMBER.fullmatch(n) for n in self.numbers),
            "ch-profile-work-budget",
        )
        self.downloader = downloader
        if downloader is None and key:
            auth = base64.b64encode((key + ":").encode()).decode()
            self.downloader = HttpDownloader(
                Provider.COMPANIES_HOUSE,
                HttpPolicy(
                    max_requests=config.max_profiles * 2,
                    max_retries=1,
                    max_bytes=10_000_000,
                    max_seconds=240,
                ),
                headers={"Authorization": "Basic " + auth, "Accept": "application/json"},
                before_request=request_budget,
            )

    def fetch(
        self, cycle: CollectionCycle, run: Run, partition_id: str, store: ArtifactStore
    ) -> Partition:
        require(
            partition_id.upper() in self.numbers and self.downloader is not None,
            "ch-profile-key-or-scope-missing",
        )
        assert self.downloader is not None
        with tempfile.TemporaryFile() as stream:
            self.downloader.download(
                "https://api.company-information.service.gov.uk/company/" + partition_id.upper(),
                stream,
            )
            artifact = store.put_immutable(
                artifact_key(Layer.BRONZE, cycle, run.run_id, "profile.json.gz", partition_id),
                gzip_chunks(chunks(stream)),
                media_type="application/json",
                compression="gzip",
            )
        return Partition(
            partition_id=partition_id,
            cycle_id=cycle.cycle_id,
            run_id=run.run_id,
            source_edition=cycle.source_edition,
            schema_version=PROFILE_SCHEMA,
            pipeline_version=run.pipeline_version,
            status="fetched",
            fetched_at=datetime.now(UTC),
            artifacts=(artifact,),
            counts=Counts(raw=1),
        )

    def parse(self, partition: Partition, store: ArtifactStore) -> Iterator[Mapping[str, object]]:
        require(
            partition.schema_version == PROFILE_SCHEMA and len(partition.artifacts) == 1,
            "ch-missing-profile-input",
        )
        with payload(store, partition.artifacts[0]) as stream:
            row = json.load(stream)
        require(
            row.get("company_number") == partition.partition_id.upper()
            and bool(NUMBER.fullmatch(partition.partition_id.upper()))
            and not row.get("errors"),
            "ch-profile-identity-mismatch",
        )
        require(
            all(
                isinstance(row.get(k), str) and row[k]
                for k in ("company_name", "company_status", "type")
            ),
            "ch-invalid-profile-schema",
        )
        codes = row.get("sic_codes", [])
        require(
            isinstance(codes, list)
            and all(isinstance(v, str) and re.fullmatch(r"\d{5}", v) for v in codes),
            "ch-invalid-profile-sics",
        )
        for key in ("date_of_creation", "date_of_cessation"):
            if row.get(key):
                date.fromisoformat(row[key])
        yield {
            "company_number": partition.partition_id.upper(),
            "company_name": row["company_name"],
            "status": row["company_status"],
            "company_type": row["type"],
            "creation_date": row.get("date_of_creation"),
            "cessation_date": row.get("date_of_cessation"),
            "sic_codes": codes,
            "registered_office": row.get("registered_office_address"),
            "observed_at": partition.fetched_at.isoformat() if partition.fetched_at else None,
            "source_profile_uri": "https://find-and-update.company-information.service.gov.uk/company/"
            + partition.partition_id.upper(),
            "legal_context_only": True,
        }

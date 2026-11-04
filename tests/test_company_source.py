"""Complete company part sets, source IDs, legal context and independent operator inclusion."""

import csv
import hashlib
import io
import json
import zipfile
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any, Self

import httpx
import pytest

from urbanlens.ingestion.companies import (
    FIELDS,
    PIPELINE,
    CompanyAdapter,
    CompanyConfig,
    Operator,
    ProfileAdapter,
    ProfileRequestBudget,
    listing_parts,
)
from urbanlens.ingestion.contracts import Provider, Stage
from urbanlens.ingestion.engine import Collector
from urbanlens.ingestion.errors import IngestionError
from urbanlens.ingestion.http import HttpDownloader, HttpPolicy
from urbanlens.ingestion.postcodes import PostcodeService
from urbanlens.ingestion.registry import Registry, SQLiteStateBackend
from urbanlens.ingestion.storage import LocalArtifactStore

NOW = datetime(2026, 11, 2, 12, tzinfo=UTC)


@pytest.fixture(autouse=True)
def fast(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("urbanlens.ingestion.http.RATES", {p: 100_000.0 for p in Provider})


def row(
    number: str, pc: str = "BN11JE", sic: str = "56101", *, position: int = 1
) -> dict[str, str]:
    value = dict.fromkeys(FIELDS, "")
    value.update(
        CompanyNumber=number,
        CompanyName="Invented Company " + number,
        CompanyStatus="Active",
        CompanyCategory="Private Limited Company",
        IncorporationDate="02/01/2020",
    )
    value["RegAddress.PostCode"] = pc
    value[f"SICCode.SicText_{position}"] = sic + " - Invented activity"
    return value


def fixture(
    tmp_path: Path, *, operator: bool = False
) -> tuple[CompanyConfig, CompanyAdapter, LocalArtifactStore, Registry]:
    store = LocalArtifactStore(tmp_path / "objects")
    registry = Registry(SQLiteStateBackend(tmp_path / "registry.sqlite"))

    def request(request: httpx.Request) -> httpx.Response:
        items = []
        for key in json.loads(request.content)["postcodes"]:
            result = (
                None
                if key == "BN99ZZ"
                else {
                    "postcode": key,
                    "quality": 1,
                    "longitude": -0.14,
                    "latitude": 50.83,
                    "codes": {
                        "admin_district": "E06000043" if key.startswith("BN") else "E09000033",
                        "lsoa21": "E01016849",
                    },
                }
            )
            items.append({"query": key, "result": result})
        return httpx.Response(
            200, stream=httpx.ByteStream(json.dumps({"status": 200, "result": items}).encode())
        )

    service = PostcodeService(
        tmp_path / "postcodes.sqlite",
        store,
        {"E01016849"},
        HttpDownloader(
            Provider.POSTCODES, HttpPolicy(max_retries=0), transport=httpx.MockTransport(request)
        ),
    )
    verified = (
        ()
        if not operator
        else (
            Operator(
                company_number="SC000001",
                reviewer="fixture reviewer",
                reviewed_at=NOW,
                evidence_uri="https://example.test/operator",
                venue_id="synthetic-venue",
            ),
        )
    )
    config = CompanyConfig(min_national_rows=0, max_national_rows=100, verified_operators=verified)
    rows = [
        [row("00000001", position=4), row("00000002", sic="99999")],
        [
            row("SC000001", "SW1A1AA", "99999"),
            row("00000004", "BN99ZZ"),
            row("00000005", "SW1A1AA"),
        ],
    ]
    bootstrap = {}
    links = []
    for index, values in enumerate(rows, 1):
        name = f"BasicCompanyData-2026-11-01-part{index}_2.zip"
        links.append(f'<a href="{name}">part</a>')
        buffer = io.StringIO(newline="")
        writer = csv.DictWriter(buffer, fieldnames=list(FIELDS))
        writer.writeheader()
        writer.writerows(values)
        path = tmp_path / name
        with zipfile.ZipFile(path, "w", compression=zipfile.ZIP_DEFLATED) as archive:
            archive.writestr("synthetic.csv", buffer.getvalue())
        bootstrap[name] = (path, hashlib.sha256(path.read_bytes()).hexdigest(), NOW)
    adapter = CompanyAdapter(
        config,
        "\n".join(links).encode(),
        service,
        {"BN1", "BN9"},
        bootstrap=bootstrap,
        listing_fetched_at=NOW,
    )
    return config, adapter, store, registry


def collect(
    config: CompanyConfig,
    adapter: CompanyAdapter,
    store: LocalArtifactStore,
    registry: Registry,
    day: date = date(2026, 11, 4),
) -> Any:
    cycle = config.cycle(
        day,
        adapter.edition,
        hashlib.sha256(adapter.listing).hexdigest(),
        tuple(sorted(adapter.parts)),
    )
    return Collector(store, registry).collect(
        cycle, adapter, config.rules(), pipeline_version=PIPELINE
    )


def test_full_scan_counts_and_fourth_sic_preserve_leading_zero_identity(tmp_path: Path) -> None:
    config, adapter, store, registry = fixture(tmp_path)
    snapshot = collect(config, adapter, store, registry)
    assert snapshot.run.status == Stage.ACCEPTED
    assert sum(p.counts.raw or 0 for p in snapshot.partitions) == 5
    assert sum(p.counts.accepted or 0 for p in snapshot.partitions) == 1
    rows: list[Any] = [r for part in snapshot.partitions for r in adapter.parse(part, store)]
    selected = [r for r in rows if r["cohort_included"]]
    assert selected[0]["company_number"] == "00000001" and selected[0]["sic_codes"] == ["56101"]
    assert all(r["absence_means_dissolved"] is False for r in selected)
    assert sum(r["source_record_count"] for r in rows) == 5
    assert all(
        p.fetched_at == NOW and p.source_extract_date == date(2026, 11, 1)
        for p in snapshot.partitions
    )


def test_verified_operator_is_included_independently_of_office_and_sic(tmp_path: Path) -> None:
    config, adapter, store, registry = fixture(tmp_path, operator=True)
    snapshot = collect(config, adapter, store, registry)
    selected = [
        r for p in snapshot.partitions for r in adapter.parse(p, store) if r["cohort_included"]
    ]
    assert {r["company_number"] for r in selected} == {"00000001", "SC000001"}
    assert [r for r in selected if r["company_number"] == "SC000001"][0][
        "cohort_reason"
    ] == "verified-operator"


@pytest.mark.parametrize(
    "html",
    [
        '<a href="BasicCompanyData-2026-11-01-part1_2.zip">one</a>',
        '<a href="BasicCompanyData-2026-11-01-part1_1.zip">one</a>'
        '<a href="BasicCompanyData-2026-12-01-part1_1.zip">two</a>',
        "upstream error",
    ],
)
def test_missing_or_mixed_listing_is_not_a_complete_edition(html: str) -> None:
    with pytest.raises(IngestionError):
        listing_parts(html)


@pytest.mark.parametrize(
    "problem", ["missing-part", "checksum", "truncated-zip", "bad-number", "different-header"]
)
def test_bad_followup_never_replaces_previous_complete_cohort(tmp_path: Path, problem: str) -> None:
    config, adapter, store, registry = fixture(tmp_path)
    previous = collect(config, adapter, store, registry)
    name = adapter.parts["part-02"]
    path, checksum, fetched = adapter.bootstrap[name]
    if problem == "missing-part":
        adapter.bootstrap.pop(name)
    elif problem == "checksum":
        adapter.bootstrap[name] = (path, "0" * 64, fetched)
    else:
        if problem == "truncated-zip":
            path.write_bytes(b"broken zip")
        else:
            values = row("BAD" if problem == "bad-number" else "00000099")
            if problem == "different-header":
                values["UnexpectedColumn"] = "extra"
            buffer = io.StringIO(newline="")
            writer = csv.DictWriter(buffer, fieldnames=list(values))
            writer.writeheader()
            writer.writerow(values)
            with zipfile.ZipFile(path, "w") as archive:
                archive.writestr("synthetic.csv", buffer.getvalue())
        adapter.bootstrap[name] = (path, hashlib.sha256(path.read_bytes()).hexdigest(), fetched)
    with pytest.raises((IngestionError, zipfile.BadZipFile)):
        collect(config, adapter, store, registry, date(2026, 12, 4))
    assert registry.current(previous.cycle) == previous


def test_profile_status_dates_and_tracked_operator_do_not_require_bulk_membership(
    tmp_path: Path,
) -> None:
    config = CompanyConfig(tracked_numbers=("SC000001",), max_profiles=1)

    def request(_: httpx.Request) -> httpx.Response:
        body = {
            "company_number": "SC000001",
            "company_name": "Invented Legal Company",
            "company_status": "dissolved",
            "type": "ltd",
            "date_of_creation": "2000-01-01",
            "date_of_cessation": "2026-10-01",
            "sic_codes": ["99999"],
            "registered_office_address": {"postal_code": "SW1A 1AA"},
        }
        return httpx.Response(200, stream=httpx.ByteStream(json.dumps(body).encode()))

    adapter = ProfileAdapter(
        config,
        downloader=HttpDownloader(Provider.COMPANIES_HOUSE, transport=httpx.MockTransport(request)),
    )
    store = LocalArtifactStore(tmp_path / "objects")
    registry = Registry(SQLiteStateBackend(tmp_path / "registry.sqlite"))
    cycle = config.cycle(
        date(2026, 11, 4), "tracked-profiles", "a" * 64, adapter.numbers, profiles=True
    )
    snapshot = Collector(store, registry).collect(
        cycle, adapter, config.rules(profiles=True), pipeline_version=PIPELINE
    )
    parsed = list(adapter.parse(snapshot.partitions[0], store))[0]
    assert (
        parsed["company_number"] == "SC000001"
        and parsed["status"] == "dissolved"
        and parsed["cessation_date"] == "2026-10-01"
    )
    assert parsed["legal_context_only"]


def test_profile_call_budget_is_explicit() -> None:
    with pytest.raises(IngestionError, match="ch-profile-work-budget"):
        ProfileAdapter(CompanyConfig(tracked_numbers=("00000001", "SC000001"), max_profiles=1))


def test_monthly_profile_budget_persists_and_counts_retries(tmp_path: Path) -> None:
    path = tmp_path / "budget.sqlite"
    budget = ProfileRequestBudget(path, 1)
    client = HttpDownloader(
        Provider.COMPANIES_HOUSE,
        HttpPolicy(max_requests=2, max_retries=1, backoff_seconds=0),
        transport=httpx.MockTransport(lambda _: httpx.Response(503, stream=httpx.ByteStream(b""))),
        before_request=budget.claim,
    )
    with pytest.raises(IngestionError, match="ch-monthly-profile-request-budget"):
        client.download("https://example.test/profile", io.BytesIO())
    assert client.requests == 1
    with pytest.raises(IngestionError, match="ch-monthly-profile-request-budget"):
        ProfileRequestBudget(path, 1).claim()


def test_recurring_command_reuses_accepted_month_before_network_or_pilot_work(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from scripts import source_pipeline

    config, adapter, store, registry = fixture(tmp_path)
    previous = collect(config, adapter, store, registry)
    directory = tmp_path / "config"
    directory.mkdir()
    (directory / "companies-source.json").write_text(config.model_dump_json())

    class Clock(datetime):
        @classmethod
        def now(cls, tz: Any = None) -> Self:
            return cls(2026, 11, 2, 12, tzinfo=UTC)

    monkeypatch.setattr(source_pipeline, "ROOT", tmp_path)
    monkeypatch.setattr(source_pipeline, "datetime", Clock)
    monkeypatch.setattr(
        source_pipeline,
        "postcode_service",
        lambda *a: pytest.fail("Accepted month needs no postcode job"),
    )
    monkeypatch.setattr(
        HttpDownloader, "download", lambda *a, **k: pytest.fail("Accepted month must not download")
    )
    assert source_pipeline.company_job("company-fetch", store, registry) == previous

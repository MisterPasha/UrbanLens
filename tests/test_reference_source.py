"""Explicit reference workbook selection, retained-product pins and isolated staging."""

import csv
import hashlib
import json
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any
from uuid import uuid4

import pytest
from openpyxl import Workbook

from scripts import geography
from urbanlens.ingestion.contracts import Run
from urbanlens.ingestion.errors import IngestionError
from urbanlens.ingestion.reference import (
    PIPELINE,
    ReferenceAdapter,
    ReferenceConfig,
    check_editions,
)
from urbanlens.ingestion.storage import LocalArtifactStore

CODE = "E01016849"
NOW = datetime(2026, 11, 1, 12, tzinfo=UTC)


def workbook(
    path: Path, *, sheet: str = "Mid-2024 LSOA 2021", duplicate: bool = False, total: int = 10
) -> None:
    book = Workbook()
    page = book.active
    page.title = sheet
    for _ in range(3):
        page.append(["synthetic notes"])
    page.append(geography.POP_FIELDS)
    values = [geography.LAD, "Synthetic Authority", CODE, "Synthetic Area", total, *([1] * 10)]
    page.append(values)
    if duplicate:
        page.append(values)
    book.create_sheet("Notes").append(["Synthetic fixture"])
    book.save(path)
    book.close()


def fixture(tmp_path: Path) -> tuple[ReferenceConfig, ReferenceAdapter, LocalArtifactStore]:
    raw = tmp_path / "original"
    raw.mkdir()
    workbook(raw / "population.xlsx")
    names = ["population.xlsx", "boundary-item.json", "lookup-item.json", "sam-item.json"]
    for name in names[1:]:
        (raw / name).write_text(json.dumps({"id": name, "modified": 1}))
    pins: dict[str, Any] = {
        "authority": geography.LAD,
        "geography": "LSOA 2021",
        "boundary": "synthetic boundary v1",
        "membership": "synthetic membership v1",
        "land_area": "synthetic hectares v1",
        "population": "Mid-2024 fixture",
        "artifacts": {
            name: {
                "sha256": hashlib.sha256((raw / name).read_bytes()).hexdigest(),
                "retrieved_at": NOW.isoformat(),
                "url": "https://example.test/" + name,
            }
            for name in names
        },
    }
    pins["artifacts"]["population.xlsx"]["url"] = geography.POP_URL
    manifest = json.dumps(pins, sort_keys=True).encode()
    config = ReferenceConfig(
        manifest_sha256=hashlib.sha256(manifest).hexdigest(),
        target_codes_sha256=hashlib.sha256(CODE.encode()).hexdigest(),
        expected_areas=1,
    )
    return config, ReferenceAdapter(config, raw, manifest), LocalArtifactStore(tmp_path / "objects")


def test_population_reader_uses_explicit_sheet_and_reconciles_all_bands(tmp_path: Path) -> None:
    workbook(tmp_path / "population.xlsx")
    rows, report = geography.populations({CODE}, raw=tmp_path)
    assert rows[CODE] == {"population": 10, "age_0_15": 2, "age_16_64": 6, "age_65_plus": 2}
    assert report["selected_sheet"] == "Mid-2024 LSOA 2021" and report["header_row"] == 4


@pytest.mark.parametrize("problem", ["wrong-year", "duplicate", "missing-code", "unreconciled"])
def test_reference_workbook_does_not_silently_change_year_or_code_set(
    tmp_path: Path, problem: str
) -> None:
    workbook(
        tmp_path / "population.xlsx",
        sheet="Mid-2025 LSOA 2021" if problem == "wrong-year" else "Mid-2024 LSOA 2021",
        duplicate=problem == "duplicate",
        total=11 if problem == "unreconciled" else 10,
    )
    with pytest.raises((ValueError, KeyError)):
        geography.populations({"E01000000"} if problem == "missing-code" else {CODE}, raw=tmp_path)


def test_staging_reads_retained_inputs_and_never_mutates_original_files(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config, adapter, store = fixture(tmp_path)
    cycle = config.cycle(date(2026, 11, 4))
    run = Run(run_id=uuid4(), cycle_id=cycle.cycle_id, pipeline_version=PIPELINE, started_at=NOW)
    assert adapter.source is not None
    original = (adapter.source / "population.xlsx").read_bytes()
    part = adapter.fetch(cycle, run, "full", store)
    (adapter.source / "population.xlsx").write_bytes(b"changed after retention")

    def validate(*, raw: Path, out: Path, pin: Path) -> None:
        assert raw != adapter.source and (raw / "population.xlsx").read_bytes() == original
        with (out / "target-areas.csv").open("w", newline="") as stream:
            fields = [
                "LSOA21CD",
                "LSOA21NM",
                "LAD22CD",
                "population",
                "age_0_15",
                "age_16_64",
                "age_65_plus",
                "land_hectares",
                "land_km2",
            ]
            writer = csv.DictWriter(stream, fieldnames=fields)
            writer.writeheader()
            writer.writerow(
                dict(
                    zip(
                        fields,
                        [CODE, "Synthetic", geography.LAD, 10, 2, 6, 2, "100", "1"],
                        strict=True,
                    )
                )
            )
        (raw / "areas-wgs84.geojson").write_text(
            json.dumps(
                {
                    "features": [
                        {
                            "properties": {"LSOA21CD": CODE},
                            "geometry": {
                                "type": "Polygon",
                                "coordinates": [
                                    [[0, 50], [0.01, 50], [0.01, 50.01], [0, 50.01], [0, 50]]
                                ],
                            },
                        }
                    ]
                }
            )
        )

    monkeypatch.setattr(geography, "validate", validate)
    rows = list(adapter.parse(part, store))
    assert rows[0]["reference_period"] == "2024-06-30" and rows[0]["population"] == 10
    assert (
        part.fetched_at == NOW
        and (adapter.source / "population.xlsx").read_bytes() == b"changed after retention"
    )


def test_missing_retained_product_fails_before_rebuilding(tmp_path: Path) -> None:
    config, adapter, store = fixture(tmp_path)
    cycle = config.cycle(date(2026, 11, 4))
    run = Run(run_id=uuid4(), cycle_id=cycle.cycle_id, pipeline_version=PIPELINE, started_at=NOW)
    part = adapter.fetch(cycle, run, "full", store)
    changed = part.model_copy(update={"artifacts": part.artifacts[1:]})
    with pytest.raises(IngestionError, match="ons-incomplete-product-set"):
        list(adapter.parse(changed, store))


def test_geographic_vintage_change_requires_migration(tmp_path: Path) -> None:
    config, adapter, store = fixture(tmp_path)
    config = config.model_copy(update={"geography_vintage": "LSOA-2011"})
    adapter.config = config
    cycle = config.cycle(date(2026, 11, 4))
    run = Run(run_id=uuid4(), cycle_id=cycle.cycle_id, pipeline_version=PIPELINE, started_at=NOW)
    part = adapter.fetch(cycle, run, "full", store)
    with pytest.raises(IngestionError, match="ons-vintage-migration-required"):
        list(adapter.parse(part, store))


@pytest.mark.parametrize("new_edition", [False, True])
def test_monthly_edition_check_stages_candidates_without_replacing_pins(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, new_edition: bool
) -> None:
    config, adapter, store = fixture(tmp_path)
    original = adapter.manifest
    assert original is not None

    class Client:
        def download(self, url: str, target: Any) -> None:
            if "/file?" not in url and "ons.gov.uk" in url:
                link = (
                    geography.POP_URL.replace(
                        "mid2022revisednov2025tomid2024", "syntheticnewedition"
                    )
                    if new_edition
                    else geography.POP_URL
                )
                target.write(f'<a href="{link}">xlsx</a>'.encode())
            else:
                target.write(b"changed candidate")
            target.seek(0)

    monkeypatch.setattr("urbanlens.ingestion.reference.HttpDownloader", lambda *a, **k: Client())
    report = check_editions(config, original, store)
    assert report["pinned_baseline_unchanged"] and len(report["products"]) == 4
    assert all(not p["bytes_unchanged"] for p in report["products"])
    assert adapter.manifest == original
    assert report["population_discovery"]["different_from_pinned"] == new_edition
    assert (report["population_discovery"]["candidate_artifact"] is not None) == new_edition

"""Stage/rebuild pinned ONS products without mutating the established geography baseline."""

from __future__ import annotations

import csv
import hashlib
import json
import re
import tempfile
from collections.abc import Iterator, Mapping
from datetime import UTC, date, datetime
from html import unescape
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urljoin, urlsplit
from uuid import NAMESPACE_URL, uuid5

from scripts import geography
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
from urbanlens.ingestion.storage import chunks, gzip_chunks
from urbanlens.ingestion.validation import ValidationRules

SCHEMA, PIPELINE = "ons-reference-v1", "reference-source-v1"


def require(value: bool, code: str) -> None:
    if not value:
        raise IngestionError(FailureClass.SCHEMA, code)


class ReferenceConfig(Contract):
    version: str = "ons-geography2021-pop2024-v1"
    scope: str = "brighton-hove-e06000043"
    manifest_sha256: str
    target_codes_sha256: str
    expected_areas: int = 165
    geography_vintage: str = "LSOA-2021"
    population_sheet: str = "Mid-2024 LSOA 2021"
    population_header_row: int = 4
    population_fields: tuple[str, ...] = tuple(geography.POP_FIELDS)
    population_reference_date: date = date(2024, 6, 30)

    def cycle(self, day: date) -> CollectionCycle:
        start, end = month_window(day)
        digest = hashlib.sha256(self.model_dump_json().encode()).hexdigest()
        return CollectionCycle(
            cycle_id=uuid5(NAMESPACE_URL, f"ons/{self.scope}/{start}/{digest}"),
            provider=Provider.ONS,
            scope=self.scope,
            query_version=self.version,
            query_sha256=digest,
            window_start=start,
            window_end=end,
            source_edition=self.version,
        )

    def rules(self) -> ValidationRules:
        return ValidationRules(
            version=self.version,
            identity_field="lsoa21cd",
            required_fields=(
                "geometry",
                "population",
                "land_km2",
                "reference_period",
                "product_editions",
            ),
            min_records=self.expected_areas,
            max_records=self.expected_areas,
            max_drop_fraction=1,
        )


class ReferenceAdapter:
    provider = Provider.ONS

    def __init__(
        self, config: ReferenceConfig, source: Path | None = None, manifest: bytes | None = None
    ) -> None:
        self.config, self.source, self.manifest = config, source, manifest

    def fetch(
        self, cycle: CollectionCycle, run: Run, partition_id: str, store: ArtifactStore
    ) -> Partition:
        require(
            cycle == self.config.cycle(cycle.window_start.date())
            and partition_id == "full"
            and self.source is not None
            and self.manifest is not None,
            "ons-query-definition-mismatch",
        )
        assert self.source is not None and self.manifest is not None
        require(
            hashlib.sha256(self.manifest).hexdigest() == self.config.manifest_sha256,
            "ons-manifest-pin",
        )
        pins = json.loads(self.manifest)
        artifacts = []
        names = sorted(pins["artifacts"])
        for name in names:
            require(
                Path(name).name == name and not name.startswith("."), "ons-unsafe-artifact-name"
            )
            path = self.source / name
            digest = hashlib.sha256()
            with path.open("rb") as stream:
                for block in chunks(stream):
                    digest.update(block)
            require(digest.hexdigest() == pins["artifacts"][name]["sha256"], "ons-source-checksum")
            with path.open("rb") as stream:
                artifacts.append(
                    store.put_immutable(
                        artifact_key(Layer.BRONZE, cycle, run.run_id, name.lower(), partition_id),
                        chunks(stream),
                        media_type="application/octet-stream",
                        compression="none",
                    )
                )
        descriptor = {
            "config": self.config.model_dump(mode="json"),
            "manifest_text": self.manifest.decode(),
            "names": names,
        }
        artifacts.append(
            store.put_immutable(
                artifact_key(
                    Layer.BRONZE, cycle, run.run_id, "reference-definition.json.gz", partition_id
                ),
                gzip_chunks(iter([json.dumps(descriptor, sort_keys=True).encode()])),
                media_type="application/json",
                compression="gzip",
            )
        )
        # Bootstrap uses the original acquisition times, not the time files were copied.
        fetched_at = max(
            datetime.fromisoformat(v["retrieved_at"]) for v in pins["artifacts"].values()
        )
        return Partition(
            partition_id=partition_id,
            cycle_id=cycle.cycle_id,
            run_id=run.run_id,
            source_edition=cycle.source_edition,
            schema_version=SCHEMA,
            pipeline_version=run.pipeline_version,
            status="fetched",
            fetched_at=fetched_at,
            artifacts=tuple(artifacts),
            counts=Counts(raw=self.config.expected_areas),
        )

    def parse(self, partition: Partition, store: ArtifactStore) -> Iterator[Mapping[str, object]]:
        require(
            partition.schema_version == SCHEMA and len(partition.artifacts) > 1,
            "ons-missing-raw-inputs",
        )
        with payload(store, partition.artifacts[-1]) as stream:
            definition = json.load(stream)
        config = ReferenceConfig.model_validate(definition["config"])
        require(config == self.config, "ons-config-mismatch")
        require(
            hashlib.sha256(definition["manifest_text"].encode()).hexdigest()
            == config.manifest_sha256,
            "ons-manifest-pin",
        )
        pins = json.loads(definition["manifest_text"])
        require(
            definition["names"] == sorted(pins["artifacts"])
            and len(partition.artifacts) == len(definition["names"]) + 1,
            "ons-incomplete-product-set",
        )
        require(
            config.geography_vintage == "LSOA-2021"
            and config.population_sheet == "Mid-2024 LSOA 2021"
            and config.population_header_row == 4
            and config.population_fields == tuple(geography.POP_FIELDS)
            and config.population_reference_date == date(2024, 6, 30),
            "ons-vintage-migration-required",
        )
        with tempfile.TemporaryDirectory(prefix="urbanlens-reference-") as directory:
            temporary = Path(directory)
            raw, out, pin = temporary / "raw", temporary / "output", temporary / "manifest.json"
            raw.mkdir()
            out.mkdir()
            pin.write_text(definition["manifest_text"], encoding="utf-8")
            for name, artifact in zip(definition["names"], partition.artifacts[:-1], strict=True):
                require(
                    Path(name).name == name and not name.startswith("."), "ons-unsafe-artifact-name"
                )
                digest = hashlib.sha256()
                with (raw / name).open("wb") as target:
                    for block in store.read_verified(artifact):
                        digest.update(block)
                        target.write(block)
                require(
                    digest.hexdigest() == pins["artifacts"][name]["sha256"], "ons-product-checksum"
                )
            geography.validate(raw=raw, out=out, pin=pin)
            with (out / "target-areas.csv").open(encoding="utf-8", newline="") as stream:
                values = list(csv.DictReader(stream))
            codes = sorted(row["LSOA21CD"] for row in values)
            require(
                len(codes) == len(set(codes)) == config.expected_areas
                and hashlib.sha256("\n".join(codes).encode()).hexdigest()
                == config.target_codes_sha256,
                "ons-target-code-mismatch",
            )
            geometries = {
                r["properties"]["LSOA21CD"]: r["geometry"]
                for r in json.loads((raw / "areas-wgs84.geojson").read_text())["features"]
            }
            editions = {
                k: pins[k]
                for k in ("geography", "boundary", "membership", "land_area", "population")
            }
            for row in values:
                counts = {
                    k: int(row[k]) for k in ("population", "age_0_15", "age_16_64", "age_65_plus")
                }
                require(
                    all(v >= 0 for v in counts.values())
                    and sum(v for k, v in counts.items() if k != "population")
                    == counts["population"]
                    and float(row["land_km2"]) > 0,
                    "ons-invalid-area-values",
                )
                yield {
                    "lsoa21cd": row["LSOA21CD"],
                    "name": row["LSOA21NM"],
                    "authority": row["LAD22CD"],
                    **counts,
                    "land_hectares": row["land_hectares"],
                    "land_km2": row["land_km2"],
                    "geometry": geometries[row["LSOA21CD"]],
                    "reference_period": config.population_reference_date.isoformat(),
                    "geography_vintage": config.geography_vintage,
                    "product_editions": editions,
                    "source_manifest_sha256": config.manifest_sha256,
                    "source_acquired_at": partition.fetched_at.isoformat()
                    if partition.fetched_at
                    else None,
                }


def check_editions(
    config: ReferenceConfig,
    manifest: bytes,
    store: ArtifactStore,
    *,
    baseline_directory: Path | None = None,
) -> dict[str, Any]:
    """Monthly live check stages changed metadata/bytes, never updates the pinned baseline."""
    require(hashlib.sha256(manifest).hexdigest() == config.manifest_sha256, "ons-manifest-pin")
    pins = json.loads(manifest)
    client = HttpDownloader(
        Provider.ONS,
        HttpPolicy(
            max_requests=10,
            max_retries=1,
            max_bytes=50_000_000,
            max_seconds=240,
            timeout_seconds=60,
            max_redirects=2,
        ),
    )
    now = datetime.now(UTC)
    cycle = config.cycle(now.date())
    run = uuid5(cycle.cycle_id, now.isoformat())
    results = []
    for name in ("boundary-item.json", "lookup-item.json", "sam-item.json", "population.xlsx"):
        old = pins["artifacts"][name]
        with tempfile.TemporaryFile() as stream:
            client.download(old["url"], stream)
            artifact = store.put_immutable(
                artifact_key(Layer.BRONZE, cycle, run, "check-" + name),
                chunks(stream),
                media_type="application/octet-stream",
                compression="none",
            )
            indicators: bool | None = None
            changed_fields: list[str] = []
            if name.endswith(".json") and baseline_directory is not None:
                stream.seek(0)
                observed = json.load(stream)
                original = json.loads((baseline_directory / name).read_text())
                fields = ("id", "modified", "title", "url")
                changed_fields = [
                    field for field in fields if original.get(field) != observed.get(field)
                ]
                indicators = not changed_fields
        results.append(
            {
                "product": name,
                "pinned_sha256": old["sha256"],
                "observed_sha256": artifact.sha256,
                "bytes_unchanged": artifact.sha256 == old["sha256"],
                "edition_indicators_unchanged": indicators,
                "changed_indicator_fields": changed_fields,
                "candidate_artifact": artifact.model_dump(mode="json"),
            }
        )
    pinned_url = pins["artifacts"]["population.xlsx"]["url"]
    uri = parse_qs(urlsplit(pinned_url).query).get("uri", [""])[0]
    dataset_path = uri.rsplit("/", 2)[0]
    require(
        dataset_path.startswith("/peoplepopulationandcommunity/"), "ons-unrecognized-dataset-page"
    )
    landing_url = "https://www.ons.gov.uk" + dataset_path
    with tempfile.TemporaryFile() as stream:
        client.download(landing_url, stream)
        listing = store.put_immutable(
            artifact_key(Layer.BRONZE, cycle, run, "check-population-dataset.html"),
            chunks(stream),
            media_type="text/html",
            compression="none",
        )
        stream.seek(0)
        html = stream.read().decode()
    links = [urljoin(landing_url, unescape(link)) for link in re.findall(r'href="([^\"]+)"', html)]
    downloads = [
        link
        for link in links
        if urlsplit(link).hostname == "www.ons.gov.uk"
        and urlsplit(link).scheme == "https"
        and (candidate := parse_qs(urlsplit(link).query).get("uri", [""])[0]).startswith(
            dataset_path + "/"
        )
        and candidate.endswith(".xlsx")
    ]
    require(bool(downloads), "ons-population-edition-discovery-failed")
    latest = downloads[0]
    different = parse_qs(urlsplit(latest).query).get("uri") != parse_qs(
        urlsplit(pinned_url).query
    ).get("uri")
    candidate_artifact = None
    if different:
        with tempfile.TemporaryFile() as stream:
            client.download(latest, stream)
            candidate_artifact = store.put_immutable(
                artifact_key(Layer.BRONZE, cycle, run, "candidate-population.xlsx"),
                chunks(stream),
                media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                compression="none",
            ).model_dump(mode="json")
    return {
        "checked_at": now.isoformat(),
        "manifest_sha256": config.manifest_sha256,
        "cadence": "calendar-monthly",
        "pinned_baseline_unchanged": True,
        "products": results,
        "population_discovery": {
            "listing_artifact": listing.model_dump(mode="json"),
            "latest_listed_download": latest,
            "different_from_pinned": different,
            "candidate_artifact": candidate_artifact,
        },
        "action": "Stage changed candidates for explicit compatibility review; "
        "metadata byte changes alone do not prove an edition change",
    }

"""Monthly cloud entry point using the same adapters, gates and fenced S3 registry."""

from __future__ import annotations

import hashlib
import json
import os
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import boto3
from botocore.config import Config

from urbanlens.ingestion.acceptance import AcceptanceService
from urbanlens.ingestion.archive import load_catalog
from urbanlens.ingestion.cadence import legacy_month_snapshot
from urbanlens.ingestion.companies import CompanyConfig, ProfileAdapter
from urbanlens.ingestion.contracts import FailureClass, Snapshot
from urbanlens.ingestion.engine import Collector
from urbanlens.ingestion.errors import IngestionError, LeaseLost
from urbanlens.ingestion.fsa import PIPELINE as FSA_PIPELINE
from urbanlens.ingestion.fsa import FsaAdapter, FsaConfig
from urbanlens.ingestion.osm import PIPELINE as OSM_PIPELINE
from urbanlens.ingestion.osm import OsmAdapter, OsmConfig, local_source
from urbanlens.ingestion.payload import payload
from urbanlens.ingestion.registry import Registry, S3StateBackend
from urbanlens.ingestion.storage import S3ArtifactStore
from urbanlens.ingestion.verification import replay_snapshot

ROOT = Path(__file__).resolve().parents[2]
SDK_CONFIG = Config(connect_timeout=5, read_timeout=30, retries={"total_max_attempts": 2})


def sdk_client(service: str, *, profile: str | None = None) -> Any:
    session = boto3.Session(profile_name=profile, region_name="eu-west-2")
    return session.client(service, config=SDK_CONFIG)


class CloudProfileBudget:
    """Atomic monthly attempt budget shared across Lambda invocations and retries."""

    def __init__(self, backend: S3StateBackend, limit: int, month: str) -> None:
        self.backend, self.limit, self.key = backend, limit, f"profiles/{month}.json"

    def claim(self) -> None:
        for _ in range(5):
            item = self.backend.load(self.key)
            used = 0 if item is None else int(json.loads(item[0])["used"])
            if used >= self.limit:
                raise IngestionError(FailureClass.BUDGET, "company-profile-monthly-request-budget")
            try:
                self.backend.compare_set(
                    self.key,
                    None if item is None else item[1],
                    json.dumps({"used": used + 1}).encode(),
                )
                return
            except LeaseLost:
                continue
        raise IngestionError(FailureClass.BUDGET, "profile-budget-contention")


def collect(provider: str, bucket: str, catalog_key: str, *, root: Path | None = None) -> Snapshot:
    root = root or ROOT
    client = sdk_client("s3")
    store = load_catalog(S3ArtifactStore(client, bucket), catalog_key)
    registry = Registry(S3StateBackend(client, bucket), lease_seconds=360)
    policy = json.loads((root / "config/source-acceptance.json").read_text())
    if policy["version"] != "source-acceptance-v1":
        raise ValueError("Unsupported acceptance policy")
    gate = AcceptanceService(
        store, change_records=policy["change_records"], change_fraction=policy["change_fraction"]
    )
    today = datetime.now(UTC).date()
    collector = Collector(store, registry, acceptance=gate, max_seconds=280)
    if provider == "fsa":
        config = FsaConfig.model_validate_json((root / "config/fsa-source.json").read_text())
        cycle = config.cycle(today)
        legacy = legacy_month_snapshot(registry, cycle, config, "fsa-authority-v1")
        return legacy or collector.collect(
            cycle, FsaAdapter(config), config.rules(), pipeline_version=FSA_PIPELINE
        )
    if provider == "osm":
        osm_config, source = local_source(root)
        cycle = osm_config.cycle(today)
        legacy = legacy_month_snapshot(registry, cycle, osm_config, "osm-venues-v1")
        return legacy or collector.collect(
            cycle, OsmAdapter(osm_config, source), osm_config.rules(), pipeline_version=OSM_PIPELINE
        )
    if provider == "companies-house-profiles":
        company_config = CompanyConfig.model_validate_json(
            (root / "config/companies-source.json").read_text()
        )
        numbers = ProfileAdapter(company_config).numbers
        digest = hashlib.sha256("\n".join(numbers).encode()).hexdigest()
        cycle = company_config.cycle(today, "tracked-profiles", digest, numbers, profiles=True)
        current = registry.current(cycle)
        if current is not None and current.cycle == cycle:
            return current  # No secret read or API use on an accepted monthly rerun.
        secret = sdk_client("ssm").get_parameter(
            Name="/urbanlens/companies-house-api-key", WithDecryption=True
        )["Parameter"]["Value"]
        budget = CloudProfileBudget(
            S3StateBackend(client, bucket, "budgets"),
            company_config.max_profiles * 2,
            today.strftime("%Y-%m"),
        )
        adapter = ProfileAdapter(company_config, secret, request_budget=budget.claim)
        return collector.collect(
            cycle,
            adapter,
            company_config.rules(profiles=True),
            pipeline_version="company-source-v1",
        )
    raise ValueError("Unsupported cloud provider")


def handler(event: dict[str, Any], context: Any) -> dict[str, Any]:
    provider = event.get("provider")
    if provider not in {"fsa", "osm", "companies-house-profiles"}:
        raise ValueError("Unsupported cloud provider")
    operation = event.get("operation", "collect")
    if operation not in {"collect", "replay"}:
        raise ValueError("Unsupported cloud operation")
    snapshot = collect(provider, os.environ["BRONZE_BUCKET"], os.environ["BOOTSTRAP_CATALOG"])
    result = {
        "provider": provider,
        "status": snapshot.run.status.value,
        "snapshot_id": str(snapshot.snapshot_id),
        "cycle_id": str(snapshot.cycle.cycle_id),
        "fetched_at": [
            p.fetched_at.isoformat() if p.fetched_at else None for p in snapshot.partitions
        ],
        "accepted": sum(p.counts.accepted or 0 for p in snapshot.partitions),
        "failed_checks": [c.code for c in snapshot.validation.checks if c.outcome != "passed"],
    }
    if operation == "replay":
        client = sdk_client("s3")
        store = load_catalog(
            S3ArtifactStore(client, os.environ["BRONZE_BUCKET"]),
            os.environ["BOOTSTRAP_CATALOG"],
        )
        if provider == "fsa":
            config = FsaConfig.model_validate_json((ROOT / "config/fsa-source.json").read_text())
            replay = replay_snapshot(snapshot, FsaAdapter(config), config.rules(), store)
        elif provider == "osm":
            with payload(store, snapshot.partitions[0].artifacts[1]) as stream:
                osm = OsmConfig.model_validate(json.load(stream)["config"])
            replay = replay_snapshot(snapshot, OsmAdapter(osm), osm.rules(), store)
        else:
            companies = CompanyConfig.model_validate_json(
                (ROOT / "config/companies-source.json").read_text()
            )
            replay = replay_snapshot(
                snapshot, ProfileAdapter(companies), companies.rules(profiles=True), store
            )
        registry = Registry(S3StateBackend(client, os.environ["BRONZE_BUCKET"]))
        if registry.current(snapshot.cycle) != snapshot:
            raise ValueError("Cloud replay changed the source baseline")
        result["replay"] = replay
    return result

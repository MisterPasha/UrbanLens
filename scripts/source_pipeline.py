"""Local source work for steps 14–17. No deployment, database load or publication."""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import os
import tempfile
from collections import Counter
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any
from uuid import uuid4

from dotenv import dotenv_values

from urbanlens.ingestion.acceptance import AcceptanceService
from urbanlens.ingestion.companies import PIPELINE as COMPANY_PIPELINE
from urbanlens.ingestion.companies import (
    CompanyAdapter,
    CompanyConfig,
    ProfileAdapter,
    ProfileRequestBudget,
)
from urbanlens.ingestion.contracts import FailureClass, Provider, Snapshot, Stage
from urbanlens.ingestion.engine import Collector, metadata_key
from urbanlens.ingestion.errors import IngestionError
from urbanlens.ingestion.http import HttpDownloader, HttpPolicy
from urbanlens.ingestion.payload import payload
from urbanlens.ingestion.postcodes import CacheEntry, PostcodeService, normalize, validate_result
from urbanlens.ingestion.reference import PIPELINE as REFERENCE_PIPELINE
from urbanlens.ingestion.reference import ReferenceAdapter, ReferenceConfig, check_editions
from urbanlens.ingestion.registry import Registry, SQLiteStateBackend
from urbanlens.ingestion.storage import LocalArtifactStore

ROOT = Path(__file__).resolve().parents[1]
PILOT = ROOT / "data/pilots/companies/2026-11-02"
DIRECTORY = ROOT / "data/ingestion"


def area_codes() -> set[str]:
    with (ROOT / "reference/geography/target-areas.csv").open(
        encoding="utf-8", newline=""
    ) as stream:
        return {row["LSOA21CD"] for row in csv.DictReader(stream)}


def postcode_service(store: LocalArtifactStore) -> PostcodeService:
    return PostcodeService(DIRECTORY / "postcodes.sqlite", store, area_codes())


def acceptance_service(store: LocalArtifactStore) -> AcceptanceService:
    policy = json.loads((ROOT / "config/source-acceptance.json").read_text())
    if (
        policy["version"] != "source-acceptance-v1"
        or not policy["first_baseline_requires_review"]
        or policy["structural_failures_overridable"]
    ):
        raise ValueError("Unsupported acceptance policy")
    return AcceptanceService(
        store, change_records=policy["change_records"], change_fraction=policy["change_fraction"]
    )


def seed_postcode_cache(service: PostcodeService, store: LocalArtifactStore) -> int:
    """Import verified retained responses; never restamp their original lookup dates."""
    count = 0
    now = datetime.now(UTC)
    for meta_path in sorted(PILOT.glob("postcodes-*.meta.json")):
        meta = json.loads(meta_path.read_text())
        path = meta_path.with_name(meta_path.name.replace(".meta.json", ".json"))
        content = path.read_bytes()
        if hashlib.sha256(content).hexdigest() != meta["sha256"]:
            raise ValueError("Pilot postcode checksum mismatch")
        fetched = datetime.fromisoformat(meta["fetched_at"])
        artifact = store.put_immutable(
            "bronze/postcodes/retained-pilot/" + path.name,
            iter([content]),
            media_type="application/json",
            compression="none",
        )
        body = json.loads(content)
        if body.get("status") != 200:
            raise ValueError("Invalid retained postcode response")
        for item in body["result"]:
            key, result = normalize(item["query"]), item["result"]
            validate_result(result, key)
            if service.cached(key, now) is None:
                service.save(
                    CacheEntry(
                        postcode=key,
                        result=result,
                        fetched_at=fetched,
                        expires_at=fetched + timedelta(days=30 if result is not None else 1),
                        artifact=artifact,
                    )
                )
                count += 1
    return count


def summary(snapshot: Snapshot) -> dict[str, Any]:
    def total(field: str) -> int | None:
        values = [getattr(p.counts, field) for p in snapshot.partitions]
        return None if any(v is None for v in values) else sum(values)

    return {
        "provider": snapshot.cycle.provider.value,
        "scope": snapshot.cycle.scope,
        "cycle_id": str(snapshot.cycle.cycle_id),
        "run_id": str(snapshot.run.run_id),
        "status": snapshot.run.status.value,
        "edition": snapshot.cycle.source_edition,
        "partitions": len(snapshot.partitions),
        "expected_partitions": len(snapshot.cycle.expected_partitions),
        "raw": total("raw"),
        "accepted": total("accepted"),
        "excluded_or_rejected": total("rejected"),
        "fetched_at": [
            None if p.fetched_at is None else p.fetched_at.isoformat() for p in snapshot.partitions
        ],
        "failed_checks": [c.code for c in snapshot.validation.checks if c.outcome != "passed"],
        "failure": None
        if snapshot.run.failure is None
        else snapshot.run.failure.model_dump(mode="json"),
    }


def company_job(task: str, store: LocalArtifactStore, registry: Registry) -> Snapshot:
    config = CompanyConfig.model_validate_json((ROOT / "config/companies-source.json").read_text())
    today = datetime.now(UTC).date()
    if task == "company-profiles":
        # After S3 handover, recurring profile attempts must use the shared cloud
        # request ledger. Cached local inspection remains available without network.
        handover = ROOT / ".cache/bronze-state.json"
        probe_adapter = ProfileAdapter(config)
        probe_digest = hashlib.sha256("\n".join(probe_adapter.numbers).encode()).hexdigest()
        probe_cycle = config.cycle(
            today, "tracked-profiles", probe_digest, probe_adapter.numbers, profiles=True
        )
        previous = registry.current(probe_cycle)
        if handover.exists() and (previous is None or previous.cycle != probe_cycle):
            raise IngestionError(FailureClass.SCOPE, "tracked-profiles-handed-over-to-cloud")
        values = dotenv_values(ROOT / ".env")
        key = os.environ.get("COMPANIES_HOUSE_API_KEY") or values.get("COMPANIES_HOUSE_API_KEY")
        retained: set[str] = set()
        for path in store.root.glob(
            "manifests/companies-house/*-profiles/*/*/attempts/*/snapshot.json"
        ):
            old = Snapshot.model_validate_json(path.read_text())
            for part in old.partitions:
                if part.fetched_at and part.fetched_at.strftime("%Y-%m") == today.strftime("%Y-%m"):
                    retained.update(a.uri for a in part.artifacts)
        budget = ProfileRequestBudget(
            DIRECTORY / "profile-requests.sqlite",
            config.max_profiles * 2,
            initial_used=len(retained),
        )
        adapter = ProfileAdapter(config, key, request_budget=budget.claim)
        digest = hashlib.sha256("\n".join(adapter.numbers).encode()).hexdigest()
        cycle = config.cycle(today, "tracked-profiles", digest, adapter.numbers, profiles=True)
        return Collector(store, registry, acceptance=acceptance_service(store), log=print).collect(
            cycle, adapter, config.rules(profiles=True), pipeline_version=COMPANY_PIPELINE
        )
    # Reuse an accepted month before discovery/network/bootstrap work. Verify its
    # retained listing still matches this configuration, edition and full part set.
    probe = config.cycle(today, "probe", "0" * 64, ("full",))
    current = registry.current(probe)
    if (
        current is not None
        and current.cycle.source_edition is not None
        and current.cycle.window_start == probe.window_start
    ):
        with payload(store, current.partitions[0].artifacts[1]) as stream:
            retained_listing = stream.read()
        expected = config.cycle(
            today,
            current.cycle.source_edition,
            hashlib.sha256(retained_listing).hexdigest(),
            current.cycle.expected_partitions,
        )
        if expected == current.cycle:
            return current
    service = postcode_service(store)
    seed_postcode_cache(service, store)
    # The official postcode directory is a pinned prefilter, not a final area assignment.
    from scripts.company_pilot import local_directory

    directory = local_directory()
    outcodes = {key[:-3] for key in directory}
    bootstrap = {}
    fetched_at = None
    if task == "company-bootstrap":
        manifest = json.loads((PILOT / "manifest.json").read_text())
        listing = (PILOT / "listing.html").read_bytes()
        if hashlib.sha256(listing).hexdigest() != manifest["listing.html"]["sha256"]:
            raise ValueError("Company listing checksum mismatch")
        fetched_at = datetime.fromisoformat(manifest["listing.html"]["fetched_at"])
        for name, record in manifest.items():
            if name.startswith("BasicCompanyData-") and name.endswith(".zip"):
                bootstrap[name] = (
                    PILOT / name,
                    record["sha256"],
                    datetime.fromisoformat(record["fetched_at"]),
                )
    else:
        downloader = HttpDownloader(
            Provider.COMPANIES_HOUSE, HttpPolicy(max_requests=2, max_retries=1, max_bytes=1_000_000)
        )
        with tempfile.TemporaryFile() as stream:
            downloader.download("https://download.companieshouse.gov.uk/en_output.html", stream)
            listing = stream.read()
        store.put_immutable(
            f"bronze/companies-house/discovery/{uuid4()}.html",
            iter([listing]),
            media_type="text/html",
            compression="none",
        )
    adapter_bulk = CompanyAdapter(
        config, listing, service, outcodes, bootstrap=bootstrap, listing_fetched_at=fetched_at
    )
    cycle = config.cycle(
        today,
        adapter_bulk.edition,
        hashlib.sha256(listing).hexdigest(),
        tuple(sorted(adapter_bulk.parts)),
    )
    return Collector(
        store,
        registry,
        max_seconds=1800,
        max_records=10_000_000,
        acceptance=acceptance_service(store),
        log=print,
    ).collect(cycle, adapter_bulk, config.rules(), pipeline_version=COMPANY_PIPELINE)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "task",
        choices=[
            "postcode-check",
            "company-bootstrap",
            "company-fetch",
            "company-profiles",
            "reference-import",
            "reference-check",
            "status",
            "review",
        ],
    )
    parser.add_argument("--candidate", type=Path)
    parser.add_argument("--reviewer")
    parser.add_argument("--reason")
    parser.add_argument("--decision", choices=["approve", "reject"], default="approve")
    parser.add_argument(
        "--reference-config", type=Path, default=ROOT / "config/reference-source.json"
    )
    parser.add_argument(
        "--reference-manifest", type=Path, default=ROOT / "reference/geography/manifest.json"
    )
    parser.add_argument(
        "--reference-directory", type=Path, default=ROOT / "data/reference/geography"
    )
    args = parser.parse_args()
    store = LocalArtifactStore(DIRECTORY / "objects")
    registry = Registry(SQLiteStateBackend(DIRECTORY / "registry.sqlite"))
    try:
        if args.task == "postcode-check":
            service = postcode_service(store)
            first = service.bulk(
                ["bn1 1je", "BN11JE", "SW1A 1AA", "BN99ZZ", "ZZ99 9ZZ", "not a postcode"],
                role="search-centre",
            )
            requests = service.downloader.requests
            repeated = service.bulk([r.raw_input for r in first], role="search-centre")
            report = {
                "states": dict(Counter(r.state for r in first)),
                "repeat_network_requests": service.downloader.requests - requests,
                "cache_hits": sum(r.cache_hit for r in repeated),
                "primary_spatial_eligible": sum(r.primary_spatial_eligible for r in first),
            }
            (DIRECTORY / "postcode-check.json").write_text(json.dumps(report, indent=2) + "\n")
            print(json.dumps(report, indent=2))
        elif args.task.startswith("company-"):
            snapshot = company_job(args.task, store, registry)
            print(json.dumps(summary(snapshot), indent=2))
            print(
                store.path(metadata_key(snapshot.cycle, f"attempts/{snapshot.run.run_id}/snapshot"))
            )
        elif args.task in {"reference-import", "reference-check"}:
            config = ReferenceConfig.model_validate_json(args.reference_config.read_text())
            manifest = args.reference_manifest.read_bytes()
            if args.task == "reference-check":
                report_path = DIRECTORY / "reference-edition-check.json"
                previous = json.loads(report_path.read_text()) if report_path.exists() else None
                now = datetime.now(UTC)
                if (
                    previous
                    and previous.get("manifest_sha256") == config.manifest_sha256
                    and datetime.fromisoformat(previous["checked_at"]).strftime("%Y-%m")
                    == now.strftime("%Y-%m")
                ):
                    report = previous
                else:
                    report = check_editions(
                        config, manifest, store, baseline_directory=args.reference_directory
                    )
                report_path.write_text(json.dumps(report, indent=2) + "\n")
                print(json.dumps(report, indent=2))
            else:
                snapshot = Collector(
                    store, registry, acceptance=acceptance_service(store), log=print
                ).collect(
                    config.cycle(datetime.now(UTC).date()),
                    ReferenceAdapter(config, args.reference_directory, manifest),
                    config.rules(),
                    pipeline_version=REFERENCE_PIPELINE,
                )
                print(json.dumps(summary(snapshot), indent=2))
                print(
                    store.path(
                        metadata_key(snapshot.cycle, f"attempts/{snapshot.run.run_id}/snapshot")
                    )
                )
        elif args.task == "review":
            if args.candidate is None or not args.reviewer or not args.reason:
                parser.error("review needs --candidate, --reviewer and --reason")
            snapshot = Snapshot.model_validate_json(args.candidate.read_text())
            structural = [
                c
                for c in snapshot.validation.checks
                if c.code not in {"baseline-review", "anomaly-review"}
            ]
            if args.decision == "approve" and (
                snapshot.completeness != "complete"
                or not structural
                or any(c.outcome != "passed" for c in structural)
            ):
                raise ValueError("Structural failures cannot be approved")
            for part in snapshot.partitions:
                for artifact in part.artifacts:
                    for _ in store.read_verified(artifact):
                        pass
            artifact = acceptance_service(store).record(
                snapshot.cycle,
                snapshot.partitions,
                reviewer=args.reviewer,
                reviewed_at=datetime.now(UTC),
                decision=args.decision,
                reason=args.reason,
            )
            print(
                json.dumps(
                    {"review": artifact.uri, "decision": args.decision, "source_not_promoted": True}
                )
            )
        else:
            latest: dict[tuple[str, str], Snapshot] = {}
            for path in sorted(store.root.glob("manifests/*/*/*/*/attempts/*/snapshot.json")):
                candidate = Snapshot.model_validate_json(path.read_text())
                key = (candidate.cycle.provider.value, candidate.cycle.scope)
                if key not in latest or candidate.run.started_at > latest[key].run.started_at:
                    latest[key] = candidate
            reports = []
            for candidate in latest.values():
                current = registry.current(candidate.cycle)
                reviewed = current is not None and (
                    "baseline-review" in current.validation.required_checks
                    or acceptance_service(store).reviewed(current.cycle, current.partitions)
                )
                health = (
                    "healthy"
                    if candidate.run.status == Stage.ACCEPTED and reviewed
                    else "held"
                    if candidate.run.status == Stage.REJECTED
                    else "failed"
                )
                reports.append(
                    {
                        "provider": candidate.cycle.provider.value,
                        "scope": candidate.cycle.scope,
                        "health": health,
                        "latest_attempt": summary(candidate),
                        "current": None if current is None else summary(current),
                        "original_source_dates_retained": True,
                    }
                )
            print(json.dumps(reports, indent=2))
    except IngestionError as error:
        raise SystemExit(error.failure.model_dump_json()) from None
    except ValueError:
        raise SystemExit(
            "Source validation/configuration failed; accepted state retained"
        ) from None


if __name__ == "__main__":
    main()

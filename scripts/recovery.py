"""Create a private recovery point and verify it only in a disposable local database."""

from __future__ import annotations

import argparse
import json
import time
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, cast
from unittest.mock import patch
from uuid import uuid4

from botocore.exceptions import ClientError
from fastapi.testclient import TestClient
from psycopg import sql
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb
from pydantic import SecretStr

from scripts.backup import code_archive, restore_check, run
from scripts.database import credential
from scripts.dev import ROOT
from scripts.gold import verify as verify_gold
from scripts.maintenance import lifecycle
from scripts.production import private_api_smoke
from scripts.silver import environment
from scripts.source_pipeline import area_codes
from urbanlens.api.app import create_app
from urbanlens.config import Settings
from urbanlens.database.migrations import connect
from urbanlens.database.releases import check_contents, publish, schema_for
from urbanlens.ingestion.companies import CompanyAdapter, CompanyConfig, ProfileAdapter
from urbanlens.ingestion.contracts import Artifact, Provider
from urbanlens.ingestion.fsa import FsaAdapter, FsaConfig
from urbanlens.ingestion.http import HttpDownloader
from urbanlens.ingestion.osm import OsmAdapter, OsmConfig
from urbanlens.ingestion.payload import payload
from urbanlens.ingestion.postcodes import CacheEntry, PostcodeService
from urbanlens.ingestion.reference import ReferenceAdapter, ReferenceConfig
from urbanlens.ingestion.registry import S3StateBackend
from urbanlens.ingestion.storage import S3ArtifactStore
from urbanlens.operations.control import Coordinator
from urbanlens.silver.audit import semantic_state
from urbanlens.silver.classification import save_json
from urbanlens.silver.eligibility import digest
from urbanlens.silver.operations import cutoff, prune
from urbanlens.silver.pipeline import Bundle, build, input_rows, verified_observations


def api(database: str, release: str | None, path: str = "establishments") -> dict[str, Any]:
    settings = Settings(
        environment="test",
        api_database_url=SecretStr(credential("local", "api", database=database)),
    )
    with TestClient(create_app(settings)) as http:
        response = http.get("/api/v1/" + path, params={"release_id": release} if release else {})
        assert response.status_code == 200, (path, response.status_code)
        body = dict(response.json())
        body.pop("request_id", None)
        return body


def raw_reconstruction(directory: Path) -> dict[str, Any]:
    started = time.monotonic()
    source, store, snapshots, entries, export, resolve = environment("s3")
    # The local bootstrap export may predate the last completed production run.
    entries, export = current_postcodes(store)
    config = CompanyConfig.model_validate_json((ROOT / "config/companies-source.json").read_bytes())
    postcodes = PostcodeService(directory / "postcodes.sqlite", source, area_codes())
    reports = []
    with patch.object(
        HttpDownloader, "download", side_effect=ValueError("Provider HTTP disabled during recovery")
    ):
        for snapshot in snapshots:
            for part in snapshot.partitions:
                provider = snapshot.cycle.provider
                if provider == Provider.FSA:
                    adapter: Any = FsaAdapter(
                        FsaConfig.model_validate_json(
                            (ROOT / "config/fsa-source.json").read_bytes()
                        )
                    )
                elif provider == Provider.OSM:
                    with payload(source, part.artifacts[1]) as stream:
                        definition = json.load(stream)
                    adapter = OsmAdapter(OsmConfig.model_validate(definition["config"]))
                elif provider == Provider.ONS:
                    with payload(source, part.artifacts[-1]) as stream:
                        definition = json.load(stream)
                    adapter = ReferenceAdapter(ReferenceConfig.model_validate(definition["config"]))
                elif snapshot.cycle.scope.endswith("-profiles"):
                    adapter = ProfileAdapter(config)
                else:
                    with payload(source, part.artifacts[1]) as stream:
                        listing = stream.read()
                    adapter = CompanyAdapter(config, listing, postcodes, set())
                rows = list(adapter.parse(part, source))
                accepted = [
                    r
                    for r in rows
                    if provider != Provider.COMPANIES_HOUSE
                    or snapshot.cycle.scope.endswith("-profiles")
                    or r.get("cohort_included") is True
                ]
                parsed, retained = input_rows(snapshot, part, source)
                assert digest(accepted) == digest(retained), (provider, part.partition_id)
                assert len(accepted) == part.counts.accepted
                reports.append(
                    dict(
                        provider=provider.value,
                        partition=part.partition_id,
                        accepted=len(accepted),
                        parsed_artifact=resolve(parsed).model_dump(mode="json"),
                        identical_to_retained=True,
                    )
                )
                print(
                    json.dumps(dict(raw_rebuilt=provider.value, partition=part.partition_id)),
                    flush=True,
                )
        bundle, manifest = build(
            snapshots,
            source,
            store,
            resolve=resolve,
            entries=entries,
            postcode_manifest=export,
            area_codes=area_codes(),
        )
    # Each immutable normalised record is decoded/hashed again, not just its manifest.
    counts = {
        str(b.records.uri): sum(1 for _ in verified_observations(store, b)) for b in bundle.batches
    }
    assert all(counts[b.records.uri] == b.count for b in bundle.batches)
    return dict(
        partitions=reports,
        silver_manifest=manifest.model_dump(mode="json"),
        normalised_partitions=len(counts),
        provider_http_requests=0,
        seconds=round(time.monotonic() - started, 2),
    )


def current_postcodes(store: Any) -> tuple[list[CacheEntry], Artifact]:
    definition, _ = Coordinator(
        S3StateBackend(store.client, store.bucket, "operations"), None, ""
    ).read("postcode-export.json")
    export = Artifact.model_validate(definition)
    body = json.loads(b"".join(store.read_verified(export)))
    return [CacheEntry.model_validate(e) for e in body["entries"]], export


def rollback(database: str) -> dict[str, Any]:
    dsn = credential("local", "migration", database=database)
    with connect(dsn) as c:
        slots = dict(c.execute("SELECT slot,release_id FROM ul_control.release_slot"))
        active, previous = slots["active"], slots["previous"]
        rows = {
            r[0]: r
            for r in c.execute(
                "SELECT release_id,model_schema,validated_contents "
                "FROM ul_control.data_release WHERE release_id=ANY(%s)",
                ([active, previous],),
            )
        }
        for identifier, (_, schema, contents) in rows.items():
            assert schema == schema_for(identifier)
            check_contents(c, schema, contents)
        original_state = semantic_state(c)
        model_checks = {
            str(identifier): verify_gold(c, r[1])["counts"] for identifier, r in rows.items()
        }
    before = api(database, str(active))
    previous_before = api(database, str(previous))
    assert api(database, None)["release_id"] == str(active)
    with connect(dsn) as c:
        bad = uuid4()
        c.execute(
            "INSERT INTO ul_control.data_release SELECT x.* FROM ul_control.data_release d "
            "CROSS JOIN LATERAL jsonb_populate_record(NULL::ul_control.data_release,"
            "to_jsonb(d)||%s::jsonb) x WHERE d.release_id=%s",
            (
                Jsonb(
                    dict(
                        release_id=str(bad),
                        model_schema=schema_for(bad),
                        status="building",
                        quality_passed=False,
                        published_at=None,
                    )
                ),
                active,
            ),
        )
        try:
            publish(c, bad)
        except ValueError as error:
            assert "validated" in str(error)
        else:
            raise ValueError("Unvalidated candidate was accepted")
        c.execute("DELETE FROM ul_control.data_release WHERE release_id=%s", (bad,))
        schema = sql.Identifier(rows[active][1])
        sample = c.execute(
            sql.SQL(
                "SELECT establishment_id,display_name FROM {}.mart_establishment "
                "ORDER BY establishment_id LIMIT 1"
            ).format(schema)
        ).fetchone()
        assert sample
        c.execute(
            sql.SQL(
                "UPDATE {}.mart_establishment SET display_name='RECOVERY TEST DAMAGED NAME' "
                "WHERE establishment_id=%s"
            ).format(schema),
            (sample[0],),
        )
    damaged = api(database, str(active), "establishments/" + str(sample[0]))
    assert damaged["data"]["display_name"] == "RECOVERY TEST DAMAGED NAME"
    with connect(dsn) as c:
        try:
            check_contents(c, rows[active][1], rows[active][2])
        except ValueError:
            pass
        else:
            raise ValueError("Published-content damage was undetected")
        try:
            publish(c, previous, rollback=True, fail_after_pointer=True)
        except ValueError as error:
            assert "Injected" in str(error)
        else:
            raise ValueError("Interrupted rollback was accepted")
    assert api(database, None)["release_id"] == str(active)
    with connect(dsn) as c:
        result = publish(c, previous, rollback=True)
    assert api(database, None)["release_id"] == str(previous)
    assert api(database, str(previous)) == previous_before
    assert api(database, str(active), "establishments/" + str(sample[0])) == damaged
    with connect(dsn) as c:
        c.execute(
            sql.SQL(
                "UPDATE {}.mart_establishment SET display_name=%s WHERE establishment_id=%s"
            ).format(schema),
            (sample[1], sample[0]),
        )
        check_contents(c, rows[active][1], rows[active][2])
        publish(c, active, rollback=True)
    assert api(database, str(active)) == before
    with connect(dsn) as c:
        assert semantic_state(c) == original_state
    return dict(
        original_active=str(active),
        original_previous=str(previous),
        bad_candidate_blocked=True,
        damaged_publication_detected=True,
        interrupted_rollback_atomic=True,
        previous_release_restored=True,
        pinned_reads_not_mixed=True,
        original_release_reinstated=True,
        semantic_history_unchanged=True,
        model_counts=model_checks,
        rollback=result,
    )


def retained_history(database: str, store: Any) -> dict[str, Any]:
    with connect(credential("local", "migration", database=database)) as c:
        with c.cursor(row_factory=dict_row) as cursor:
            cursor.execute(
                "SELECT e.*,a.sha256,a.byte_count,a.compression,a.media_type "
                "FROM ul_silver.change_event e JOIN ul_control.artifact a "
                "ON a.uri=e.retained_evidence_uri ORDER BY e.event_id"
            )
            events = cursor.fetchall()
        total = c.execute("SELECT count(*) FROM ul_silver.change_event").fetchone()
        assert total and total[0] == len(events)
        preview = prune(c, apply=False)
    proofs = []
    for row in events:
        uri = row["retained_evidence_uri"]
        assert "/evidence/history/" in uri and "/bronze/" not in uri
        artifact = Artifact(
            uri=uri,
            sha256=row["sha256"],
            byte_count=row["byte_count"],
            compression=row["compression"],
            media_type=row["media_type"],
        )
        body = json.loads(b"".join(store.read_verified(artifact)))
        assert digest(body) == row["event_key"]
        assert body["before"] == row["before_value"] and body["after"] == row["after_value"]
        assert body["evidence"] == row["evidence_fields"]
        proofs.append(
            dict(event_id=str(row["event_id"]), event_key=row["event_key"], evidence_uri=uri)
        )
    now = datetime.now(UTC)
    return dict(
        events_checked=len(proofs),
        event_identity_sha256=digest(proofs),
        examples=proofs[:3],
        bronze_reads=0,
        simulated_raw_unavailability=True,
        actual_events_older_than_90_days=sum(
            row["observed_at"] < now - timedelta(days=90) for row in events
        ),
        history_boundary=cutoff(now).isoformat(),
        retention_preview=preview,
        limitation="Current events are recent; unavailable-Bronze reads simulate later raw expiry, "
        "not an existing year-old dataset.",
    )


def retention(store: Any, directory: Path) -> dict[str, Any]:
    rules = store.client.get_bucket_lifecycle_configuration(Bucket=store.bucket)["Rules"]
    expected = lifecycle()["Rules"]

    def signature(rule: dict[str, Any]) -> tuple[Any, ...]:
        return (
            rule.get("Prefix", rule.get("Filter", {}).get("Prefix", "")),
            rule["Status"],
            rule.get("Expiration", {}).get("Days"),
            rule.get("AbortIncompleteMultipartUpload", {}).get("DaysAfterInitiation"),
        )

    assert {signature(r) for r in expected} <= {signature(r) for r in rules}
    assert not any(
        r.get("Expiration")
        and r.get("Prefix", r.get("Filter", {}).get("Prefix", ""))
        in ("", "silver/", "evidence/", "manifests/")
        for r in rules
    )
    examples = []
    for prefix, days in (("bronze/", 90), ("backups/neon/daily/", 7), ("backups/neon/weekly/", 28)):
        objects = store.client.list_objects_v2(Bucket=store.bucket, Prefix=prefix, MaxKeys=1).get(
            "Contents", []
        )
        if objects:
            obj = objects[0]
            earliest = obj["LastModified"] + timedelta(days=days)
            midnight = earliest.replace(hour=0, minute=0, second=0, microsecond=0)
            eligible = midnight if earliest == midnight else midnight + timedelta(days=1)
            examples.append(
                dict(
                    key=obj["Key"],
                    days=days,
                    eligible_utc=eligible.isoformat(),
                    actual_deletion="asynchronous; no deletion performed",
                )
            )
    try:
        versioning = store.client.get_bucket_versioning(Bucket=store.bucket)
        versioning = {k: v for k, v in versioning.items() if k != "ResponseMetadata"}
    except ClientError as error:
        if error.response["Error"]["Code"] != "AccessDenied":
            raise
        versioning = {"status": "unverified", "reason": "GetBucketVersioning permission denied"}
    return dict(
        rules=rules,
        compliant=True,
        expiry_examples=examples,
        destructive_actions=0,
        versioning=versioning,
        limitation="Expiry eligibility is not a guaranteed deletion time. Noncurrent versions, "
        "if enabled, require separate retention rules.",
    )


def verify_reconstruction(store: Any, report: dict[str, Any]) -> dict[str, Any]:
    """Validate a saved replay report and its immutable output when resuming."""
    bundle = Bundle.model_validate_json(
        b"".join(store.read_verified(Artifact.model_validate(report["silver_manifest"])))
    )
    now = datetime.now(UTC)
    ages = []
    _, postcode_export = current_postcodes(store)
    providers: dict[str, int] = {}
    for batch in bundle.batches:
        count = sum(1 for _ in verified_observations(store, batch))
        assert count == batch.count
        providers[batch.provider.value] = providers.get(batch.provider.value, 0) + count
        if batch.provider == Provider.POSTCODES:
            assert batch.source_manifest == postcode_export
        if batch.snapshot:
            for part in batch.snapshot.partitions:
                assert part.fetched_at is not None
                age = (now - part.fetched_at).total_seconds() / 86400
                assert 0 <= age < 90, "Selected raw acquisition is outside the recovery window"
                ages.append(age)
    assert all(p["identical_to_retained"] for p in report["partitions"])
    assert report["provider_http_requests"] == 0
    return dict(
        providers=providers,
        maximum_raw_acquisition_age_days=round(max(ages), 3),
        all_selected_raw_inputs_within_90_days=True,
        normalised_partitions_verified=len(bundle.batches),
        postcode_evidence="Dated retained cache; no fabricated provider snapshot",
    )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--backup-key", help="Reuse a retained recovery manifest instead of creating another point"
    )
    parser.add_argument(
        "--resume", type=Path, help="Finish a local rehearsal after its restore passed"
    )
    args = parser.parse_args()
    started = time.monotonic()
    directory = (
        args.resume.resolve()
        if args.resume
        else ROOT / "data/recovery" / datetime.now(UTC).strftime("%Y-%m-%d-%H%M%S")
    )
    if args.resume:
        if not directory.is_relative_to((ROOT / "data/recovery").resolve()) or args.backup_key:
            raise ValueError("Resume one workspace recovery directory without --backup-key")
    else:
        directory.mkdir(parents=True)
    _, remote, *_ = environment("s3")
    store = cast(S3ArtifactStore, remote)
    coordinator = Coordinator(S3StateBackend(store.client, store.bucket, "operations"), None, "")
    if coordinator.read("active.json")[0].get("run_key"):
        raise ValueError("Wait for the active production workflow before taking a recovery point")
    before = private_api_smoke()
    if args.resume:
        manifest = json.loads((directory / "selected-backup.json").read_bytes())
    elif args.backup_key:
        if not args.backup_key.startswith("backups/neon/") or not args.backup_key.endswith(".json"):
            raise ValueError("Select a private Neon backup manifest")
        artifact = store.stat(args.backup_key)
        assert artifact is not None
        manifest = json.loads(b"".join(store.read_verified(artifact)))
    else:
        backup = run(
            "neon",
            store,
            tier="daily",
            verify_restore=False,
            point_suffix="recovery-" + datetime.now(UTC).strftime("%H%M%S"),
        )
        (directory / "backup.json").write_text(json.dumps(backup, indent=2))
        manifest = backup["manifests"][0]
    (directory / "selected-backup.json").write_text(json.dumps(manifest, indent=2))
    print(
        json.dumps(
            dict(
                backup_captured_at=manifest["captured_at"], bytes=manifest["artifact"]["byte_count"]
            )
        ),
        flush=True,
    )

    def inspect_restored(database: str) -> dict[str, Any]:
        result = dict(
            rollback=rollback(database), retained_history=retained_history(database, store)
        )
        (directory / "restored-checks.json").write_text(json.dumps(result, indent=2, default=str))
        return result

    if args.resume:
        restored = json.loads((directory / "restore.json").read_bytes())
        for name in ("restored", "logical_contents_match", "temporary_database_removed"):
            assert restored[name] is True
        assert restored["recovery_details"]["rollback"]["previous_release_restored"] is True
        for _ in store.read_verified(Artifact.model_validate(manifest["artifact"])):
            pass
    else:
        restored = restore_check(
            store,
            Artifact.model_validate(manifest["artifact"]),
            manifest["logical_contents"],
            inspect_restored=inspect_restored,
        )
    (directory / "restore.json").write_text(json.dumps(restored, indent=2, default=str))
    retention_report = retention(store, directory)
    (directory / "retention.json").write_text(json.dumps(retention_report, indent=2, default=str))
    raw_path = directory / "raw-reconstruction.json"
    if args.resume and raw_path.exists():
        raw_report = json.loads(raw_path.read_bytes())
    else:
        raw_report = raw_reconstruction(directory)
        raw_path.write_text(json.dumps(raw_report, indent=2, default=str))
    raw_report["verification"] = verify_reconstruction(store, raw_report)
    report = dict(
        completed_at=datetime.now(UTC).isoformat(),
        backup=manifest,
        restore=restored,
        retention=retention_report,
        raw_reconstruction=raw_report,
        production_before=before,
        production_after=private_api_smoke(),
        invocation_seconds=round(time.monotonic() - started, 2),
        resumed=bool(args.resume),
        verification_code=code_archive(store).model_dump(mode="json"),
    )
    assert report["production_before"] == report["production_after"]
    proof = save_json(store, "evidence/maintenance/recovery", report)
    report["evidence"] = proof.model_dump(mode="json")
    (directory / "report.json").write_text(json.dumps(report, indent=2, default=str))
    _, revision = coordinator.read("last-restore.json")
    restore_summary = dict(
        status="isolated_restore_verified",
        verified_at=datetime.now(UTC).isoformat(),
        backup_captured_at=manifest["captured_at"],
        backup_sha256=manifest["artifact"]["sha256"],
        evidence=proof.model_dump(mode="json"),
    )
    coordinator.write(
        "last-restore.json",
        revision,
        restore_summary,
    )
    measurements, revision = coordinator.read("measurements.json")
    if measurements:
        coordinator.write(
            "measurements.json",
            revision,
            {
                **measurements,
                "restore_status": restore_summary["status"],
                "last_restore": restore_summary,
            },
        )
    print(
        json.dumps(
            dict(
                recovery="passed",
                report=str(directory / "report.json"),
                seconds=report["invocation_seconds"],
            )
        )
    )


if __name__ == "__main__":
    main()

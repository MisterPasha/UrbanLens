"""Replay accepted production inputs in a disposable local database; never write Neon."""

from __future__ import annotations

import argparse
import json
import os
import shutil
import subprocess
import sys
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from fastapi.testclient import TestClient
from psycopg import sql
from psycopg.conninfo import conninfo_to_dict
from pydantic import SecretStr

from scripts.database import credential, key, scratch_database, values
from scripts.dev import ROOT
from scripts.production import client
from urbanlens.api.app import create_app
from urbanlens.config import Settings
from urbanlens.database.migrations import ROLES, connect, migrate
from urbanlens.ingestion.contracts import Artifact, Provider
from urbanlens.ingestion.interfaces import ArtifactStore
from urbanlens.ingestion.registry import S3StateBackend
from urbanlens.ingestion.storage import S3ArtifactStore
from urbanlens.operations.assets import hydrate
from urbanlens.operations.control import Coordinator
from urbanlens.silver.audit import semantic_state
from urbanlens.silver.eligibility import digest
from urbanlens.silver.load import load
from urbanlens.silver.pipeline import Bundle, verified_observations


def command(module: str, *arguments: str) -> None:
    started = time.monotonic()
    with (ROOT / ".cache" / (module + "-last.log")).open("wb") as log:
        result = subprocess.run(
            [sys.executable, "-m", "scripts." + module, *arguments],
            cwd=ROOT,
            stdout=log,
            stderr=subprocess.STDOUT,
            timeout=1200,
        )
    if result.returncode:
        raise ValueError(f"Rehearsal {module} failed; inspect its private workspace log")
    print(
        json.dumps({"stage": module, "seconds": round(time.monotonic() - started, 2)}), flush=True
    )


def api_read(path: str, release: str) -> dict[str, Any]:
    settings = Settings(
        environment="test",
        api_database_url=SecretStr(credential("local", "api")),
        postcode_cache_path=ROOT / ".cache/api-postcodes.sqlite",
    )
    with TestClient(create_app(settings)) as http:
        response = http.get(path, params={"release_id": release})
        if response.status_code != 200:
            raise ValueError(f"Rehearsal API read failed: {path} ({response.status_code})")
        body = dict(response.json())
        body.pop("request_id", None)
        return body


def projection() -> dict[str, Any]:
    with connect(credential("local", "pipeline")) as connection:
        return semantic_state(connection)


def process() -> None:
    for module, args in (
        ("silver", ["load", "--storage", "s3", "--target", "local"]),
        ("classify", ["load", "--storage", "s3", "--target", "local"]),
        ("locations", ["--target", "local"]),
        ("resolve", ["run", "--target", "local", "--apply-reviews"]),
        ("companies", ["run", "--target", "local"]),
        ("history", ["project", "--target", "local"]),
    ):
        command(module, *args)


def traces(schema: str, release: str, bundle: Bundle, store: ArtifactStore) -> list[dict[str, Any]]:
    result = []
    with connect(credential("local", "migration")) as connection:
        for provider in ("fsa", "osm"):
            row = connection.execute(
                sql.SQL(
                    "SELECT e.establishment_id::text,e.display_name,"
                    "i.source_key,o.record_hash,a.uri "
                    "FROM {}.mart_establishment e JOIN ul_silver.establishment_source_link l "
                    "USING(establishment_id) JOIN ul_silver.source_identity i USING(identity_id) "
                    "JOIN ul_silver.observation_version o USING(identity_id) "
                    "JOIN ul_control.artifact a ON a.artifact_id=o.source_artifact_id "
                    "WHERE i.provider=%s AND l.valid_to IS NULL AND l.decision='accepted' "
                    "AND o.valid_to IS NULL ORDER BY e.establishment_id LIMIT 1"
                ).format(sql.Identifier(schema)),
                (provider,),
            ).fetchone()
            assert row is not None
            path = "/api/v1/establishments/" + row[0]
            body = api_read(path, release)
            result.append(
                dict(
                    provider=provider,
                    source_key=row[2],
                    record_hash=row[3],
                    input_uri=row[4],
                    model="mart_establishment",
                    entity=row[0],
                    display_name=row[1],
                    api=path,
                    response_sha256=digest(body["data"]),
                    visible_in="Explore map/list and venue details",
                )
            )
        row = connection.execute(
            sql.SQL(
                "SELECT c.company_number,c.legal_name,c.registered_office_postcode,a.uri,"
                "p.authority_code,p.area_code,pa.uri FROM {}.mart_company c "
                "JOIN ul_silver.observation_version o ON o.observation_id=c.source_observation_id "
                "JOIN ul_control.artifact a ON a.artifact_id=o.source_artifact_id "
                "JOIN ul_silver.postcode_cache p ON "
                "p.postcode=replace(c.registered_office_postcode,' ','') "
                "JOIN ul_control.artifact pa ON pa.artifact_id=p.source_artifact_id "
                "WHERE c.registered_office_cohort AND p.authority_code='E06000043' "
                "ORDER BY c.company_number LIMIT 1"
            ).format(sql.Identifier(schema))
        ).fetchone()
        assert row is not None
        path = "/api/v1/companies/" + row[0]
        body = api_read(path, release)
        result.append(
            dict(
                provider="companies-house",
                source_key=row[0],
                display_name=row[1],
                input_uri=row[3],
                model="mart_company",
                api=path,
                response_sha256=digest(body["data"]),
                visible_in="Companies registered-office list",
            )
        )
        result.append(
            dict(
                provider="postcodes-io",
                source_key=row[2],
                input_uri=row[6],
                authority_code=row[4],
                area_code=row[5],
                model="mart_company",
                company_number=row[0],
                api=path,
                contribution="Registered-office geographic enrichment and cohort inclusion",
                visible_in="Companies city cohort; postcode enrichment is an approximate location",
            )
        )
        area = connection.execute(
            sql.SQL(
                "SELECT area_code,area_name,population,a.uri,p.uri FROM {}.dim_area d "
                "JOIN ul_control.artifact a ON a.artifact_id=d.geography_artifact_id "
                "JOIN ul_control.artifact p ON p.artifact_id=d.population_artifact_id "
                "ORDER BY area_code LIMIT 1"
            ).format(sql.Identifier(schema))
        ).fetchone()
        assert area is not None
        path = "/api/v1/areas/" + area[0] + "/summary"
        body = api_read(path, release)
        ons = next(b for b in bundle.batches if b.provider == Provider.ONS)
        # Keep the joined products alongside the direct population-workbook anchor.
        observations = list(verified_observations(store, ons))
        observation = next(r for r in observations if r.source_key == area[0])
        assert observation.area is not None and observation.area.population == area[2]
        result.append(
            dict(
                provider="ons",
                source_key=area[0],
                display_name=area[1],
                population=area[2],
                geography_uri=area[3],
                population_join_anchor_uri=area[4],
                retained_joined_product_inputs=[
                    e.artifact.model_dump(mode="json") for e in observation.evidence
                ],
                model="dim_area / mart_area",
                api=path,
                response_sha256=digest(body["data"]),
                visible_in="Compare areas and population-based rates",
            )
        )
    return result


def worker(*, quality: bool = False) -> None:
    from scripts.silver import bundle_for, environment

    if not str(conninfo_to_dict(credential("local", "migration"))["dbname"]).startswith(
        "urbanlens_step19_test_"
    ):
        raise ValueError("Rehearsal worker requires its newly created disposable database")
    with connect(credential("local", "migration")) as connection:
        applied = migrate(connection)
        assert migrate(connection) == []
    if not (ROOT / ".cache/silver-s3.json").is_file():
        command("silver", "build", "--storage", "s3")
        command("classify", "build", "--storage", "s3")
    process()
    first = projection()
    command("gold", "build", "--target", "local", "--schema", "ul_candidate_rehearsal_first")
    gold = json.loads((ROOT / "data/gold/build-local.json").read_bytes())
    command("release", "prepare", "--target", "local", "--source-schema", gold["schema"])
    release = json.loads((ROOT / "data/gold/release-local-prepare.json").read_bytes())
    command("release", "publish", "--target", "local", "--release-id", release["release_id"])
    api_before = api_read("/api/v1/establishments", release["release_id"])
    quality_report = None
    if quality:
        from scripts.quality import verify

        quality_report = verify(gold, release)
        (ROOT / "data/quality.json").write_text(
            json.dumps(quality_report, indent=2, default=str), encoding="utf-8"
        )
    _, store, snapshots, _, _, _ = environment("s3")
    bundle, manifest = bundle_for(store, "s3")
    omitted = next(
        i
        for i, b in enumerate(bundle.batches)
        if b.provider == Provider.COMPANIES_HOUSE
        and b.snapshot
        and len(b.snapshot.cycle.expected_partitions) > 1
    )
    partial = bundle.model_copy(
        update={"batches": bundle.batches[:omitted] + bundle.batches[omitted + 1 :]}
    )
    try:
        with connect(credential("local", "pipeline")) as connection:
            load(connection, store, partial)
    except ValueError as error:
        if "every expected snapshot partition" not in str(error):
            raise
        interruption = str(error)
    else:
        raise ValueError("Incomplete real company cohort was incorrectly accepted")
    assert projection() == first
    assert api_read("/api/v1/establishments", release["release_id"]) == api_before
    process()
    repeated = projection()
    assert repeated == first, "Identical-input replay changed semantic state"
    command("gold", "build", "--target", "local", "--schema", "ul_candidate_rehearsal_repeat")
    second = json.loads((ROOT / "data/gold/build-local.json").read_bytes())
    assert gold["contents"] == second["contents"], "Replay changed Gold content"
    stable_inputs = gold["inputs"]
    assert stable_inputs == second["inputs"]
    report = dict(
        checked_at=datetime.now(UTC).isoformat(),
        migrations=applied,
        accepted_scopes=[
            dict(
                provider=s.cycle.provider.value,
                scope=s.cycle.scope,
                snapshot_id=str(s.snapshot_id),
                expected_parts=len(s.cycle.expected_partitions),
                accepted_parts=len(s.partitions),
                status=s.run.status.value,
                window_start=s.cycle.window_start.isoformat(),
                window_end=s.cycle.window_end.isoformat(),
            )
            for s in snapshots
        ],
        source_manifest=manifest.model_dump(mode="json"),
        first_state=first,
        repeated_state=repeated,
        gold_contents=gold["contents"],
        counts=gold["counts"],
        dbt=gold["dbt"],
        stable_release_inputs_sha256=digest(stable_inputs),
        full_release_inputs_including_code_identical=True,
        failure=dict(
            omitted_company_partition=bundle.batches[omitted].partition,
            rejection=interruption,
            semantic_state_unchanged=True,
            published_api_unchanged=True,
            source_freshness_unchanged=True,
        ),
        contributions=traces(release["schema"], release["release_id"], bundle, store),
        scope="Fresh local database with current inputs; historical editions are not reconstructed",
        upstream_provider_requests=0,
        neon_writes=0,
        quality=quality_report,
    )
    (ROOT / "data/rehearsal.json").write_text(
        json.dumps(report, indent=2, default=str), encoding="utf-8"
    )
    print(
        json.dumps({"rehearsal": "passed", "venues": gold["counts"]["mart_establishment"]}),
        flush=True,
    )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--worker", action="store_true", help=argparse.SUPPRESS)
    parser.add_argument("--quality", action="store_true", help="Include step-53 fault/audit checks")
    parser.add_argument(
        "--reuse-inputs",
        type=Path,
        help="Prior successful rehearsal directory; verify its retained inputs",
    )
    args = parser.parse_args()
    if args.worker:
        worker(quality=args.quality)
        return
    started = time.monotonic()
    destination = ROOT / "data/rehearsals" / datetime.now(UTC).strftime("%Y-%m-%d-%H%M%S")
    destination.mkdir(parents=True)
    workspace = ROOT / ".cache" / ("rehearsal-" + destination.name)
    workspace.mkdir()
    for name in ("scripts", "src", "config", "reference", "dbt"):
        shutil.copytree(
            ROOT / name,
            workspace / name,
            ignore=shutil.ignore_patterns(
                "__pycache__", "target", "logs", "dbt_packages", "profiles.yml"
            ),
        )
    for name in ("pyproject.toml", "uv.lock"):
        shutil.copyfile(ROOT / name, workspace / name)
    (workspace / ".cache").mkdir()
    (workspace / "data/silver").mkdir(parents=True)
    (workspace / "data/gold").mkdir()
    stack = client("cloudformation").describe_stacks(StackName="urbanlens-production")["Stacks"][0]
    parameters = {p["ParameterKey"]: p["ParameterValue"] for p in stack["Parameters"]}
    bucket = parameters["ArtifactsBucket"]
    store = S3ArtifactStore(client("s3"), bucket)
    engine = Coordinator(S3StateBackend(client("s3"), bucket, "operations"), None, "")
    assert not engine.read("active.json")[0].get("run_key")
    run = engine.read("last-refresh.json")[0]
    build = client("codebuild").batch_get_builds(ids=[run["build_id"]])["builds"][0]
    variables = {v["name"]: v["value"] for v in build["environment"]["environmentVariables"]}
    checkpoint_key = (
        "evidence/production/"
        + variables["URBANLENS_RUN_KEY"].removesuffix(".json")
        + "/"
        + variables["SOFTWARE_VERSION"]
        + "/publish.json"
    )
    checkpoint = store.stat(checkpoint_key)
    assert checkpoint is not None
    files = json.loads(b"".join(store.read_verified(checkpoint)))["files"]
    for filename in (".cache/bronze-state.json", ".cache/reference-check.json"):
        (workspace / filename).write_bytes(
            b"".join(store.read_verified(Artifact.model_validate(files[filename])))
        )
    config_artifact = store.stat(parameters["RuntimeConfigKey"])
    assert config_artifact is not None
    hydrate(workspace, store, json.loads(b"".join(store.read_verified(config_artifact))))
    if args.reuse_inputs:
        previous = json.loads((args.reuse_inputs / "report.json").read_bytes())
        if previous["production_build"] != build["id"] or not previous["local_database_removed"]:
            raise ValueError("Reuse requires a successful rehearsal of the same completed refresh")
        prior_workspace = ROOT / ".cache" / ("rehearsal-" + args.reuse_inputs.name)
        for filename in ("silver-s3.json", "classification-s3.json"):
            artifact = Artifact.model_validate_json(
                (prior_workspace / ".cache" / filename).read_bytes()
            )
            # Fully verify retained bundle bytes; individual record hashes are
            # checked again during load. Local state is never reused.
            b"".join(store.read_verified(artifact))
            (workspace / ".cache" / filename).write_text(artifact.model_dump_json())
    env = {k: v for k, v in os.environ.items() if not k.startswith("URBANLENS_")}
    env.update(
        AWS_PROFILE="urbanlens_dev",
        AWS_DEFAULT_REGION="eu-west-2",
        URBANLENS_CLOUD_RUNTIME="1",
        PYTHONUTF8="1",
        PYTHONPATH=os.pathsep.join((str(workspace / "src"), str(workspace))),
    )
    if (ROOT / ".cache/aws-system-ca.pem").is_file():
        env["AWS_CA_BUNDLE"] = str(ROOT / ".cache/aws-system-ca.pem")
    local = values()
    env.update({k: v for k, v in local.items() if k.startswith("LOCAL_POSTGRES_")})
    with scratch_database("local") as database:
        env["LOCAL_POSTGRES_DB"] = database
        for purpose in ROLES:
            env[key("local", purpose)] = credential("local", purpose, database=database)
        with (destination / "run.log").open("wb") as log:
            result = subprocess.run(
                [
                    sys.executable,
                    "-m",
                    "scripts.rehearsal",
                    "--worker",
                    *(["--quality"] if args.quality else []),
                ],
                cwd=workspace,
                env=env,
                stdout=log,
                stderr=subprocess.STDOUT,
                timeout=2400,
            )
        if result.returncode:
            raise ValueError("Rehearsal failed; inspect private data/rehearsals run.log")
        report = json.loads((workspace / "data/rehearsal.json").read_bytes())
    report.update(
        local_database_removed=True,
        total_seconds=round(time.monotonic() - started, 2),
        production_run=run,
        production_build=build["id"],
    )
    (destination / "report.json").write_text(json.dumps(report, indent=2), encoding="utf-8")
    print(json.dumps({"report": str(destination / "report.json"), "status": "passed"}))


if __name__ == "__main__":
    main()

"""Freeze, validate, publish, roll back and clean private Gold model sets."""

from __future__ import annotations

import argparse
import json
from datetime import UTC, datetime
from typing import Any
from uuid import UUID, uuid4

import psycopg
from psycopg import sql
from psycopg.types.json import Jsonb

from scripts.backup import code_archive
from scripts.database import credential
from scripts.gold import dbt_run, prepare_schema, verify
from scripts.silver import ROOT, environment
from urbanlens.database.migrations import connect
from urbanlens.database.releases import check_contents, cleanup, publish, schema_for
from urbanlens.silver.classification import save_json


def prepare(target: str, source: str = "ul_candidate_gold") -> dict[str, Any]:
    # The source report pins the exact validated input state, even if providers later update.
    report = json.loads((ROOT / f"data/gold/build-{target}.json").read_text(encoding="utf-8"))
    if report["schema"] != source:
        raise ValueError("Source schema differs from the validated build report")
    _, store, *_ = environment("s3")
    identifier = uuid4()
    schema = schema_for(identifier)
    origin = report["inputs"]
    intent = dict(
        version="release-manifest-v1",
        release_id=str(identifier),
        schema=schema,
        source_build=report["evidence"],
        inputs=origin,
        quality=report["dbt"],
        counts=report["counts"],
        contents=report["contents"],
        publisher_code=code_archive(store).model_dump(mode="json"),
        published_at=None,
    )
    proof = save_json(store, "evidence/releases/prepare", intent)
    with connect(credential(target, "migration")) as c:
        c.execute("SELECT pg_advisory_xact_lock(192033,0)")
        count_row = c.execute(
            "SELECT count(*) FROM ul_control.data_release "
            "WHERE model_schema IS NOT NULL AND cleaned_at IS NULL"
        ).fetchone()
        assert count_row is not None
        n = count_row[0]
        if n >= 4:
            raise ValueError("Four retained model schemas already exist; run release cleanup first")
        taxonomy = next(
            v for kind, v, _ in origin["classification_definitions"] if kind == "taxonomy"
        )
        c.execute(
            (
                "INSERT INTO"
                " ul_control.data_release(release_id,status,geography_version,popu"
                "lation_edition,population_reference_date,taxonomy_version,pipelin"
                "e_version,manifest_uri,manifest_sha256,model_schema,input_manifes"
                "t,quality_results,validated_contents) VALUES"
                " (%s,'building',%s,%s,%s,%s,'release-v1',%s,%s,%s,%s,%s,%s)"
            ),
            (
                identifier,
                origin["geography_version"],
                origin["population_edition"],
                origin["population_reference_date"],
                taxonomy,
                proof.uri,
                proof.sha256,
                schema,
                Jsonb(origin),
                Jsonb(report["dbt"]),
                Jsonb(report["contents"]),
            ),
        )
    try:
        prepare_schema(target, schema)
        with connect(credential(target, "migration")) as c:
            c.execute("SELECT pg_advisory_lock(192020,1)")
            check_contents(c, source, report["contents"])
            for table in report["counts"]:
                c.execute(
                    sql.SQL("CREATE TABLE {}.{} (LIKE {}.{} INCLUDING ALL)").format(
                        sql.Identifier(schema),
                        sql.Identifier(table),
                        sql.Identifier(source),
                        sql.Identifier(table),
                    )
                )
                c.execute(
                    sql.SQL("INSERT INTO {}.{} SELECT * FROM {}.{}").format(
                        sql.Identifier(schema),
                        sql.Identifier(table),
                        sql.Identifier(source),
                        sql.Identifier(table),
                    )
                )
                c.execute(
                    sql.SQL("UPDATE {}.{} SET release_id=%s").format(
                        sql.Identifier(schema), sql.Identifier(table)
                    ),
                    (identifier,),
                )
                c.execute(
                    sql.SQL("GRANT SELECT ON {}.{} TO urbanlens_dbt").format(
                        sql.Identifier(schema), sql.Identifier(table)
                    )
                )
            c.commit()
            variables = json.loads((ROOT / f".cache/gold-{target}.json").read_text())["variables"]
            variables["release_id"] = str(identifier)
            dbt_run(target, schema, variables, ["run-operation", "install_queries"])
            check_contents(c, schema, report["contents"])
            verified = verify(c, schema)
            if verified["contents"] != report["contents"]:
                raise ValueError("Frozen release differs from validated source")
            c.execute(
                sql.SQL("REVOKE ALL ON ALL TABLES IN SCHEMA {} FROM urbanlens_dbt").format(
                    sql.Identifier(schema)
                )
            )
            c.execute(
                sql.SQL("REVOKE ALL ON SCHEMA {} FROM urbanlens_dbt").format(sql.Identifier(schema))
            )
            c.execute(
                (
                    "UPDATE ul_control.data_release SET"
                    " status='validated',quality_passed=true WHERE release_id=%s"
                ),
                (identifier,),
            )
            for entry in origin["source_snapshots"]:
                c.execute(
                    (
                        "INSERT INTO"
                        " ul_control.release_source(release_id,scope_id,snapshot_id)"
                        " SELECT %s,scope_id,snapshot_id FROM ul_control.source_snapshot"
                        " WHERE snapshot_id=%s"
                    ),
                    (identifier, entry["snapshot_id"]),
                )
        result = dict(
            release_id=str(identifier),
            schema=schema,
            status="validated",
            manifest=proof.model_dump(mode="json"),
            counts=report["counts"],
        )
    except Exception as error:
        failed = dict(
            release_id=str(identifier),
            status="failed",
            error_type=type(error).__name__,
            intent=proof.model_dump(mode="json"),
        )
        failed_proof = save_json(store, "evidence/releases/failed", failed)
        with connect(credential(target, "migration")) as c:
            c.execute(
                (
                    "UPDATE ul_control.data_release SET"
                    " status='failed',manifest_uri=%s,manifest_sha256=%s WHERE"
                    " release_id=%s"
                ),
                (failed_proof.uri, failed_proof.sha256, identifier),
            )
        raise
    return result


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("task", choices=["prepare", "publish", "rollback", "cleanup", "status"])
    parser.add_argument("--target", choices=["local", "neon"], default="local")
    parser.add_argument("--release-id", type=UUID)
    parser.add_argument("--source-schema", default="ul_candidate_gold")
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args()
    if args.task == "prepare":
        report = prepare(args.target, args.source_schema)
    else:
        with connect(credential(args.target, "migration")) as c:
            if args.task == "cleanup":
                report = cleanup(c, apply=args.apply)
            elif args.task == "status":
                report = dict(
                    slots={
                        s: str(i)
                        for s, i in c.execute("SELECT slot,release_id FROM ul_control.release_slot")
                    }
                )
            else:
                identifier = args.release_id
                if args.task == "rollback" and identifier is None:
                    row = c.execute(
                        "SELECT release_id FROM ul_control.release_slot WHERE slot='previous'"
                    ).fetchone()
                    identifier = row[0] if row else None
                if identifier is None:
                    parser.error("Provide a validated --release-id")
                _, store, *_ = environment("s3")
                now = datetime.now(UTC)
                row = c.execute(
                    (
                        "SELECT"
                        " input_manifest,quality_results,validated_contents,manifest_uri"
                        " FROM ul_control.data_release WHERE release_id=%s"
                    ),
                    (identifier,),
                ).fetchone()
                if row is None:
                    raise ValueError("Unknown release")
                manifest = save_json(
                    store,
                    "manifests/releases",
                    dict(
                        release_id=str(identifier),
                        published_at=now.isoformat(),
                        action=args.task,
                        inputs=row[0],
                        quality=row[1],
                        contents=row[2],
                        preparation_evidence=row[3],
                    ),
                )
                report = publish(
                    c,
                    identifier,
                    rollback=args.task == "rollback",
                    published_at=now,
                    manifest=manifest.model_dump(mode="json"),
                )
    path = ROOT / f"data/gold/release-{args.target}-{args.task}.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    try:
        main()
    except (ValueError, psycopg.Error) as error:
        path = ROOT / ".cache/release-error.txt"
        path.parent.mkdir(exist_ok=True)
        path.write_text(str(error), encoding="utf-8")
        raise SystemExit(
            "Release operation failed; inspect private .cache/release-error.txt"
        ) from None

"""Export one frozen, validated public read model; never export working/raw tables."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any
from uuid import UUID

from psycopg import sql

from scripts.database import credential
from scripts.dev import ROOT
from scripts.silver import environment
from urbanlens import reuse as public_projection
from urbanlens.database.migrations import connect
from urbanlens.database.releases import check_contents, schema_for
from urbanlens.ingestion.storage import chunks
from urbanlens.reuse import archive, company, encoded, inspect, sha, venue


def build(target: str, identifier: UUID | None = None) -> dict[str, Any]:
    policy = json.loads((ROOT / "config/reuse-policy.json").read_bytes())
    tables: dict[str, list[dict[str, Any]]] = {}
    with connect(credential(target, "migration")) as c:
        c.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY")
        if identifier is None:
            selected = c.execute(
                "SELECT release_id FROM ul_control.release_slot WHERE slot='active'"
            ).fetchone()
            if not selected:
                raise ValueError("No active release")
            identifier = selected[0]
        assert identifier is not None
        c.execute("SELECT pg_advisory_xact_lock_shared(192033,hashtext(%s))", (str(identifier),))
        release = c.execute(
            "SELECT model_schema,status,quality_passed,input_manifest,validated_contents "
            "FROM ul_control.data_release WHERE release_id=%s AND cleaned_at IS NULL",
            (identifier,),
        ).fetchone()
        if (
            not release
            or release[0] != schema_for(identifier)
            or release[1] not in {"published", "validated"}
            or not release[2]
        ):
            raise ValueError("Select a frozen validated release")
        schema, _, _, inputs, contents = release
        check_contents(c, schema, contents)
        columns: dict[str, list[str]] = {}
        for table, column in c.execute(
            "SELECT table_name,column_name FROM information_schema.columns "
            "WHERE table_schema=%s ORDER BY table_name,ordinal_position",
            (schema,),
        ):
            columns.setdefault(table, []).append(column)
        if columns != policy["reviewed_columns"]:
            raise ValueError(
                "Frozen model columns changed; update the field-level publication review"
            )
        raw = {}
        for table, names in columns.items():
            expression = sql.SQL("to_jsonb(t)")
            if "geom" in names:
                expression = sql.SQL(
                    "(to_jsonb(t)-'geom')||jsonb_build_object('geometry',public.ST_AsGeoJSON(geom,15)::jsonb)"
                )
            elif "location" in names:
                expression = sql.SQL(
                    "(to_jsonb(t)-'location')||jsonb_build_object('longitude',public.ST_X(location::public.geometry),'latitude',public.ST_Y(location::public.geometry))"
                )
            elif "registered_office_location" in names:
                expression = sql.SQL("to_jsonb(t)-'registered_office_location'")
            raw[table] = [
                r[0]
                for r in c.execute(
                    sql.SQL("SELECT {} FROM {}.{} t").format(
                        expression,
                        sql.Identifier(schema),
                        sql.Identifier(table),
                    )
                )
            ]
        for name in ("mart_establishment", "mart_venue_context"):
            tables[name] = [venue(r) for r in raw[name] if r["eligibility"] != "excluded"]
        tables["mart_company"] = [company(r) for r in raw["mart_company"]]
        tables["mart_company_link"] = []
        for row in raw["mart_company_link"]:
            public = {k: v for k, v in row.items() if k not in {"reviewer", "evidence_uri"}}
            if not row["evidence_uri"].startswith("https://"):
                raise ValueError("A company relationship lacks public evidence; hold publication")
            public["public_evidence_url"] = row["evidence_uri"]
            tables["mart_company_link"].append(public)
        tables["mart_activity"] = [
            r[0]
            for r in c.execute(
                sql.SQL("""
            SELECT jsonb_build_object('release_id',release_id,'event_id',event_id,
              'entity_type',entity_type,'establishment_id',establishment_id,'company_number',company_number,
              'event_type',event_type,'observed_at',observed_at,'source_event_date',source_event_date,
              'before_value',ul_api.activity_context(before_value),
              'after_value',ul_api.activity_context(after_value),
              'evidence',jsonb_build_object('provider',evidence_fields->'provider',
                'processing_version',evidence_fields->'processing_version','record_hash',event_key,
                'retained_evidence_available',retained_evidence_uri IS NOT NULL),
              'rule_version',rule_version,'baseline_established_at',baseline_established_at,
              'interpretation',interpretation) FROM {}.mart_activity
        """).format(sql.Identifier(schema))
            )
        ]
        for name in (
            "dim_area",
            "mart_area",
            "mart_city",
            "mart_category",
            "mart_coverage",
            "mart_activity_status",
            "mart_source_health",
        ):
            tables[name] = raw[name]
            for row in tables[name]:
                row.pop("current_release_published_at", None)
                row.pop("geography_artifact_id", None)
                row.pop("population_artifact_id", None)
                if name == "mart_area":
                    row.pop("geometry", None)  # Join the one exact geometry in dim_area instead.
        tables["mart_release"] = [
            {
                **{
                    k: v
                    for k, v in raw["mart_release"][0].items()
                    if k not in {"input_manifest", "published_at", "status"}
                },
                "status": "validated",
                "input_sha256": sha(encoded(inputs)),
            }
        ]
        count = len(tables["mart_establishment"])
        assert sum(r["establishment_count"] for r in tables["mart_category"]) == count
        assert (
            next(r["establishment_count"] for r in tables["mart_city"] if r["category"] == "all")
            == count
        )
        assert (
            sum(r["establishment_count"] for r in tables["mart_area"] if r["category"] == "all")
            == count
        )
        # Compare every nonexcluded detail with its actual published API projection.
        compared = 0
        if release[1] == "published":
            for row in tables["mart_establishment"] + tables["mart_venue_context"]:
                response = c.execute(
                    "SELECT ul_api.read_release(%s,'detail',"
                    "jsonb_build_object('establishment_id',%s::text))->'data'",
                    (identifier, row["establishment_id"]),
                ).fetchone()
                assert response is not None
                expected = response[0]
                for key, value in expected.items():
                    if key == "sources":
                        continue
                    if row.get(key) != value:
                        raise ValueError("Export differs from API disclosure: " + key)
                assert (
                    [{k: s.get(k) for k in expected["sources"][0]} for s in row["sources"]]
                    == expected["sources"]
                    if expected["sources"]
                    else not row["sources"]
                )
                compared += 1
        check_contents(c, schema, contents)
    snapshots = [
        {k: v for k, v in s.items() if k != "manifest_uri"} for s in inputs["source_snapshots"]
    ]
    manifest = dict(
        version="urbanlens-reuse-v1",
        release_id=str(identifier),
        licence="ODbL-1.0",
        built_at=tables["mart_release"][0]["built_at"],
        population_reference_date=tables["mart_release"][0]["population_reference_date"],
        geography_version=tables["mart_release"][0]["geography_version"],
        input_sha256=sha(encoded(inputs)),
        source_snapshots=snapshots,
        rule_versions={
            k: inputs[k]
            for k in ("classification_definitions", "review_definitions", "versions")
            if k in inputs
        },
        policy_sha256=sha(encoded(policy)),
        exporter_sha256=sha(Path(__file__).read_bytes()),
        projection_sha256=sha(Path(public_projection.__file__).read_bytes()),
        counts={k: len(v) for k, v in tables.items()},
        frozen_model_contents=contents,
        scope=policy["scope"],
        notice=(
            "Source observations and company offices are not verified current operators, "
            "openings or closures. FSA ratings apply to the linked FSA record, "
            "not automatically to a replacement business."
        ),
    )
    inspect(manifest)
    features = [
        dict(
            type="Feature",
            geometry=r["geometry"],
            properties={k: v for k, v in r.items() if k != "geometry"},
        )
        for r in tables["dim_area"]
    ]
    extras = {
        "areas.geojson": encoded(dict(type="FeatureCollection", features=features)),
        "publication-policy.json": encoded(policy),
        "ODbL-1.0.txt": (ROOT / "reference/reuse/odbl-1.0.txt").read_bytes(),
        "OGL-3.0.html": (ROOT / "reference/reuse/ogl-3.0.html").read_bytes(),
        "NOTICES.txt": (ROOT / "reference/reuse/NOTICES.txt").read_bytes(),
    }
    body = archive(tables, manifest, extras)
    destination = ROOT / "data/reuse" / str(identifier)
    destination.mkdir(parents=True, exist_ok=True)
    name = f"urbanlens-{identifier}.zip"
    path = destination / name
    path.write_bytes(body)
    (destination / (name + ".sha256")).write_text(sha(body) + "  " + name + "\n")
    report = dict(
        release_id=str(identifier),
        path=str(path),
        sha256=sha(body),
        byte_count=len(body),
        counts=manifest["counts"],
        api_details_compared=compared,
        policy_sha256=manifest["policy_sha256"],
    )
    (destination / "verification.json").write_text(json.dumps(report, indent=2) + "\n")
    return report


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("task", choices=["build", "publish", "receipt"])
    parser.add_argument("--target", choices=["local", "neon"], default="neon")
    parser.add_argument("--release-id", type=UUID)
    args = parser.parse_args()
    _, store, *_ = environment("s3")
    if args.task == "build":
        if args.release_id:
            existing = store.stat(f"reuse/candidates/{args.release_id}/manifest.json")
            if existing:
                from scripts.publish_reuse import candidate
                from urbanlens.reuse import verify

                prepared = candidate(store, str(args.release_id))
                from urbanlens.ingestion.contracts import Artifact

                verify(b"".join(store.read_verified(Artifact.model_validate(prepared["artifact"]))))
                if prepared["policy_sha256"] != sha(
                    encoded(json.loads((ROOT / "config/reuse-policy.json").read_bytes()))
                ):
                    raise ValueError("Stored bundle needs a new publication-policy review")
                (ROOT / "data/reuse").mkdir(parents=True, exist_ok=True)
                (ROOT / "data/reuse/current.json").write_bytes(encoded(prepared))
                print(json.dumps({**prepared, "reused": True}, indent=2))
                return
        report = build(args.target, args.release_id)
        path = Path(report["path"])
        with path.open("rb") as stream:
            artifact = store.put_immutable(
                f"reuse/candidates/{report['release_id']}/{report['sha256']}.zip",
                chunks(stream),
                media_type="application/zip",
                compression="none",
            )
        body = encoded({**report, "path": None, "artifact": artifact.model_dump(mode="json")})
        store.put_immutable(
            f"reuse/candidates/{report['release_id']}/manifest.json",
            iter([body]),
            media_type="application/json",
            compression="none",
        )
    else:
        from scripts.publish_reuse import publish_package, receipt

        if args.task == "receipt":
            report = receipt(store, str(args.release_id))
        else:
            report = publish_package(store, str(args.release_id) if args.release_id else None)
    if args.task == "build":
        (ROOT / "data/reuse/current.json").write_bytes(body)
    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    main()

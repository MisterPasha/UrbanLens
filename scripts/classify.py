"""Versioned eligibility/category processing; no matching or public publication."""

from __future__ import annotations

import argparse
import json

import psycopg

from scripts.database import credential
from scripts.silver import ROOT, bundle_for, environment
from urbanlens.database.migrations import connect
from urbanlens.ingestion.contracts import Artifact
from urbanlens.silver.classification import ClassificationBundle, build, check, load
from urbanlens.silver.eligibility import Reviews, Taxonomy


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("task", choices=["build", "check", "load"])
    parser.add_argument("--storage", choices=["local", "s3"], default="local")
    parser.add_argument("--target", choices=["local", "neon"], default="local")
    args = parser.parse_args()
    _, store, _, _, _, _ = environment(args.storage)
    index = ROOT / f".cache/classification-{args.storage}.json"
    if args.task == "build":
        source, source_manifest = bundle_for(store, args.storage)
        taxonomy = Taxonomy.model_validate_json((ROOT / "config/taxonomy.json").read_bytes())
        reviews = Reviews.model_validate_json(
            (ROOT / "config/eligibility-reviews.json").read_bytes()
        )
        bundle, manifest, summary, rows = build(store, source, source_manifest, taxonomy, reviews)
        report = {**summary, **check(store, bundle)}
        index.write_text(manifest.model_dump_json(indent=2) + "\n")
        path = ROOT / f"data/silver/eligibility-review-{manifest.sha256[:12]}.json"
        path.parent.mkdir(parents=True, exist_ok=True)
        if not path.exists():
            path.write_text(
                json.dumps(
                    {
                        "classification_manifest": manifest.model_dump(mode="json"),
                        "instructions": "Optional review queue. Do not guess. Many coarse FSA "
                        "records may resolve after accepted OSM matching in "
                        "step 23. Enter decision "
                        "(eligible/excluded/unresolved), primary_category for "
                        "eligible, public_venue_confirmed, notes and evidence. "
                        "Existing answers will not be overwritten.",
                        "records": rows,
                    },
                    indent=2,
                    ensure_ascii=False,
                )
                + "\n",
                encoding="utf-8",
            )
        report["review_file"] = str(path.relative_to(ROOT))
    else:
        manifest = Artifact.model_validate_json(index.read_text())
        bundle = ClassificationBundle.model_validate_json(b"".join(store.read_verified(manifest)))
        if args.task == "check":
            report = check(store, bundle)
        else:
            if args.storage != "s3":
                raise ValueError("Database classification requires portable S3 provenance")
            with connect(credential(args.target, "pipeline")) as connection:
                report = load(connection, store, bundle)
    path = ROOT / f"data/silver/classification-{args.storage}-{args.task}-{args.target}.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    try:
        main()
    except (ValueError, psycopg.Error) as error:
        path = ROOT / ".cache/classification-error.txt"
        path.parent.mkdir(exist_ok=True)
        path.write_text(str(error))
        raise SystemExit(
            f"Classification failed ({type(error).__name__}); "
            "inspect private .cache/classification-error.txt"
        ) from None

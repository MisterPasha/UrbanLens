"""Resolve private Silver establishments; never publish a Gold release."""

from __future__ import annotations

import argparse
import hashlib
import json
from typing import Any

import psycopg

from scripts.database import credential
from scripts.identity_reviews import import_pilot
from scripts.silver import ROOT, environment
from urbanlens.database.migrations import connect
from urbanlens.ingestion.contracts import Artifact, Contract
from urbanlens.silver.classification import save_json
from urbanlens.silver.identity import Entry, Reviews, Rules
from urbanlens.silver.resolution import rotate_generation, run
from urbanlens.silver.review_mappings import Application, CorrectionBatch, DecisionBatch


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "task",
        choices=["run", "import-reviews", "rotate-generation", "review-check", "review-schema"],
    )
    parser.add_argument("--target", choices=["local", "neon"], default="local")
    parser.add_argument("--registry", help="Prior run summary supplying shared canonical UUIDs")
    parser.add_argument("--review", help="Explicit generation-rotation review JSON")
    parser.add_argument(
        "--apply-reviews",
        action="store_true",
        help="Apply the source-bound step 24 review definitions",
    )
    args = parser.parse_args()
    if args.task == "review-schema":
        schemas: dict[str, type[Contract]] = {
            "review-decisions": DecisionBatch,
            "review-corrections": CorrectionBatch,
        }
        for name, model in schemas.items():
            (ROOT / f"reference/silver/{name}.schema.json").write_text(
                json.dumps(model.model_json_schema(), indent=2) + "\n", encoding="utf-8"
            )
        print("Wrote review schemas in reference/silver")
        return
    _, store, _, _, _, _ = environment("s3")
    report: dict[str, Any]
    if args.task == "import-reviews":
        path = ROOT / "config/identity-reviews.json"
        if path.exists():
            raise ValueError("Identity reviews already exist; import will not overwrite decisions")
        reviews, report = import_pilot(store, ROOT / "data/pilots/venues/2026-11-02")
        path.write_text(reviews.model_dump_json(indent=2) + "\n", encoding="utf-8")
    elif args.task == "rotate-generation":
        if not args.review:
            raise ValueError("Generation rotation requires an explicit --review JSON file")
        review = json.loads((ROOT / args.review).read_text())
        artifact = save_json(store, "evidence/identity-generation-reviews", review)
        with connect(credential(args.target, "pipeline")) as connection:
            generation = rotate_generation(
                connection, review["source"], review["reviewer"], review["reason"], artifact
            )
        report = dict(
            new_generation=generation,
            source_replay_required=True,
            source_freshness_changed=False,
            lifecycle_event_created=False,
        )
    else:
        rules = Rules.model_validate_json((ROOT / "config/identity-rules.json").read_bytes())
        reviews = Reviews.model_validate_json((ROOT / "config/identity-reviews.json").read_bytes())
        application = None
        if args.apply_reviews or args.task == "review-check":
            application = Application(
                reviews,
                DecisionBatch.model_validate_json(
                    (ROOT / "config/venue-review-decisions.json").read_text(encoding="utf-8")
                ),
                CorrectionBatch.model_validate_json(
                    (ROOT / "config/venue-review-corrections.json").read_text(encoding="utf-8")
                ),
            )
            review_file = ROOT / application.decisions.review_file
            if (
                review_file.exists()
                and hashlib.sha256(review_file.read_bytes()).hexdigest()
                != application.decisions.review_file_sha256
            ):
                raise ValueError(
                    "Review file changed; regenerate the versioned mapping before importing"
                )
        registry = None
        registry_inputs = None
        if args.registry:
            summary = json.loads((ROOT / args.registry).read_text())
            artifact = Artifact.model_validate(summary["artifact"])
            body = json.loads(b"".join(store.read_verified(artifact)))
            registry = {
                k: e["establishment_id"] for e in body["establishments"] for k in e["members"]
            }
            registry_inputs = [Entry.model_validate(e) for e in body["inputs"]]
            if application:
                application.registry_replacements = body.get("replacements", [])
        with connect(credential(args.target, "pipeline")) as connection:
            if args.task == "review-check":
                from urbanlens.silver.resolution import inputs

                assert application is not None
                entries, rows = inputs(connection)
                application.bind(entries, rows, application.decisions.resolution_manifest)
                from urbanlens.silver.identity import resolve

                pairs, groups, _ = resolve(
                    entries,
                    rules,
                    application.bind(entries, rows, application.decisions.resolution_manifest),
                )
                print(
                    json.dumps(
                        dict(
                            source_bound_reviews=len(application.original.reviews)
                            + len(application.decisions.reviews),
                            field_corrections=len(application.corrections.corrections),
                            proposed_groups=len(groups),
                            candidate_pairs=len(pairs),
                        ),
                        indent=2,
                    )
                )
                return
            report = run(connection, store, rules, reviews, registry, registry_inputs, application)
        proof = json.loads(
            b"".join(store.read_verified(Artifact.model_validate(report["artifact"])))
        )
        suffix = "-hospitality" if application else ""
        queue = (
            ROOT / f"data/silver/identity-review-{report['artifact']['sha256'][:12]}{suffix}.json"
        )
        inactive = {k for e in proof["establishments"] for k in e.get("inactive_sources", {})}
        pending = [
            p
            for p in proof["candidates"]
            if p["decision"] == "pending"
            and (not application or (p["left"] not in inactive and p["right"] not in inactive))
        ]
        if application:
            report["pending_hospitality_pairs"] = len(pending)
        if not queue.exists():
            queue.parent.mkdir(parents=True, exist_ok=True)
            queue_entries = {Entry.model_validate(e).key: e for e in proof["inputs"]}
            queue.write_text(
                json.dumps(
                    dict(
                        resolution_manifest=report["artifact"],
                        instructions=(
                            "Step 24 queue: enter same place, different places or unsure, "
                            "with reason/evidence. Preserve source keys, generations and hashes. "
                            "Answers do not automatically approve merges and are not overwritten."
                        ),
                        pairs=[
                            dict(
                                candidate=p,
                                left_source=queue_entries[p["left"]],
                                right_source=queue_entries[p["right"]],
                                decision=None,
                                reason="",
                                evidence="",
                            )
                            for p in pending
                        ],
                        source_holds=[
                            dict(
                                establishment_id=e["establishment_id"],
                                members=e["members"],
                                reasons=e["review_reasons"],
                            )
                            for e in proof["establishments"]
                            if e["review_reasons"]
                            and (not application or e["eligibility"] != "excluded")
                        ],
                    ),
                    indent=2,
                    ensure_ascii=False,
                )
                + "\n",
                encoding="utf-8",
            )
        report["review_file"] = str(queue.relative_to(ROOT))
    path = ROOT / f"data/silver/resolution-{args.task}-{args.target}.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    try:
        main()
    except (ValueError, psycopg.Error) as error:
        (ROOT / ".cache/resolution-error.txt").write_text(str(error))
        raise SystemExit(
            f"Resolution failed ({type(error).__name__}); inspect private "
            ".cache/resolution-error.txt"
        ) from None

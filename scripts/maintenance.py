"""Private backups, bounded history cleanup and measured Silver coverage."""

from __future__ import annotations

import argparse
import json
from datetime import UTC, datetime
from typing import Any, cast
from urllib.parse import quote, unquote, urlsplit

from scripts.backup import code_archive
from scripts.backup import run as backup
from scripts.database import credential
from scripts.silver import ROOT, environment
from urbanlens.database.migrations import connect
from urbanlens.ingestion.contracts import Artifact
from urbanlens.ingestion.interfaces import ArtifactStore
from urbanlens.silver.classification import save_json
from urbanlens.silver.operations import coverage, cutoff, evidence_roots, json_uris, prune


def replay(c: Any, store: ArtifactStore, target: str) -> dict[str, Any]:
    """Replay pinned accepted inputs and the original location assessment date."""
    import hashlib

    from scripts.history import failed_attempts, query_transitions
    from scripts.silver import bundle_for
    from urbanlens.silver.audit import semantic_state
    from urbanlens.silver.classification import ClassificationBundle
    from urbanlens.silver.classification import load as classify
    from urbanlens.silver.companies import Register
    from urbanlens.silver.companies import run as companies
    from urbanlens.silver.identity import Reviews, Rules
    from urbanlens.silver.load import load
    from urbanlens.silver.location_processing import run as locate
    from urbanlens.silver.resolution import run as resolve
    from urbanlens.silver.review_mappings import Application, CorrectionBatch, DecisionBatch

    before = semantic_state(c)
    source, source_manifest = bundle_for(store, "s3")
    snapshots = list(
        {b.snapshot.snapshot_id: b.snapshot for b in source.batches if b.snapshot}.values()
    )
    loaded = load(
        c,
        store,
        source,
        interruptions=failed_attempts(store, snapshots),
        compatible_queries=query_transitions(),
    )
    classification_manifest = Artifact.model_validate_json(
        (ROOT / ".cache/classification-s3.json").read_text(encoding="utf-8")
    )
    classification = ClassificationBundle.model_validate_json(
        b"".join(store.read_verified(classification_manifest))
    )
    classified = classify(c, store, classification)
    parameters = json.loads(
        (ROOT / f"data/silver/locations-{target}.json").read_text(encoding="utf-8")
    )
    located = locate(c, store, datetime.fromisoformat(parameters["assessed_as_of"]), [])
    rules = Rules.model_validate_json(
        (ROOT / "config/identity-rules.json").read_text(encoding="utf-8")
    )
    original = Reviews.model_validate_json(
        (ROOT / "config/identity-reviews.json").read_text(encoding="utf-8")
    )
    decisions = DecisionBatch.model_validate_json(
        (ROOT / "config/venue-review-decisions.json").read_text(encoding="utf-8")
    )
    corrections = CorrectionBatch.model_validate_json(
        (ROOT / "config/venue-review-corrections.json").read_text(encoding="utf-8")
    )
    review_file = ROOT / decisions.review_file
    if (
        review_file.exists()
        and hashlib.sha256(review_file.read_bytes()).hexdigest() != decisions.review_file_sha256
    ):
        raise ValueError("Editable review answers changed after the frozen mapping")
    resolved = resolve(
        c, store, rules, original, application=Application(original, decisions, corrections)
    )
    legal = companies(
        c,
        store,
        Register.model_validate_json(
            (ROOT / "config/company-links.json").read_text(encoding="utf-8")
        ),
    )
    after = semantic_state(c)
    if before != after:
        changed = [k for k in before["parts"] if before["parts"][k] != after["parts"][k]]
        raise ValueError("Replay changed semantic state: " + ", ".join(changed))
    return dict(
        before=before,
        after=after,
        identical=True,
        source_manifest=source_manifest.model_dump(mode="json"),
        classification_manifest=classification_manifest.model_dump(mode="json"),
        location_assessment_date=parameters["assessed_as_of"],
        source_load=loaded,
        classification=classified,
        location_changed=located["inserted_or_changed"],
        identity_changed=resolved["inserted_or_changed_establishments"],
        new_field_provenance=resolved.get("new_field_provenance", 0),
        company_changed=legal["changed_companies"],
        history_events=resolved["history_events"],
        provider_requests=0,
    )


def lifecycle() -> dict[str, Any]:
    rules = [
        dict(ID="Raw90Days", Status="Enabled", Prefix="bronze/", Expiration=dict(Days=90)),
        dict(
            ID="AbortIncompleteUploads",
            Status="Enabled",
            AbortIncompleteMultipartUpload=dict(DaysAfterInitiation=1),
        ),
    ]
    for target in ("local", "neon"):
        for tier, days in (("daily", 7), ("weekly", 28)):
            rules.append(
                dict(
                    ID=target.title() + tier.title() + "Backups" + str(days) + "Days",
                    Status="Enabled",
                    Prefix=f"backups/{target}/{tier}/",
                    Expiration=dict(Days=days),
                )
            )
    return dict(Rules=rules)


def garbage(c: Any, store: ArtifactStore, *, apply: bool) -> dict[str, Any]:
    """Expire unreferenced large Silver objects and expired event evidence only."""
    remote = cast(Any, store)
    protected = evidence_roots(c)
    pending = list(protected)
    visited = set()
    # Walk small pinned proof/manifests, not obsolete raw payloads or entire national files.
    while pending:
        uri = pending.pop()
        if uri in visited:
            continue
        visited.add(uri)
        parsed = urlsplit(uri)
        key = unquote(parsed.path.lstrip("/"))
        if parsed.netloc != remote.bucket or key.startswith("bronze/"):
            continue
        if not key.endswith(".json"):
            continue
        artifact = store.stat(key)
        if artifact is None:
            raise ValueError("Referenced evidence is missing; cleanup stopped")
        if artifact.byte_count > 16 * 1024 * 1024:
            raise ValueError("Referenced manifest exceeds cleanup scan budget")
        refs = json_uris(json.loads(b"".join(store.read_verified(artifact))))
        for ref in refs - protected:
            protected.add(ref)
            pending.append(ref)
        if len(visited) > 10000:
            raise ValueError("Evidence traversal exceeds cleanup budget")
    boundary = cutoff(datetime.now(UTC))
    candidates = []
    objects = 0
    for prefix in ("silver/", "evidence/history/"):
        for page in remote.client.get_paginator("list_objects_v2").paginate(
            Bucket=remote.bucket, Prefix=remote.prefix + prefix
        ):
            for item in page.get("Contents", []):
                objects += 1
                if objects > 25000:
                    raise ValueError("Object scan exceeds cleanup budget")
                uri = "s3://" + remote.bucket + "/" + quote(item["Key"], safe="/-_.~")
                if item["LastModified"] < boundary and uri not in protected:
                    candidates.append(dict(key=item["Key"], bytes=item["Size"]))
    if apply:
        for item in candidates:
            remote.client.delete_object(Bucket=remote.bucket, Key=item["key"])
    return dict(
        applied=apply,
        unreferenced_old_objects=candidates,
        protected_uris=len(protected),
        scanned_objects=objects,
        cutoff=boundary.isoformat(),
    )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "task", choices=["coverage", "backup", "retention", "lifecycle", "audit", "replay"]
    )
    parser.add_argument("--target", choices=["local", "neon"], default="local")
    parser.add_argument("--tier", choices=["daily", "weekly", "both"], default="both")
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args()
    _, store, _, _, _, _ = environment("s3")
    if args.task == "backup":
        report = backup(args.target, store, tier=args.tier)
    elif args.task == "lifecycle":
        remote = cast(Any, store)
        desired = lifecycle()
        actual = remote.client.get_bucket_lifecycle_configuration(Bucket=remote.bucket)["Rules"]

        def signature(rule: dict[str, Any]) -> tuple[Any, ...]:
            return (
                rule.get("Prefix", rule.get("Filter", {}).get("Prefix", "")),
                rule["Status"],
                rule.get("Expiration", {}).get("Days"),
                rule.get("AbortIncompleteMultipartUpload", {}).get("DaysAfterInitiation"),
            )

        compliant = {signature(r) for r in desired["Rules"]} <= {signature(r) for r in actual}
        if args.apply and not compliant:
            managed = {r.get("Prefix", "") for r in desired["Rules"]}
            desired["Rules"].extend(r for r in actual if signature(r)[0] not in managed)
            remote.client.put_bucket_lifecycle_configuration(
                Bucket=remote.bucket, LifecycleConfiguration=desired
            )
        actual = remote.client.get_bucket_lifecycle_configuration(Bucket=remote.bucket)["Rules"]
        compliant = {signature(r) for r in desired["Rules"]} <= {signature(r) for r in actual}
        report = dict(desired=desired, actual=actual, applied=args.apply, compliant=compliant)
    else:
        with connect(credential(args.target, "migration")) as c:
            c.execute("SELECT pg_advisory_xact_lock(192020,1)")
            if args.task == "replay":
                report = replay(c, store, args.target)
            elif args.task == "audit":
                from urbanlens.silver.audit import inspect, semantic_state

                report = dict(state=semantic_state(c), audit=inspect(c))
            elif args.task == "coverage":
                report = coverage(c)
            else:
                report = dict(
                    database=prune(c, apply=args.apply), objects=garbage(c, store, apply=args.apply)
                )
            report["code"] = code_archive(store).model_dump(mode="json")
            proof = save_json(store, "evidence/maintenance/" + args.task, report)
            report["evidence"] = proof.model_dump(mode="json")
    path = ROOT / f"data/silver/{args.task}-{args.target}.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    summary = {
        k: v for k, v in report.items() if k not in {"sources", "measures", "manifests", "audit"}
    }
    if "audit" in report:
        summary["audit"] = {
            k: v for k, v in report["audit"].items() if k not in {"samples", "plausible_nonmatches"}
        }
    if "manifests" in report:
        summary["backups"] = [
            dict(
                artifact=m["artifact"],
                captured_at=m["captured_at"],
                contents_sha256=m["logical_contents"]["sha256"],
            )
            for m in report["manifests"]
        ]
    print(json.dumps(summary, indent=2))
    if args.task == "audit" and not report["audit"]["passed"]:
        raise SystemExit("Audit found inconsistencies; see the saved report")


if __name__ == "__main__":
    main()

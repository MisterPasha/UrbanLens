"""Portable classification artifacts, bounded current decisions and coverage counts."""

from __future__ import annotations

import hashlib
import tempfile
from collections import Counter
from collections.abc import Iterator
from typing import Any, Literal

import psycopg
from psycopg.types.json import Jsonb

from urbanlens.ingestion.contracts import Artifact, Contract, Provider
from urbanlens.ingestion.interfaces import ArtifactStore
from urbanlens.ingestion.storage import chunks, gzip_chunks
from urbanlens.silver.eligibility import Decision, Reviews, Taxonomy, classify, digest
from urbanlens.silver.load import artifact_id
from urbanlens.silver.pipeline import Batch, Bundle, json_line, read_records, verified_observations


class DecisionBatch(Contract):
    provider: Provider
    scope: str
    partition: str
    input_records: Artifact
    records: Artifact
    count: int
    decoded_sha256: str


class ClassificationBundle(Contract):
    version: Literal["classification-bundle-v1"] = "classification-bundle-v1"
    engine_version: Literal["eligibility-engine-v1"] = "eligibility-engine-v1"
    source_manifest: Artifact
    taxonomy: Artifact
    reviews: Artifact
    batches: tuple[DecisionBatch, ...]


def save_json(store: ArtifactStore, prefix: str, value: Any) -> Artifact:
    body = json_line(value)
    key = prefix + "/" + hashlib.sha256(body).hexdigest() + ".json"
    return store.put_immutable(key, iter([body]), media_type="application/json", compression="none")


def definitions(store: ArtifactStore, bundle: ClassificationBundle) -> tuple[Taxonomy, Reviews]:
    taxonomy = Taxonomy.model_validate_json(b"".join(store.read_verified(bundle.taxonomy)))
    reviews = Reviews.model_validate_json(b"".join(store.read_verified(bundle.reviews)))
    return taxonomy, reviews


def build(
    store: ArtifactStore,
    source: Bundle,
    source_manifest: Artifact,
    taxonomy: Taxonomy,
    reviews: Reviews,
) -> tuple[ClassificationBundle, Artifact, dict[str, Any], list[dict[str, Any]]]:
    batches = []
    counts: Counter[str] = Counter()
    category_counts: Counter[str] = Counter()
    reasons: Counter[str] = Counter()
    review_rows = []
    seen: set[tuple[str, str, str]] = set()
    for batch in source.batches:
        count, checksum = 0, hashlib.sha256()
        with tempfile.TemporaryFile() as stream:

            def lines(input_batch: Batch = batch, output_hash: Any = checksum) -> Iterator[bytes]:
                nonlocal count
                for record in verified_observations(store, input_batch):
                    key = (record.provider.value, record.scope, record.source_key)
                    if key in seen:
                        raise ValueError("Duplicate classification input identity")
                    seen.add(key)
                    decision = classify(record, taxonomy, reviews)
                    selection = decision.selection
                    counts["all_observations"] += 1
                    if decision.applicable:
                        counts["venue_observations"] += 1
                        counts[selection.eligibility] += 1
                        counts["review_required"] += int(selection.review_required)
                        if selection.eligibility == "eligible":
                            assert selection.primary_category is not None
                            category_counts[selection.primary_category] += 1
                            counts["fine_category"] += int(selection.category_state == "specific")
                        counts["review_applied"] += int(decision.review_status == "applied")
                        counts["stale_review"] += int(decision.review_status == "stale")
                        reasons.update(selection.reasons)
                    else:
                        counts["not_applicable"] += 1
                    if selection.review_required:
                        review_rows.append(
                            dict(
                                review_id="E-" + digest(key)[:12],
                                provider=record.provider.value,
                                scope=record.scope,
                                source_key=record.source_key,
                                expected_record_hash=record.record_hash,
                                name=record.name.value,
                                address=record.address.value,
                                postcode=record.postcode.value,
                                source_tags=decision.secondary_tags,
                                automatic_selection=selection.model_dump(mode="json"),
                                decision=None,
                                primary_category=None,
                                public_venue_confirmed=False,
                                notes="",
                                evidence="",
                            )
                        )
                    line = json_line(decision.model_dump(mode="json"))
                    count += 1
                    output_hash.update(line)
                    yield line

            for block in gzip_chunks(lines()):
                stream.write(block)
            stream.seek(0)
            key = (
                "silver/classified/eligibility-engine-v1/"
                + digest(taxonomy.model_dump(mode="json"))
                + "/"
                + digest(reviews.model_dump(mode="json"))
                + "/"
                + checksum.hexdigest()
                + ".jsonl.gz"
            )
            artifact = store.put_immutable(
                key, chunks(stream), media_type="application/x-ndjson", compression="gzip"
            )
        batches.append(
            DecisionBatch(
                provider=batch.provider,
                scope=batch.scope,
                partition=batch.partition,
                input_records=batch.records,
                records=artifact,
                count=count,
                decoded_sha256=checksum.hexdigest(),
            )
        )
    result = ClassificationBundle(
        source_manifest=source_manifest,
        taxonomy=save_json(store, "evidence/taxonomy", taxonomy.model_dump(mode="json")),
        reviews=save_json(store, "evidence/eligibility-reviews", reviews.model_dump(mode="json")),
        batches=tuple(batches),
    )
    manifest = save_json(store, "manifests/classification", result.model_dump(mode="json"))
    denominator = counts["eligible"]
    summary = dict(
        taxonomy_version=taxonomy.version,
        reviews_version=reviews.version,
        counts=dict(counts),
        primary_categories=dict(category_counts),
        reasons=dict(reasons),
        fine_category_coverage=dict(
            numerator=counts["fine_category"],
            denominator=denominator,
            fraction=counts["fine_category"] / denominator if denominator else None,
            definition="Fine-category eligible venue observations / all eligible venue "
            "observations, including broad and ambiguous. Source-level only; "
            "duplicates not yet resolved.",
        ),
        eligibility_resolution=dict(
            numerator=counts["eligible"] + counts["excluded"],
            denominator=counts["venue_observations"],
            definition=(
                "Eligible or excluded venue observations / all venue observations; "
                "unresolved remains separate."
            ),
        ),
        fine_category_among_all_candidates=dict(
            numerator=counts["fine_category"],
            denominator=counts["venue_observations"],
            definition=(
                "Fine-category eligible observations / all venue observations, "
                "including unresolved and excluded. Not unique establishments."
            ),
        ),
        primary_competitor_counts_ready=False,
        canonical_entities_or_releases_created=False,
    )
    return result, manifest, summary, review_rows


def verified_decisions(store: ArtifactStore, batch: DecisionBatch) -> Iterator[Decision]:
    count, checksum = 0, hashlib.sha256()
    for value in read_records(store, batch.records):
        record = Decision.model_validate(value)
        if record.provider != batch.provider or record.scope != batch.scope:
            raise ValueError("Classification source scope mismatch")
        count += 1
        checksum.update(json_line(value))
        yield record
    if count != batch.count or checksum.hexdigest() != batch.decoded_sha256:
        raise ValueError("Classification count/hash mismatch")


def check(store: ArtifactStore, bundle: ClassificationBundle) -> dict[str, Any]:
    taxonomy, reviews = definitions(store, bundle)
    source = Bundle.model_validate_json(b"".join(store.read_verified(bundle.source_manifest)))
    sources = {(b.provider, b.scope, b.partition): b for b in source.batches}
    seen: set[tuple[Provider, str, str]] = set()
    for batch in bundle.batches:
        source_batch = sources.pop((batch.provider, batch.scope, batch.partition))
        if source_batch.records != batch.input_records:
            raise ValueError("Classification input differs from pinned source manifest")
        records = {r.source_key: r for r in verified_observations(store, source_batch)}
        for decision in verified_decisions(store, batch):
            original = records.pop(decision.source_key)
            key = (decision.provider, decision.scope, decision.source_key)
            if key in seen or classify(original, taxonomy, reviews) != decision:
                raise ValueError("Classification is duplicated or not reproducible")
            seen.add(key)
        if records:
            raise ValueError("Classification omitted source records")
    if sources:
        raise ValueError("Classification omitted source partitions")
    return {"verified_decisions": len(seen), "all_recomputed_from_pinned_sources": True}


def load(
    connection: psycopg.Connection[Any], store: ArtifactStore, bundle: ClassificationBundle
) -> dict[str, Any]:
    report = check(store, bundle)
    taxonomy, reviews = definitions(store, bundle)
    loaded = 0
    with connection.transaction():
        # Same lock as source loading: a source change cannot race classification.
        connection.execute("SELECT pg_advisory_xact_lock(192020,1)")
        for kind, definition, artifact in (
            ("taxonomy", taxonomy, bundle.taxonomy),
            ("reviews", reviews, bundle.reviews),
        ):
            aid = artifact_id(connection, artifact)
            sha = digest(definition.model_dump(mode="json"))
            connection.execute(
                "INSERT INTO ul_control.classification_definition VALUES (%s,%s,%s,%s) "
                "ON CONFLICT(kind,version) DO NOTHING",
                (kind, definition.version, sha, aid),
            )
            if connection.execute(
                "SELECT sha256 FROM ul_control.classification_definition WHERE kind=%s AND "
                "version=%s",
                (kind, definition.version),
            ).fetchone() != (sha,):
                raise ValueError("Changed classification definition requires a new version")
        for batch in bundle.batches:
            if batch.provider == Provider.POSTCODES:
                continue
            decision_artifact = artifact_id(connection, batch.records)
            current = {
                r[0]: (r[1], r[2])
                for r in connection.execute(
                    "SELECT i.source_key,o.observation_id,o.record_hash FROM "
                    "ul_silver.observation_version o "
                    "JOIN ul_silver.source_identity i USING(identity_id) JOIN "
                    "ul_control.source_scope s USING(scope_id) "
                    "WHERE i.provider=%s AND s.scope_key=%s AND o.valid_to IS NULL",
                    (batch.provider.value, batch.scope),
                )
            }
            rows = []
            for decision in verified_decisions(store, batch):
                actual = current.get(decision.source_key)
                if actual is None or actual[1] != decision.record_hash:
                    raise ValueError("Classification requires its exact current source observation")
                selection = decision.selection
                body = decision.model_dump(mode="json")
                if len(json_line(body)) > 60000:
                    raise ValueError("Classification working state exceeds record budget")
                rows.append(
                    (
                        actual[0],
                        decision.record_hash,
                        decision.taxonomy_version,
                        decision.taxonomy_sha256,
                        decision.reviews_version,
                        decision.reviews_sha256,
                        decision.engine_version,
                        decision.decision_hash,
                        decision.applicable,
                        selection.eligibility,
                        selection.primary_category,
                        selection.category_state,
                        list(selection.candidate_categories),
                        selection.review_required,
                        decision.review_status,
                        Jsonb(body),
                        decision_artifact,
                    )
                )
            with connection.cursor() as cursor:
                cursor.executemany(
                    "INSERT INTO ul_silver.observation_classification(observation_id,"
                    "record_hash,taxonomy_version,taxonomy_sha256,"
                    "reviews_version,reviews_sha256,engine_version,decision_hash,applicable,"
                    "eligibility,primary_category,category_state,"
                    "candidate_categories,review_required,review_status,decision,artifact_id) "
                    "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) "
                    "ON CONFLICT(observation_id) DO UPDATE SET "
                    "taxonomy_version=excluded.taxonomy_version,"
                    "taxonomy_sha256=excluded.taxonomy_sha256,"
                    "reviews_version=excluded.reviews_version,"
                    "reviews_sha256=excluded.reviews_sha256,"
                    "engine_version=excluded.engine_version,"
                    "decision_hash=excluded.decision_hash,applicable=excluded.applicable,"
                    "eligibility=excluded.eligibility,"
                    "primary_category=excluded.primary_category,"
                    "category_state=excluded.category_state,"
                    "candidate_categories=excluded.candidate_categories,"
                    "review_required=excluded.review_required,"
                    "review_status=excluded.review_status,decision=excluded.decision,"
                    "artifact_id=excluded.artifact_id,processed_at=clock_timestamp() "
                    "WHERE "
                    "ul_silver.observation_classification.decision_hash<>excluded.decision_hash",
                    rows,
                )
                loaded += cursor.rowcount
    return {
        **report,
        "inserted_or_changed": loaded,
        "canonical_entities_or_releases_created": False,
    }

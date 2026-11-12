"""Build immutable, portable Silver JSONL from accepted retained observations."""

from __future__ import annotations

import hashlib
import json
import tempfile
import xml.etree.ElementTree as ET
from collections.abc import Callable, Iterator
from datetime import datetime
from typing import Any, Literal, Protocol

from urbanlens.ingestion.companies import csv_rows
from urbanlens.ingestion.contracts import Artifact, Contract, Partition, Provider, Snapshot, Stage
from urbanlens.ingestion.engine import load_model, metadata_key
from urbanlens.ingestion.interfaces import ArtifactStore
from urbanlens.ingestion.payload import payload
from urbanlens.ingestion.postcodes import CacheEntry, Lookup, validate_result
from urbanlens.ingestion.storage import ByteReader, chunks, gzip_chunks
from urbanlens.silver import normalise
from urbanlens.silver.models import VERSION, Evidence, Observation


class Batch(Contract):
    provider: Provider
    scope: str
    partition: str
    snapshot: Snapshot | None
    source_manifest: Artifact
    records: Artifact
    count: int
    decoded_sha256: str


class Bundle(Contract):
    version: Literal["silver-bundle-v1"] = "silver-bundle-v1"
    normalisation_version: Literal["silver-normalisation-v1"] = VERSION
    batches: tuple[Batch, ...]


def json_line(value: Any) -> bytes:
    return (
        json.dumps(
            value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False
        )
        + "\n"
    ).encode()


def read_records(store: ArtifactStore, artifact: Artifact) -> Iterator[dict[str, Any]]:
    with payload(store, artifact, max_bytes=128 * 1024 * 1024) as stream:
        for line in stream:
            value = json.loads(line)
            if not isinstance(value, dict):
                raise ValueError("Expected JSONL object")
            yield value


def input_rows(
    snapshot: Snapshot, part: Partition, store: ArtifactStore
) -> tuple[Artifact, list[dict[str, Any]]]:
    artifact = load_model(
        store,
        metadata_key(snapshot.cycle, f"attempts/{snapshot.run.run_id}/{part.partition_id}-silver"),
        Artifact,
    )
    if artifact is None:
        raise ValueError("Accepted parsed artifact is missing")
    rows = list(read_records(store, artifact))
    if len(rows) != part.counts.accepted:
        raise ValueError("Accepted row count disagrees with source manifest")
    return artifact, rows


def source_observations(
    snapshot: Snapshot,
    part: Partition,
    store: ArtifactStore,
    resolve: Callable[[Artifact], Artifact],
) -> Iterator[Observation]:
    if (
        snapshot.run.status != Stage.ACCEPTED
        or snapshot.completeness != "complete"
        or not snapshot.validation.passed
    ):
        raise ValueError("Normalisation requires an accepted complete collection")
    if part.fetched_at is None:
        raise ValueError("Original observation time is missing")
    parsed, rows = input_rows(snapshot, part, store)
    context: dict[str, Any] = dict(
        scope=snapshot.cycle.scope,
        snapshot_id=snapshot.snapshot_id,
        cycle_id=snapshot.cycle.cycle_id,
        observed_at=part.fetched_at,
        ingested_at=snapshot.run.started_at,
        source_extracted_at=part.source_extracted_at,
        source_extract_date=part.source_extract_date,
    )
    provider = snapshot.cycle.provider

    def evidence(
        locator: str, row_locator: str, artifacts: tuple[Artifact, ...] | None = None
    ) -> tuple[Evidence, ...]:
        return tuple(
            Evidence(artifact=resolve(a), locator=locator)
            for a in (artifacts or (part.artifacts[0],))
        ) + (Evidence(artifact=resolve(parsed), locator=row_locator),)

    if provider == Provider.FSA:
        wanted = {r["fhrs_id"]: i for i, r in enumerate(rows, 1)}
        found: set[str] = set()
        with payload(store, part.artifacts[0]) as stream:
            ordinal = 0
            for _, element in ET.iterparse(stream, events=("end",)):
                if element.tag != "EstablishmentDetail":
                    continue
                ordinal += 1
                number = (element.findtext("FHRSID") or "").strip()
                if number in wanted:
                    if number in found:
                        raise ValueError("Duplicate FSA raw identity")
                    found.add(number)
                    raw: dict[str, Any] = {c.tag: c.text for c in element}
                    raw.update(
                        {
                            "Longitude": element.findtext("Geocode/Longitude"),
                            "Latitude": element.findtext("Geocode/Latitude"),
                            "Scores": {c.tag: c.text for c in element.findall("Scores/*")},
                        }
                    )
                    yield normalise.fsa(
                        raw,
                        {
                            **context,
                            "evidence": evidence(
                                f"EstablishmentCollection/EstablishmentDetail[{ordinal}]",
                                f"line/{wanted[number]}",
                            ),
                        },
                    )
                element.clear()
        if found != set(wanted):
            raise ValueError("Accepted FSA record was not found in raw evidence")
    elif provider == Provider.OSM:
        for i, row in enumerate(rows, 1):
            event = row.get("source_timestamp")
            yield normalise.osm(
                row,
                {
                    **context,
                    "source_event_at": None
                    if event is None
                    else datetime.fromisoformat(event.replace("Z", "+00:00")),
                    "evidence": evidence(
                        f"elements[type={row['element_type']},id={row['osm_id']}]", f"line/{i}"
                    ),
                },
            )
    elif provider == Provider.COMPANIES_HOUSE:
        if len(part.artifacts) == 1:
            with payload(store, part.artifacts[0]) as stream:
                raw = json.load(stream)
            yield normalise.company(
                raw, {**context, "evidence": evidence("$", "line/1")}, profile=True
            )
        else:
            wanted_rows = {r["source_record"]: (i, r) for i, r in enumerate(rows, 1)}
            found_ordinals: set[int] = set()
            with payload(store, part.artifacts[0], max_bytes=128 * 1024 * 1024) as stream:
                for ordinal, raw in enumerate(csv_rows(stream), 1):
                    if ordinal not in wanted_rows:
                        continue
                    i, accepted = wanted_rows[ordinal]
                    if raw["CompanyNumber"].strip() != accepted["company_number"]:
                        raise ValueError("Raw company row and accepted identity disagree")
                    found_ordinals.add(ordinal)
                    raw_company: dict[str, Any] = dict(raw)
                    raw_company["_cohort"] = accepted["cohort_reason"] == "registered-office-cohort"
                    yield normalise.company(
                        raw_company,
                        {
                            **context,
                            "evidence": evidence(
                                f"zip/single-csv/record/{ordinal} (header excluded)", f"line/{i}"
                            ),
                        },
                        profile=False,
                    )
            if found_ordinals != set(wanted_rows):
                raise ValueError("Accepted company record was not found in raw evidence")
    elif provider == Provider.ONS:
        for i, row in enumerate(rows, 1):
            yield normalise.area(
                row,
                {
                    **context,
                    "evidence": evidence(
                        f"LSOA21CD={row['lsoa21cd']} (joined products; see field_sources)",
                        f"line/{i}",
                        part.artifacts,
                    ),
                },
            )
    else:
        raise ValueError("Postcodes use dated cache evidence, not a fabricated source snapshot")


def cache_observations(
    entries: list[CacheEntry], area_codes: set[str], resolve: Callable[[Artifact], Artifact]
) -> Iterator[Observation]:
    for entry in sorted(entries, key=lambda e: e.postcode):
        validate_result(entry.result, entry.postcode)
        result = entry.result or {}
        codes = result.get("codes", {})
        authority, area = codes.get("admin_district"), codes.get("lsoa21")
        state = (
            "unmatched"
            if entry.result is None
            else "outside"
            if authority and authority != "E06000043"
            else "resolved"
            if authority == "E06000043" and area in area_codes
            else "incompatible-geography"
        )
        lookup = Lookup.model_validate(
            dict(
                raw_input=result.get("postcode", entry.postcode),
                postcode=entry.postcode,
                state=state,
                fetched_at=entry.fetched_at,
                expires_at=entry.expires_at,
                artifact=entry.artifact,
                longitude=result.get("longitude"),
                latitude=result.get("latitude"),
                authority=authority,
                lsoa21=area,
                quality=result.get("quality"),
                source_edition=result.get("dataset_version"),
            )
        )
        yield normalise.postcode_lookup(
            lookup,
            dict(
                scope="dated-postcode-cache",
                snapshot_id=None,
                cycle_id=None,
                observed_at=entry.fetched_at,
                evidence=(
                    Evidence(
                        artifact=resolve(entry.artifact), locator=f"result[query={entry.postcode}]"
                    ),
                ),
            ),
        )


class SeekableReader(ByteReader, Protocol):
    def seek(self, offset: int) -> int: ...


def retained_gzip(
    store: ArtifactStore, key: str, stream: SeekableReader, decoded_sha256: str
) -> Artifact:
    """Reuse verified content despite platform-dependent gzip headers or encoders."""
    existing = store.stat(key)
    if existing:
        if existing.compression != "gzip":
            raise ValueError("Expected a retained gzip artifact")
        checksum = hashlib.sha256()
        with payload(store, existing, max_bytes=128 * 1024 * 1024) as retained:
            for block in chunks(retained):
                checksum.update(block)
        if checksum.hexdigest() != decoded_sha256:
            raise ValueError("Retained compressed artifact has different decoded content")
        return existing.model_copy(update={"media_type": "application/x-ndjson"})
    stream.seek(0)
    return store.put_immutable(
        key, chunks(stream), media_type="application/x-ndjson", compression="gzip"
    )


def write_batch(
    output: ArtifactStore, key: str, records: Iterator[Observation], **metadata: Any
) -> Batch:
    count, digest, identities = 0, hashlib.sha256(), set()

    def lines() -> Iterator[bytes]:
        nonlocal count
        for record in records:
            if record.source_key in identities:
                raise ValueError("Duplicate identity within normalised partition")
            identities.add(record.source_key)
            count += 1
            line = json_line(record.model_dump(mode="json"))
            digest.update(line)
            yield line

    with tempfile.TemporaryFile() as stream:
        for block in gzip_chunks(lines()):
            stream.write(block)
        stream.seek(0)
        addressed = key.removesuffix(".jsonl.gz") + "/" + digest.hexdigest() + ".jsonl.gz"
        artifact = retained_gzip(output, addressed, stream, digest.hexdigest())
    return Batch(**metadata, records=artifact, count=count, decoded_sha256=digest.hexdigest())


def build(
    snapshots: list[Snapshot],
    source: ArtifactStore,
    output: ArtifactStore,
    *,
    resolve: Callable[[Artifact], Artifact] = lambda a: a,
    entries: list[CacheEntry] | None = None,
    postcode_manifest: Artifact | None = None,
    area_codes: set[str] | None = None,
    log: Callable[[str], None] = lambda _: None,
) -> tuple[Bundle, Artifact]:
    batches = []
    streams: set[tuple[Provider, str]] = set()
    for snapshot in sorted(snapshots, key=lambda s: (s.cycle.provider.value, s.cycle.scope)):
        stream = (snapshot.cycle.provider, snapshot.cycle.scope)
        if stream in streams:
            raise ValueError("Select one accepted snapshot per source scope")
        streams.add(stream)
        manifest = source.stat(metadata_key(snapshot.cycle, "accepted"))
        if manifest is None:
            raise ValueError("Accepted manifest is missing")
        retained = Snapshot.model_validate_json(b"".join(source.read_verified(manifest)))
        if retained != snapshot:
            raise ValueError("Selected snapshot differs from retained accepted manifest")
        for part in snapshot.partitions:
            batch = write_batch(
                output,
                f"silver/normalised/{VERSION}/{snapshot.snapshot_id}/{part.partition_id}.jsonl.gz",
                source_observations(snapshot, part, source, resolve),
                provider=snapshot.cycle.provider,
                scope=snapshot.cycle.scope,
                partition=part.partition_id,
                snapshot=snapshot,
                source_manifest=resolve(manifest),
            )
            if batch.count != part.counts.accepted:
                raise ValueError("Normalisation count differs from accepted source rows")
            batches.append(batch)
            log(f"{batch.provider.value}/{batch.partition}: {batch.count} observations")
    if entries is not None:
        if postcode_manifest is None or area_codes is None:
            raise ValueError("Postcode export and target areas are required")
        key_digest = hashlib.sha256(
            json_line(
                [e.model_dump(mode="json") for e in sorted(entries, key=lambda e: e.postcode)]
            )
        ).hexdigest()
        batches.append(
            write_batch(
                output,
                f"silver/normalised/{VERSION}/postcodes/{key_digest}.jsonl.gz",
                cache_observations(entries, area_codes, resolve),
                provider=Provider.POSTCODES,
                scope="dated-postcode-cache",
                partition="cache",
                snapshot=None,
                source_manifest=resolve(postcode_manifest),
            )
        )
    bundle = Bundle(batches=tuple(batches))
    content = json_line(bundle.model_dump(mode="json"))
    manifest = output.put_immutable(
        f"manifests/silver/{VERSION}/{hashlib.sha256(content).hexdigest()}.json",
        iter([content]),
        media_type="application/json",
        compression="none",
    )
    return bundle, manifest


def verified_observations(store: ArtifactStore, batch: Batch) -> Iterator[Observation]:
    count, digest = 0, hashlib.sha256()
    for value in read_records(store, batch.records):
        record = Observation.model_validate(value)
        if record.provider != batch.provider or record.scope != batch.scope:
            raise ValueError("Observation does not belong to manifest source scope")
        if batch.snapshot is not None and (
            record.snapshot_id != batch.snapshot.snapshot_id
            or record.cycle_id != batch.snapshot.cycle.cycle_id
        ):
            raise ValueError("Observation snapshot differs from manifest")
        count += 1
        digest.update(json_line(value))
        yield record
    if count != batch.count or digest.hexdigest() != batch.decoded_sha256:
        raise ValueError("Normalised artifact count/hash mismatch")

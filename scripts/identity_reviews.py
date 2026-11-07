"""Import the owner's pilot decisions bound to the original reviewed source meanings."""

from __future__ import annotations

import hashlib
import json
import xml.etree.ElementTree as ET
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from shapely.geometry import shape
from shapely.ops import unary_union

from scripts.silver import bundle_for
from urbanlens.ingestion.interfaces import ArtifactStore
from urbanlens.ingestion.osm import normalize as normalise_osm
from urbanlens.silver import normalise
from urbanlens.silver.identity import PairReview, Ref, Reviews
from urbanlens.silver.models import Observation
from urbanlens.silver.pipeline import verified_observations


def import_pilot(store: ArtifactStore, directory: Path) -> tuple[Reviews, dict[str, int]]:
    review_path = directory / "venue_match_review.json"
    body = json.loads(review_path.read_text(encoding="utf-8"))
    metadata = body["review_metadata"]
    if metadata["status"] != "complete":
        raise ValueError("Pilot review is incomplete")
    for name, sha in metadata["source_files_sha256"].items():
        if hashlib.sha256((directory / name).read_bytes()).hexdigest() != sha:
            raise ValueError("Pilot source evidence changed since review")
    bundle, _ = bundle_for(store, "s3")
    records = {}
    areas = []
    for batch in bundle.batches:
        if batch.provider.value not in {"fsa", "osm", "ons"}:
            continue
        for record in verified_observations(store, batch):
            if record.area:
                areas.append(shape(record.area.geometry))
            else:
                records[record.provider.value, record.source_key] = record
    city = unary_union(areas)
    raw_fsa = {
        str(e.findtext("FHRSID")).strip(): e
        for e in ET.fromstring((directory / "fsa.xml").read_bytes()).findall(
            "EstablishmentCollection/EstablishmentDetail"
        )
    }
    raw_osm = {
        f"{e['type']}/{e['id']}": e
        for e in json.loads((directory / "osm.json").read_bytes())["elements"]
        if e.get("type") in {"node", "way", "relation"}
    }
    evidence = {}
    for name in ["venue_match_review.json", "fsa.xml", "osm.json", "manifest.json"]:
        content = (directory / name).read_bytes()
        evidence[name] = store.put_immutable(
            "evidence/identity-pilot/" + hashlib.sha256(content).hexdigest() + "/" + name,
            iter([content]),
            media_type="application/xml" if name.endswith(".xml") else "application/json",
            compression="none",
        )
    matched = stale = 0

    def ref(side: dict[str, Any]) -> Ref:
        nonlocal matched, stale
        provider = side["source"].casefold()
        current: Observation = records[provider, side["id"]]
        context = dict(
            scope=current.scope,
            snapshot_id=current.snapshot_id,
            cycle_id=current.cycle_id,
            observed_at=current.observed_at,
            evidence=current.evidence,
        )
        if provider == "fsa":
            element = raw_fsa[current.source_key]
            raw: dict[str, Any] = {c.tag: c.text for c in element}
            raw.update(
                Longitude=element.findtext("Geocode/Longitude"),
                Latitude=element.findtext("Geocode/Latitude"),
                Scores={c.tag: c.text for c in element.findall("Scores/*")},
            )
            original = normalise.fsa(raw, context)
        else:
            row = normalise_osm(
                raw_osm[current.source_key],
                {"query_sha256": "0" * 64, "boundary_sha256": "0" * 64},
                datetime.now(UTC),
                city,
            )
            original = normalise.osm(row, context)
        matched += original.record_hash == current.record_hash
        stale += original.record_hash != current.record_hash
        return Ref(
            provider=provider,
            scope=current.scope,
            source_key=current.source_key,
            generation=1,
            record_hash=original.record_hash,
        )

    mappings = []
    held = set()
    preferred = set()
    for pair in body["fsa_osm_pairs"] + body["osm_duplicate_pairs"]:
        left, right = ref(pair["left"]), ref(pair["right"])
        mappings.append(
            PairReview(
                review_id=pair["review_id"],
                left=left,
                right=right,
                decision=pair["decision"],
                reviewer=metadata["reviewer"],
                reviewed_on=metadata["review_date"],
                evidence=evidence["venue_match_review.json"].uri + "#" + pair["review_id"],
                reason=pair.get("notes") or "Owner reviewed this pair as " + pair["decision"] + ".",
            )
        )
        if pair["review_id"] in {"M007", "M030"}:
            held.add(right.key)
        if pair["review_id"] == "M027":
            held.add(left.key)
        if pair["review_id"] == "M028":
            preferred.add(right.key)
    return Reviews(
        version="identity-reviews-v1",
        reviews=tuple(mappings),
        held_source_keys=tuple(sorted(held)),
        preferred_name_keys=tuple(sorted(preferred)),
    ), dict(
        reviewed_pairs=len(mappings),
        matching_source_endpoints=matched,
        changed_source_endpoints=stale,
    )

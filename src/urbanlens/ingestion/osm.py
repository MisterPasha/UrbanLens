"""Bounded OSM evidence collection; no canonical matching or public request dependency."""

from __future__ import annotations

import hashlib
import json
import math
import tempfile
import unicodedata
from collections import Counter
from collections.abc import Iterator, Mapping
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any
from urllib.parse import urlencode
from uuid import NAMESPACE_URL, uuid5

from pydantic import Field
from shapely.geometry import LineString, Point, Polygon, box, mapping, shape
from shapely.ops import polygonize_full, unary_union

from urbanlens.ingestion.cadence import month_window
from urbanlens.ingestion.contracts import (
    CollectionCycle,
    Contract,
    Counts,
    FailureClass,
    Partition,
    Provider,
    Run,
)
from urbanlens.ingestion.errors import IngestionError
from urbanlens.ingestion.http import HttpDownloader, HttpPolicy
from urbanlens.ingestion.interfaces import ArtifactStore, Layer, artifact_key
from urbanlens.ingestion.payload import payload
from urbanlens.ingestion.storage import chunks, gzip_chunks
from urbanlens.ingestion.validation import ValidationRules

SCHEMA = "osm-overpass-json-v1"
PIPELINE = "osm-source-v1"
KINDS = {"node", "way", "relation"}


def require(condition: bool, code: str) -> None:
    if not condition:
        raise IngestionError(FailureClass.SCHEMA, code)


class OsmConfig(Contract):
    version: str = "osm-venues-monthly-v2"
    scope: str = "brighton-hove-e06000043"
    endpoint: str = "https://overpass-api.de/api/interpreter"
    boundary_sha256: str = Field(pattern=r"^[a-f0-9]{64}$")
    template_sha256: str = Field(pattern=r"^[a-f0-9]{64}$")
    padding_degrees: float = Field(default=0.03, ge=0.01, le=0.1)
    min_records: int = Field(default=1000, ge=0)
    max_records: int = Field(default=20000, ge=1)
    max_drop_fraction: float = Field(default=0.2, ge=0, le=1)

    def cycle(self, day: date) -> CollectionCycle:
        digest = hashlib.sha256(self.model_dump_json().encode()).hexdigest()
        start, end = month_window(day)
        return CollectionCycle(
            cycle_id=uuid5(NAMESPACE_URL, f"osm/{self.scope}/{start.date()}/{digest}"),
            provider=Provider.OSM,
            scope=self.scope,
            query_version=self.version,
            query_sha256=digest,
            window_start=start,
            window_end=end,
        )

    def rules(self) -> ValidationRules:
        return ValidationRules(
            version=self.version,
            identity_field="id",
            required_fields=("element_type", "osm_id", "tags", "geometry_state"),
            min_records=self.min_records,
            max_records=self.max_records,
            max_drop_fraction=self.max_drop_fraction,
        )


def definition(config: OsmConfig, boundary: bytes, template: bytes) -> dict[str, Any]:
    require(hashlib.sha256(boundary).hexdigest() == config.boundary_sha256, "osm-boundary-hash")
    require(hashlib.sha256(template).hexdigest() == config.template_sha256, "osm-template-hash")
    geojson = json.loads(boundary)
    city = shape(geojson["geometry"])
    require(
        city.is_valid and not city.is_empty and city.geom_type in {"Polygon", "MultiPolygon"},
        "osm-invalid-city",
    )
    west, south, east, north = city.bounds
    pad = config.padding_degrees
    bounds = [south - pad, west - pad, north + pad, east + pad]
    bbox = ",".join(f"{v:.7f}" for v in bounds)
    query = template.decode().replace("{{bbox}}", bbox)
    require("{{" not in query and "out count;" in query, "osm-invalid-query-template")
    return {
        "config": config.model_dump(mode="json"),
        "boundary": geojson,
        "boundary_text": boundary.decode(),
        "template": template.decode(),
        "bbox": bounds,
        "query": query,
        "query_sha256": hashlib.sha256(query.encode()).hexdigest(),
        "http_method": "POST",
        "request_body_sha256": hashlib.sha256(urlencode({"data": query}).encode()).hexdigest(),
        "boundary_sha256": config.boundary_sha256,
    }


def timestamp(value: object) -> datetime:
    require(isinstance(value, str), "osm-invalid-timestamp")
    try:
        result = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        raise IngestionError(FailureClass.SCHEMA, "osm-invalid-timestamp") from None
    require(result.utcoffset() == timedelta(0), "osm-non-utc-timestamp")
    require(result <= datetime.now(UTC) + timedelta(minutes=5), "osm-future-timestamp")
    return result


def document(value: Any) -> tuple[list[dict[str, Any]], datetime]:
    require(isinstance(value, dict), "osm-invalid-document")
    require(not value.get("remark") and not value.get("error"), "osm-error-bearing-response")
    require(value.get("version") == 0.6, "osm-invalid-api-version")
    metadata = value.get("osm3s")
    require(isinstance(metadata, dict), "osm-missing-source-metadata")
    extracted = timestamp(metadata.get("timestamp_osm_base"))
    values = value.get("elements")
    require(isinstance(values, list) and bool(values), "osm-missing-terminal-count")
    require(all(isinstance(e, dict) for e in values), "osm-invalid-element")
    require(values[-1].get("type") == "count", "osm-missing-terminal-count")
    elements = values[:-1]
    require(all(e.get("type") in KINDS for e in elements), "osm-unexpected-element-type")
    tags = values[-1].get("tags", {})
    actual = Counter(e["type"] for e in elements)
    try:
        require(
            all(
                type(tags[key]) is int
                or (isinstance(tags[key], str) and tags[key].isascii() and tags[key].isdigit())
                for key in ("nodes", "ways", "relations", "total")
            ),
            "osm-invalid-terminal-count",
        )
        expected = {kind: int(tags[kind + "s"]) for kind in KINDS}
        total = int(tags["total"])
    except (KeyError, TypeError, ValueError):
        raise IngestionError(FailureClass.INCOMPLETE, "osm-invalid-terminal-count") from None
    require(
        total == len(elements) and all(expected[k] == actual[k] for k in KINDS),
        "osm-terminal-count-mismatch",
    )
    for element in elements:
        require(type(element.get("id")) is int and element["id"] > 0, "osm-invalid-element-id")
        require(isinstance(element.get("tags"), dict), "osm-missing-tags")
        require(
            all(isinstance(k, str) and isinstance(v, str) for k, v in element["tags"].items()),
            "osm-invalid-tags",
        )
        if "version" in element:
            require(
                type(element["version"]) is int and element["version"] > 0,
                "osm-invalid-element-version",
            )
        if "timestamp" in element:
            require(timestamp(element["timestamp"]) <= extracted, "osm-edit-after-base")
    return elements, extracted


def coordinates(values: Any) -> list[tuple[float, float]]:
    if not isinstance(values, list) or not values:
        raise ValueError("Missing geometry")
    result = []
    for item in values:
        if not isinstance(item, dict):
            raise ValueError("Incomplete geometry")
        x, y = item.get("lon"), item.get("lat")
        if (
            not isinstance(x, (float, int))
            or not isinstance(y, (float, int))
            or isinstance(x, bool)
            or isinstance(y, bool)
        ):
            raise ValueError("Invalid coordinate")
        if not (math.isfinite(x) and math.isfinite(y) and -180 <= x <= 180 and -90 <= y <= 90):
            raise ValueError("Invalid coordinate")
        result.append((float(x), float(y)))
    return result


def geometry(element: dict[str, Any]) -> tuple[Any, str]:
    """Return original-footprint geometry; unresolved features remain source evidence."""
    try:
        if element["type"] == "node":
            return Point(coordinates([element])[0]), "source-node"
        if element["type"] == "way":
            points = coordinates(element.get("geometry"))
            nodes = element.get("nodes")
            if not isinstance(nodes, list) or len(nodes) != len(points):
                return None, "unresolved-incomplete-way"
            if (
                len(points) < 4
                or nodes[0] != nodes[-1]
                or points[0] != points[-1]
                or element["tags"].get("area") == "no"
            ):
                return None, "unresolved-open-way"
            polygon = Polygon(points)
        elif element["tags"].get("type") == "multipolygon":
            outer: list[Any] = []
            inner: list[Any] = []
            for member in element.get("members", []):
                if member.get("type") != "way" or member.get("role") not in {"", "outer", "inner"}:
                    return None, "unresolved-relation-member"
                points = coordinates(member.get("geometry"))
                if len(points) < 2:
                    return None, "unresolved-relation-member"
                (inner if member.get("role") == "inner" else outer).append(LineString(points))
            polygons, cuts, dangles, invalid = polygonize_full(outer)
            if polygons.is_empty or not all(g.is_empty for g in (cuts, dangles, invalid)):
                return None, "unresolved-relation-ring"
            polygon = unary_union(list(polygons.geoms))
            if inner:
                holes, cuts, dangles, invalid = polygonize_full(inner)
                holes = unary_union(list(holes.geoms))
                if (
                    holes.is_empty
                    or not all(g.is_empty for g in (cuts, dangles, invalid))
                    or not polygon.covers(holes)
                ):
                    return None, "unresolved-relation-hole"
                polygon = polygon.difference(holes)
        else:
            return None, "unresolved-nonmultipolygon-relation"
        if polygon.is_empty or not polygon.is_valid or polygon.area <= 0:
            return None, "unresolved-invalid-polygon"
        return polygon, "polygon-point-on-surface"
    except (KeyError, TypeError, ValueError):
        return None, "unresolved-invalid-geometry"


def normalize(
    element: dict[str, Any], source: dict[str, Any], extracted: datetime, city: Any = None
) -> dict[str, Any]:
    city = shape(source["boundary"]["geometry"]) if city is None else city
    footprint, method = geometry(element)
    point = (
        None
        if footprint is None
        else footprint
        if footprint.geom_type == "Point"
        else footprint.representative_point()
    )
    tags = element["tags"]
    tokens = [t.strip() for t in tags.get("fhrs:id", "").split(";") if t.strip()]
    ids = sorted({t for t in tokens if t.isascii() and t.isdigit() and int(t) > 0})
    venue_tag = bool(
        {t.strip() for t in tags.get("amenity", "").split(";")}
        & {"restaurant", "cafe", "fast_food", "pub", "bar"}
        or "bakery" in {t.strip() for t in tags.get("shop", "").split(";")}
    )
    return {
        "id": f"osm/{element['type']}/{element['id']}",
        "element_type": element["type"],
        "osm_id": element["id"],
        "source_version": element.get("version"),
        "source_timestamp": element.get("timestamp"),
        "tags": tags,
        "name_raw": tags.get("name"),
        "name": " ".join(unicodedata.normalize("NFKC", tags.get("name", "")).split()),
        "fhrs_ids": ids,
        "invalid_fhrs_tokens": sorted(set(tokens) - set(ids)),
        "venue_tag": venue_tag,
        "geometry_state": method,
        "longitude": None if point is None else point.x,
        "latitude": None if point is None else point.y,
        "geometry": None if footprint is None else mapping(footprint),
        "original_geometry": {
            k: element[k]
            for k in ("lat", "lon", "nodes", "geometry", "members", "bounds")
            if k in element
        },
        "in_city": None if point is None else bool(city.covers(point)),
        "intersects_city": None if footprint is None else bool(city.intersects(footprint)),
        "crosses_city_boundary": None
        if footprint is None
        else bool(city.intersects(footprint) and not city.covers(footprint)),
        "source_extracted_at": extracted.isoformat(),
        "query_sha256": source["query_sha256"],
        "boundary_sha256": source["boundary_sha256"],
    }


class OsmAdapter:
    provider = Provider.OSM

    def __init__(
        self,
        config: OsmConfig,
        source: dict[str, Any] | None = None,
        downloader: HttpDownloader | None = None,
    ) -> None:
        self.config, self.source = config, source
        self.downloader = downloader or HttpDownloader(
            Provider.OSM,
            HttpPolicy(
                max_requests=2,
                max_retries=1,
                max_bytes=32 * 1024 * 1024,
                max_seconds=240,
                timeout_seconds=100,
            ),
            headers={
                "Accept": "application/json",
                "Content-Type": "application/x-www-form-urlencoded",
            },
        )

    def fetch(
        self, cycle: CollectionCycle, run: Run, partition_id: str, store: ArtifactStore
    ) -> Partition:
        require(
            cycle == self.config.cycle(cycle.window_start.date()) and partition_id == "full",
            "osm-query-definition-mismatch",
        )
        require(self.source is not None, "osm-missing-query-definition")
        assert self.source is not None
        saved = store.put_immutable(
            artifact_key(Layer.BRONZE, cycle, run.run_id, "definition.json.gz", partition_id),
            gzip_chunks(iter([json.dumps(self.source, sort_keys=True).encode()])),
            media_type="application/json",
            compression="gzip",
        )
        with tempfile.TemporaryFile() as target:
            self.downloader.download(
                self.config.endpoint,
                target,
                method="POST",
                content=urlencode({"data": self.source["query"]}).encode(),
            )
            raw = store.put_immutable(
                artifact_key(Layer.BRONZE, cycle, run.run_id, "elements.json.gz", partition_id),
                gzip_chunks(chunks(target)),
                media_type="application/json",
                compression="gzip",
            )
        count, extracted = None, None
        try:
            with payload(store, raw) as stream:
                elements, extracted = document(json.load(stream))
                count = len(elements)
        except (IngestionError, ValueError):
            pass  # Retain raw bytes; parse must reject malformed/partial content before acceptance.
        return Partition(
            partition_id=partition_id,
            cycle_id=cycle.cycle_id,
            run_id=run.run_id,
            schema_version=SCHEMA,
            pipeline_version=run.pipeline_version,
            status="fetched",
            source_extracted_at=extracted,
            fetched_at=datetime.now(UTC),
            artifacts=(raw, saved),
            counts=Counts(raw=count),
        )

    def parse(self, partition: Partition, store: ArtifactStore) -> Iterator[Mapping[str, object]]:
        require(
            partition.schema_version == SCHEMA and len(partition.artifacts) == 2,
            "osm-missing-raw-inputs",
        )
        try:
            with payload(store, partition.artifacts[1]) as stream:
                source = json.load(stream)
            config = OsmConfig.model_validate(source["config"])
            require(config == self.config, "osm-replay-config-mismatch")
            require(
                definition(config, source["boundary_text"].encode(), source["template"].encode())
                == source,
                "osm-retained-definition-mismatch",
            )
            with payload(store, partition.artifacts[0]) as stream:
                elements, extracted = document(json.load(stream))
            require(
                partition.source_extracted_at is None or partition.source_extracted_at == extracted,
                "osm-source-time-mismatch",
            )
            south, west, north, east = source["bbox"]
            envelope = box(west, south, east, north)
            city = shape(source["boundary"]["geometry"])
            for element in elements:
                tags = element["tags"]
                require(
                    "fhrs:id" in tags
                    or bool(
                        {t.strip() for t in tags.get("amenity", "").split(";")}
                        & {"restaurant", "cafe", "fast_food", "pub", "bar"}
                        or "bakery" in {t.strip() for t in tags.get("shop", "").split(";")}
                    ),
                    "osm-unexpected-query-result",
                )
                row = normalize(element, source, extracted, city)
                if row["geometry"] is not None:
                    require(
                        envelope.intersects(shape(row["geometry"])), "osm-outside-extraction-scope"
                    )
                yield row
        except (ValueError, KeyError, TypeError):
            raise IngestionError(
                FailureClass.SCHEMA, "osm-malformed-response-or-definition"
            ) from None


def local_source(root: Path) -> tuple[OsmConfig, dict[str, Any]]:
    config = OsmConfig.model_validate_json((root / "config/osm-source.json").read_text())
    source = definition(
        config,
        (root / "data/reference/geography/city-coverage-wgs84.geojson").read_bytes(),
        (root / "reference/ingestion/osm-query.overpassql").read_bytes(),
    )
    return config, source

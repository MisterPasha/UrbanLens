"""PostGIS spatial contracts, plus real pinned ONS geometries when acquired locally."""

import json
import os
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import psycopg
import pytest
from dotenv import dotenv_values

from urbanlens.spatial import DISTANCE_EPSILON_METRES

ROOT = Path(__file__).resolve().parents[2]
pytestmark = pytest.mark.database


@pytest.fixture
def spatial_connection() -> Iterator[psycopg.Connection[Any]]:
    values = dotenv_values(ROOT / ".env")

    def setting(name: str, default: str = "") -> str:
        return os.environ.get(name) or values.get(name) or default

    with psycopg.connect(
        host=setting("LOCAL_POSTGRES_HOST", "127.0.0.1"),
        port=int(setting("LOCAL_POSTGRES_PORT", "54322")),
        dbname=setting("LOCAL_POSTGRES_DB", "urbanlens"),
        user=setting("LOCAL_POSTGRES_USER", "urbanlens_local"),
        password=setting("LOCAL_POSTGRES_PASSWORD"),
        connect_timeout=5,
    ) as connection:
        try:
            yield connection
        finally:
            connection.rollback()


@pytest.mark.parametrize("radius", [250, 500, 1000, 2000])
def test_geography_metres_and_inclusive_distance(
    spatial_connection: psycopg.Connection[Any],
    radius: int,
) -> None:
    with spatial_connection.cursor() as cursor:
        cursor.execute(
            """
            WITH centre AS (
                SELECT ST_SetSRID(ST_MakePoint(-0.14, 50.82), 4326)::geography AS p
            ), probes AS (
                SELECT p, ST_Project(p, %s - 1, 0.0) AS inside,
                       ST_Project(p, %s + 0.01, 0.0) AS outside,
                       ST_Project(p, %s, 0.0) AS edge FROM centre
            )
            SELECT ST_SRID(p::geometry), ST_DWithin(p, inside, %s),
                   ST_DWithin(p, outside, %s),
                   ST_DWithin(p, edge, %s), ST_Distance(p, edge)
            FROM probes
            """,
            (
                radius,
                radius,
                radius,
                radius + DISTANCE_EPSILON_METRES,
                radius + DISTANCE_EPSILON_METRES,
                radius + DISTANCE_EPSILON_METRES,
            ),
        )
        result = cursor.fetchone()
        assert result is not None
        assert result[:4] == (4326, True, False, True)
        assert result[4] == pytest.approx(radius, abs=0.00001)


def test_boundary_tie_break_and_projected_units(
    spatial_connection: psycopg.Connection[Any],
) -> None:
    with spatial_connection.cursor() as cursor:
        cursor.execute(
            """
            WITH areas(code, geom) AS (VALUES
                ('E01000002', ST_MakeEnvelope(100, 0, 200, 100, 27700)),
                ('E01000001', ST_MakeEnvelope(0, 0, 100, 100, 27700))
            ), point AS (SELECT ST_SetSRID(ST_MakePoint(100, 50), 27700) AS p)
            SELECT code, ST_Area(geom), ST_Covers(geom, p), ST_Contains(geom, p)
            FROM areas CROSS JOIN point WHERE ST_Covers(geom, p) ORDER BY code
            """
        )
        rows = cursor.fetchall()
        assert rows == [("E01000001", 10000.0, True, False), ("E01000002", 10000.0, True, False)]
        assert rows[0][0] == "E01000001"


def test_real_ons_wgs84_storage_and_city_coverage(
    spatial_connection: psycopg.Connection[Any],
) -> None:
    path = ROOT / "data/reference/geography/areas-wgs84.geojson"
    if not path.exists():
        pytest.skip("Real ONS artifacts not acquired; run geography-fetch and geography-check")
    features = json.loads(path.read_text(encoding="utf-8"))["features"]
    summary = json.loads((ROOT / "reference/geography/inspection.json").read_text(encoding="utf-8"))
    with spatial_connection.cursor() as cursor:
        cursor.execute(
            "CREATE TEMP TABLE geography_probe "
            "(code text PRIMARY KEY, geom geometry(Geometry,4326))"
        )
        cursor.executemany(
            "INSERT INTO geography_probe VALUES (%s, ST_SetSRID(ST_GeomFromGeoJSON(%s),4326))",
            [(f["properties"]["LSOA21CD"], json.dumps(f["geometry"])) for f in features],
        )
        cursor.execute(
            """
            SELECT count(*), count(*) FILTER (WHERE ST_IsValid(geom) AND NOT ST_IsEmpty(geom)
                AND ST_SRID(geom)=4326 AND ST_Covers(geom, ST_PointOnSurface(geom)))
            FROM geography_probe
            """
        )
        assert cursor.fetchone() == (summary["lsoa_count"], summary["lsoa_count"])
        cursor.execute(
            """
            WITH city AS (SELECT ST_UnaryUnion(ST_Collect(geom)) AS geom FROM geography_probe),
            edge AS (SELECT geom, ST_PointN(ST_ExteriorRing(geom), 1) AS p FROM city)
            SELECT ST_IsValid(geom), ST_Covers(geom,p), ST_Contains(geom,p),
                   ST_Covers(geom,ST_Buffer(p::geography,500)::geometry),
                   ST_Covers(geom,ST_SetSRID(ST_MakePoint(-0.14,50.79),4326)) FROM edge
            """
        )
        assert cursor.fetchone() == (True, True, False, False, False)

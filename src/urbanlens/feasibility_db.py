"""Representative scratch-schema load/dbt measurements, never production migrations."""

from __future__ import annotations

import csv
import json
import os
import shutil
import subprocess
import sys
import tempfile
import time
import xml.etree.ElementTree as ET
from pathlib import Path
from typing import Any

import psycopg
from psycopg.conninfo import conninfo_to_dict

from urbanlens.feasibility import peak_rss, require

SCHEMA = "urbanlens_feasibility"


def normalise(inputs: Path) -> tuple[list[tuple[Any, ...]], list[tuple[Any, ...]]]:
    from shapely.geometry import LineString, Point, Polygon, shape

    venues = []
    source = inputs / "pilots/venues/2026-11-02"
    for row in ET.parse(source / "fsa.xml").findall(".//EstablishmentDetail"):

        def field(name: str, row: ET.Element = row) -> str:
            return row.findtext(name, "").strip()

        address = " ".join(field(f"AddressLine{i}") for i in range(1, 5)).strip()
        held = field("AddressLine1").lower() in {"private address", "address not disclosed"}
        held = held or (not address and len(field("PostCode").replace(" ", "")) < 5)
        lat, lon = field("Geocode/Latitude"), field("Geocode/Longitude")
        located = bool(lat and lon and not held)
        venues.append(
            (
                "fsa/" + field("FHRSID"),
                "fsa",
                field("BusinessName"),
                field("PostCode"),
                field("BusinessTypeID"),
                float(lon) if located else None,
                float(lat) if located else None,
                held,
                False,
            )
        )
    elements = json.loads((source / "osm.json").read_text(encoding="utf-8"))["elements"]
    for row in elements:
        if row["type"] == "count":
            continue
        tags = row.get("tags", {})
        point = None
        approximate = row["type"] != "node"
        if row["type"] == "node":
            point = Point(row["lon"], row["lat"])
        elif row["type"] == "way" and row.get("geometry"):
            coords = [(p["lon"], p["lat"]) for p in row["geometry"]]
            if len(coords) >= 4 and coords[0] == coords[-1]:
                point = Polygon(coords).representative_point()
            elif len(coords) >= 2:
                point = LineString(coords).centroid
        venues.append(
            (
                f"osm/{row['type']}/{row['id']}",
                "osm",
                tags.get("name", ""),
                tags.get("addr:postcode", ""),
                tags.get("amenity", tags.get("shop", "")),
                point.x if point else None,
                point.y if point else None,
                False,
                approximate,
            )
        )
    features = json.loads(
        (inputs / "reference/geography/areas-wgs84.geojson").read_text(encoding="utf-8")
    )["features"]
    areas = [
        (f["properties"]["LSOA21CD"], "SRID=4326;" + shape(f["geometry"]).wkt) for f in features
    ]
    return venues, areas


def benchmark(inputs: Path, dsn: str, work: Path) -> dict[str, Any]:
    started = time.perf_counter()
    normal_start = time.perf_counter()
    venues, areas = normalise(inputs)
    normal_seconds = time.perf_counter() - normal_start
    with (inputs / "pilots/companies/2026-11-02/registered-office-cohort.csv").open(
        encoding="utf-8", newline=""
    ) as stream:
        companies = list(csv.DictReader(stream))
    connect_start = time.perf_counter()
    with psycopg.connect(dsn, connect_timeout=20, prepare_threshold=None) as connection:
        connect_seconds = time.perf_counter() - connect_start
        with connection.cursor() as cursor:
            cursor.execute("CREATE EXTENSION IF NOT EXISTS postgis")
            cursor.execute("SELECT to_regnamespace(%s)", (SCHEMA,))
            exists = cursor.fetchone()
            if exists and exists[0]:
                cursor.execute("SELECT version FROM urbanlens_feasibility.probe_version")
                require(cursor.fetchone() == (1,), "Unrecognised existing probe schema")
            else:
                cursor.execute("CREATE SCHEMA urbanlens_feasibility")
                cursor.execute("CREATE TABLE urbanlens_feasibility.probe_version(version int)")
                cursor.execute("INSERT INTO urbanlens_feasibility.probe_version VALUES (1)")
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS urbanlens_feasibility.areas (
                    code text PRIMARY KEY, geom geometry(Geometry,4326) NOT NULL);
                CREATE TABLE IF NOT EXISTS urbanlens_feasibility.venues (
                    id text PRIMARY KEY, source text, name text, postcode text, venue_type text,
                    lon double precision, lat double precision, held boolean, approximate boolean,
                    point geometry(Point,4326), lsoa_code text);
                CREATE TABLE IF NOT EXISTS urbanlens_feasibility.companies (
                    number text PRIMARY KEY, name text, postcode text, lsoa_code text, sics text[]);
                TRUNCATE urbanlens_feasibility.venues, urbanlens_feasibility.areas,
                         urbanlens_feasibility.companies;
            """)
            load_start = time.perf_counter()
            with cursor.copy("COPY urbanlens_feasibility.areas (code,geom) FROM STDIN") as copy:
                for area in areas:
                    copy.write_row(area)
            with (
                cursor.copy("""COPY urbanlens_feasibility.venues
                (id,source,name,postcode,venue_type,lon,lat,held,approximate) FROM STDIN""") as copy
            ):
                for venue in venues:
                    copy.write_row(venue)
            with cursor.copy("COPY urbanlens_feasibility.companies FROM STDIN") as copy:
                for company in companies:
                    copy.write_row(
                        (
                            company["company_number"],
                            company["company_name"],
                            company["postcode"],
                            company["lsoa21_code"],
                            company["sic_codes"].split(";"),
                        )
                    )
            cursor.execute("""
                UPDATE urbanlens_feasibility.venues
                SET point=ST_SetSRID(ST_MakePoint(lon,lat),4326)
                WHERE lon IS NOT NULL AND lat IS NOT NULL AND NOT held;
                CREATE INDEX IF NOT EXISTS probe_area_geom
                    ON urbanlens_feasibility.areas USING gist(geom);
                CREATE INDEX IF NOT EXISTS probe_venue_point
                    ON urbanlens_feasibility.venues USING gist(point);
                UPDATE urbanlens_feasibility.venues v SET lsoa_code=(
                    SELECT min(a.code) FROM urbanlens_feasibility.areas a
                    WHERE ST_Covers(a.geom,v.point));
                CREATE INDEX IF NOT EXISTS probe_venue_lsoa
                    ON urbanlens_feasibility.venues(lsoa_code);
                CREATE INDEX IF NOT EXISTS probe_company_lsoa
                    ON urbanlens_feasibility.companies(lsoa_code);
                ANALYZE urbanlens_feasibility.venues;
            """)
            load_seconds = time.perf_counter() - load_start
            cursor.execute("SELECT current_setting('server_version'),postgis_lib_version()")
            versions = cursor.fetchone()
        connection.commit()
    dbt_seconds, scratch_bytes = run_dbt(dsn, work)
    with psycopg.connect(dsn, connect_timeout=20, prepare_threshold=None) as connection:
        with connection.cursor() as cursor:
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS urbanlens_feasibility.gold_candidate
                    AS TABLE urbanlens_feasibility.gold_probe WITH NO DATA;
                CREATE TABLE IF NOT EXISTS urbanlens_feasibility.gold_previous
                    AS TABLE urbanlens_feasibility.gold_probe WITH NO DATA;
                TRUNCATE urbanlens_feasibility.gold_candidate,urbanlens_feasibility.gold_previous;
                INSERT INTO urbanlens_feasibility.gold_candidate
                    SELECT * FROM urbanlens_feasibility.gold_probe;
                INSERT INTO urbanlens_feasibility.gold_previous
                    SELECT * FROM urbanlens_feasibility.gold_probe;
            """)
            cursor.execute("SELECT sum(venue_count) FROM urbanlens_feasibility.gold_probe")
            totals = cursor.fetchone()
            require(
                totals is not None and totals[0] == len(venues), "dbt totals failed to reconcile"
            )
            cursor.execute(
                """SELECT c.relname,pg_relation_size(c.oid),
                pg_indexes_size(c.oid),pg_total_relation_size(c.oid)
                FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
                WHERE n.nspname=%s AND c.relkind='r' ORDER BY c.relname""",
                (SCHEMA,),
            )
            sizes = [
                {"table": r[0], "heap_bytes": r[1], "index_bytes": r[2], "total_bytes": r[3]}
                for r in cursor.fetchall()
            ]
            cursor.execute("SELECT pg_database_size(current_database())")
            db_size = cursor.fetchone()
            cursor.execute(
                "SELECT count(*) FROM urbanlens_feasibility.venues WHERE held AND point IS NOT NULL"
            )
            require(cursor.fetchone() == (0,), "Privacy-held location was enriched")
    return {
        "scope": "unmerged source records, scratch schema only; not a market total",
        "normalisation_seconds": normal_seconds,
        "connect_seconds": connect_seconds,
        "load_index_spatial_seconds": load_seconds,
        "dbt_seconds": dbt_seconds,
        "dbt_scratch_bytes": scratch_bytes,
        "total_seconds": time.perf_counter() - started,
        "venues": len(venues),
        "areas": len(areas),
        "companies": len(companies),
        "versions": versions,
        "relations": sizes,
        "schema_total_bytes": sum(r["total_bytes"] for r in sizes),
        "database_bytes": db_size[0] if db_size else None,
        "process_peak_rss_bytes": peak_rss(),
        "dbt_child_peak_rss_bytes": peak_rss(children=True),
    }


def run_dbt(dsn: str, work: Path) -> tuple[float, int]:
    """Actual adapter run using one direct connection; secrets exist only in its environment."""
    settings = conninfo_to_dict(dsn)
    directory = Path(tempfile.mkdtemp(prefix="dbt-probe-", dir=work))
    (directory / "models").mkdir()
    (directory / "dbt_project.yml").write_text(
        "name: feasibility\nversion: '1.0'\nconfig-version: 2\nprofile: feasibility\n"
        "model-paths: [models]\n",
        encoding="utf-8",
    )
    profile = "feasibility:\n  target: probe\n  outputs:\n    probe:\n      type: postgres\n"
    fields = {
        "host": settings["host"],
        "user": settings["user"],
        "dbname": settings["dbname"],
        "port": int(settings.get("port") or 5432),
        "sslmode": settings.get("sslmode", "prefer"),
        "schema": SCHEMA,
        "threads": 1,
        "connect_timeout": 20,
    }
    for key, value in fields.items():
        profile += "      " + key + ": " + json.dumps(value) + "\n"
    profile += "      password: \"{{ env_var('DBT_ENV_SECRET_FEASIBILITY_PASSWORD') }}\"\n"
    (directory / "profiles.yml").write_text(profile, encoding="utf-8")
    (directory / "models/gold_probe.sql").write_text(
        "{{ config(materialized='table') }}\n"
        "select coalesce(lsoa_code,'unassigned') lsoa_code,venue_type,count(*) venue_count "
        "from urbanlens_feasibility.venues group by 1,2\n",
        encoding="utf-8",
    )
    environment = os.environ.copy()
    environment.update(
        DBT_ENV_SECRET_FEASIBILITY_PASSWORD=str(settings.get("password") or ""),
        DBT_SEND_ANONYMOUS_USAGE_STATS="false",
        DBT_USE_COLORS="false",
    )
    executable = str(Path(sys.executable).with_name("dbt.exe" if os.name == "nt" else "dbt"))
    started = time.perf_counter()
    result = subprocess.run(
        [executable, "run", "--project-dir", str(directory), "--profiles-dir", str(directory)],
        capture_output=True,
        env=environment,
    )
    (directory / "run.txt").write_bytes(result.stdout + result.stderr)
    require(result.returncode == 0, "dbt probe failed; inspect private run.txt")
    scratch = sum(path.stat().st_size for path in directory.rglob("*") if path.is_file())
    return time.perf_counter() - started, scratch


def dump_probe(dsn: str, work: Path) -> tuple[dict[str, Any], Path]:
    """Native PostgreSQL backup; credentials are environment-only and never argv."""
    executable = shutil.which("pg_dump")
    require(executable is not None, "Native pg_dump client is missing from this runtime")
    assert executable is not None
    settings = conninfo_to_dict(dsn)
    environment = os.environ.copy()
    names = {
        "host": "PGHOST",
        "port": "PGPORT",
        "user": "PGUSER",
        "password": "PGPASSWORD",
        "dbname": "PGDATABASE",
        "sslmode": "PGSSLMODE",
    }
    for key, variable in names.items():
        if settings.get(key) is not None:
            environment[variable] = str(settings[key])
    path = work / "probe.dump"
    started = time.perf_counter()
    result = subprocess.run(
        [
            executable,
            "--format=custom",
            "--schema=urbanlens_feasibility",
            "--no-owner",
            "--no-privileges",
            "--file",
            str(path),
        ],
        capture_output=True,
        env=environment,
    )
    require(result.returncode == 0, "Native backup failed; credentials omitted")
    version = subprocess.run([executable, "--version"], capture_output=True, check=True)
    return {
        "bytes": path.stat().st_size,
        "seconds": time.perf_counter() - started,
        "client": version.stdout.decode().strip(),
    }, path

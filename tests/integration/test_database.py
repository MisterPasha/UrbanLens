"""Real PostGIS/driver transaction check, run explicitly and in container-backed CI."""

import os
from pathlib import Path

import psycopg
import pytest
from dotenv import dotenv_values


@pytest.mark.database
def test_postgis_connection_and_transaction() -> None:
    values = dotenv_values(Path(__file__).resolve().parents[2] / ".env")

    def setting(name: str, default: str = "") -> str:
        return os.environ.get(name) or values.get(name) or default

    password = setting("LOCAL_POSTGRES_PASSWORD")
    if not password:
        pytest.fail("Local database password is missing. Run python scripts/dev.py setup.")

    with psycopg.connect(
        host=setting("LOCAL_POSTGRES_HOST", "127.0.0.1"),
        port=int(setting("LOCAL_POSTGRES_PORT", "54322")),
        dbname=setting("LOCAL_POSTGRES_DB", "urbanlens"),
        user=setting("LOCAL_POSTGRES_USER", "urbanlens_local"),
        password=password,
        connect_timeout=5,
    ) as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                "SELECT current_setting('server_version_num')::int, postgis_lib_version()"
            )
            version = cursor.fetchone()
            assert version is not None
            assert 170000 <= version[0] < 180000
            assert version[1].startswith("3.6.")
            cursor.execute("CREATE TEMP TABLE foundation_probe (point geography(Point, 4326))")
            cursor.execute(
                "INSERT INTO foundation_probe VALUES (ST_SetSRID(ST_MakePoint(%s, %s), 4326))",
                (-0.14, 50.82),
            )
            cursor.execute(
                "SELECT ST_SRID(point::geometry), ST_DWithin(point, point, 1) FROM foundation_probe"
            )
            assert cursor.fetchone() == (4326, True)
        connection.rollback()

"""Fresh real databases test migrations, published-only access and spatial indexes."""

import shutil
from collections.abc import Iterator
from pathlib import Path

import psycopg
import pytest

from scripts.database import credential, scratch_database
from urbanlens.database.migrations import ROLES, SQL_DIRECTORY, connect, migrate
from urbanlens.database.verification import exercise

pytestmark = pytest.mark.database


@pytest.fixture
def database_roles() -> Iterator[dict[str, str]]:
    with scratch_database("local") as name:
        dsns = {purpose: credential("local", purpose, database=name) for purpose in ROLES}
        with connect(dsns["migration"]) as connection:
            assert migrate(connection) == list(range(1, len(list(SQL_DIRECTORY.glob("*.sql"))) + 1))
            assert migrate(connection) == []
        yield dsns


def test_real_scoped_roles_published_views_and_spatial_contracts(
    database_roles: dict[str, str],
) -> None:
    assert all(exercise(database_roles).values())


def test_edited_applied_migration_is_rejected(
    database_roles: dict[str, str], tmp_path: Path
) -> None:
    for path in SQL_DIRECTORY.glob("*.sql"):
        shutil.copyfile(path, tmp_path / path.name)
    path = next(tmp_path.glob("0001_*.sql"))
    path.write_bytes(path.read_bytes() + b"\n-- edited fixture\n")
    with connect(database_roles["migration"]) as connection:
        original = connection.execute(
            "SELECT version,sha256 FROM ul_control.schema_migration ORDER BY version"
        ).fetchall()
        with pytest.raises(ValueError, match="edited"):
            migrate(connection, tmp_path)
        assert (
            connection.execute(
                "SELECT version,sha256 FROM ul_control.schema_migration ORDER BY version"
            ).fetchall()
            == original
        )


def test_failed_migration_rolls_back_ddl_and_history(
    database_roles: dict[str, str], tmp_path: Path
) -> None:
    for path in SQL_DIRECTORY.glob("*.sql"):
        shutil.copyfile(path, tmp_path / path.name)
    next_version = len(list(SQL_DIRECTORY.glob("*.sql"))) + 1
    path = tmp_path / f"{next_version:04}_failure.sql"
    path.write_text("CREATE TABLE ul_silver.failed_migration_probe(id integer); SELECT 1/0;")
    with connect(database_roles["migration"]) as connection:
        with pytest.raises(psycopg.errors.DivisionByZero):
            migrate(connection, tmp_path)
        assert connection.execute(
            "SELECT to_regclass('ul_silver.failed_migration_probe')"
        ).fetchone() == (None,)
        assert connection.execute(
            "SELECT count(*) FROM ul_control.schema_migration WHERE version=%s", (next_version,)
        ).fetchone() == (0,)
        path.write_text("CREATE TABLE ul_silver.recovered_migration_probe(id integer);")
        assert migrate(connection, tmp_path) == [next_version]
        assert migrate(connection, tmp_path) == []

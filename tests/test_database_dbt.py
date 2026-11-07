"""Scoped dbt diagnostics must not fall back to the administrator credentials."""

import subprocess
from pathlib import Path
from typing import Any

import pytest

from urbanlens.database import dbt


def test_dbt_uses_scoped_login_and_keeps_password_out_of_profile(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    (tmp_path / ".env").write_text(
        "LOCAL_POSTGRES_PASSWORD=owner-placeholder\n"
        "URBANLENS_LOCAL_DBT_DATABASE_URL=postgresql://urbanlens_dbt:role-placeholder@127.0.0.1/db\n"
    )
    calls = []

    def run(arguments: list[str], **kwargs: Any) -> subprocess.CompletedProcess[str]:
        environment = kwargs["env"]
        assert environment["UL_DBT_USER"] == "urbanlens_dbt"
        assert environment["UL_DBT_PASSWORD"] == "role-placeholder"
        profile = Path(arguments[-1]) / "profiles.yml"
        assert "role-placeholder" not in profile.read_text()
        assert "schema: ul_gold" in profile.read_text()
        calls.append(arguments)
        return subprocess.CompletedProcess(arguments, 0)

    monkeypatch.setattr(subprocess, "run", run)
    assert dbt.debug(tmp_path, "local") == 0
    assert len(calls) == 1


def test_dbt_rejects_owner_connection_in_scoped_setting(tmp_path: Path) -> None:
    (tmp_path / ".env").write_text(
        "URBANLENS_LOCAL_DBT_DATABASE_URL=postgresql://owner:test-placeholder@127.0.0.1/db\n"
    )
    with pytest.raises(ValueError, match="restricted"):
        dbt.debug(tmp_path, "local")

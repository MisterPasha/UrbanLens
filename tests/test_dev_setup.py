"""A fresh checkout can configure disposable local services without cloud secrets."""

from pathlib import Path

import pytest
from dotenv import dotenv_values

from scripts import dev


def test_fresh_setup_generates_local_password_without_cloud_credentials(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    template = (dev.ROOT / ".env.example").read_text(encoding="utf-8")
    (tmp_path / ".env.example").write_text(template, encoding="utf-8")
    monkeypatch.setattr(dev, "ROOT", tmp_path)
    dev.initialise_env()
    values = dotenv_values(tmp_path / ".env")
    assert len(values["LOCAL_POSTGRES_PASSWORD"] or "") >= 24
    assert values["LOCAL_POSTGRES_HOST"] == "127.0.0.1"
    assert values["LOCAL_POSTGRES_DB"] == "urbanlens"
    assert not values["COMPANIES_HOUSE_API_KEY"]
    assert not values["URBANLENS_PIPELINE_DATABASE_URL"]
    original = (tmp_path / ".env").read_bytes()
    dev.initialise_env()
    assert (tmp_path / ".env").read_bytes() == original


def test_setup_preserves_existing_private_configuration_without_a_template(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    path = tmp_path / ".env"
    original = b"LOCAL_POSTGRES_PASSWORD=existing-test-placeholder\n"
    path.write_bytes(original)
    monkeypatch.setattr(dev, "ROOT", tmp_path)
    dev.initialise_env()
    assert path.read_bytes() == original

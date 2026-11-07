"""Nonsecret application settings; source and database credentials arrive separately."""

from pathlib import Path
from typing import Literal

from pydantic import Field, SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_prefix="URBANLENS_", env_file=".env", extra="ignore", frozen=True
    )

    environment: Literal["local", "test", "production"] = "local"
    api_database_url: SecretStr | None = Field(default=None, repr=False)
    local_api_database_url: SecretStr | None = Field(default=None, repr=False)
    postcode_cache_path: Path = Path(".cache/api-postcodes.sqlite")
    api_requests_per_minute: int = Field(default=120, ge=1, le=1000)

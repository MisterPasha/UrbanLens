"""Nonsecret application settings; source and database credentials arrive separately."""

from typing import Literal

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_prefix="URBANLENS_", env_file=".env", extra="ignore", frozen=True
    )

    environment: Literal["local", "test", "production"] = "local"

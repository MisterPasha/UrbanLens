"""Production API entry point; retrieve only the read-only database login at cold start."""

from __future__ import annotations

import os
from typing import Any

from mangum import Mangum
from pydantic import SecretStr

from urbanlens.api.app import create_app
from urbanlens.config import Settings

_adapter: Mangum | None = None


def handler(event: dict[str, Any], context: Any) -> dict[str, Any]:
    global _adapter
    if _adapter is None:
        import boto3

        # Credentials never become command arguments, image layers or log messages.
        secret = boto3.client("ssm").get_parameter(
            Name=os.environ["API_DATABASE_PARAMETER"], WithDecryption=True
        )["Parameter"]["Value"]
        _adapter = Mangum(
            create_app(Settings(environment="production", api_database_url=SecretStr(secret))),
            lifespan="off",
        )
    return dict(_adapter(event, context))

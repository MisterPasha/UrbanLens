"""Liveness must keep working without provider/database credentials or network access."""

import asyncio
from unittest.mock import patch

import httpx

from urbanlens.api.app import create_app


def test_health_never_connects_to_database_or_discloses_credentials() -> None:
    async def request_health() -> httpx.Response:
        transport = httpx.ASGITransport(app=create_app())
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
            return await client.get("/health")

    with patch("psycopg.connect", side_effect=AssertionError("health must not connect")):
        response = asyncio.run(request_health())
    assert response.status_code == 200
    assert response.json() == {"status": "ok", "service": "urbanlens-api"}

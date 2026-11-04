"""Minimal API entry point. Health never opens a database connection."""

from typing import Literal

from fastapi import FastAPI
from pydantic import BaseModel

from urbanlens import __version__


class HealthResponse(BaseModel):
    status: Literal["ok"] = "ok"
    service: Literal["urbanlens-api"] = "urbanlens-api"


def create_app() -> FastAPI:
    app = FastAPI(title="UrbanLens API", version=__version__)

    @app.get("/health", response_model=HealthResponse)
    def health() -> HealthResponse:
        """Report process liveness, not database readiness or data availability."""
        return HealthResponse()

    return app


app = create_app()

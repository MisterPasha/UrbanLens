"""Only curated, release-bound SQL is reachable using the read-only API login."""

from __future__ import annotations

from typing import Any
from uuid import UUID

import psycopg
from psycopg.conninfo import conninfo_to_dict
from psycopg.types.json import Jsonb

from urbanlens.config import Settings


class ApiError(Exception):
    def __init__(self, status: int, code: str, message: str, *, refresh: bool = False) -> None:
        self.status, self.code, self.message, self.refresh = status, code, message, refresh


class PublishedRepository:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings

    def read(
        self, operation: str, release_id: UUID | None, filters: dict[str, Any] | None = None
    ) -> dict[str, Any]:
        value = (
            self.settings.local_api_database_url if self.settings.environment == "local" else None
        ) or self.settings.api_database_url
        if value is None:
            raise ApiError(503, "data_unavailable", "Published data is temporarily unavailable")
        dsn = value.get_secret_value()
        if conninfo_to_dict(dsn).get("user") != "urbanlens_api":
            raise ApiError(
                503, "database_role_misconfigured", "The API database login is not configured"
            )
        try:
            with psycopg.connect(dsn, connect_timeout=3, prepare_threshold=None) as c:
                c.execute("SET TRANSACTION READ ONLY")
                c.execute(
                    "SELECT"
                    " set_config('statement_timeout','5000',true),set_config('lock_tim"
                    "eout','2000',true)"
                )
                row = c.execute(
                    "SELECT ul_api.read_release(%s,%s,%s)",
                    (release_id, operation, Jsonb(filters or {})),
                ).fetchone()
                if row is None:
                    raise ApiError(
                        503, "data_unavailable", "Published data is temporarily unavailable"
                    )
                return dict(row[0])
        except psycopg.Error as error:
            mapping = {
                "UL410": (410, "release_expired", "This release has expired; refresh metadata"),
                "UL503": (503, "data_unavailable", "No validated release is available"),
                "UL404": (404, "venue_not_found", "The venue is not available in this release"),
                "UL405": (404, "company_not_found", "The company is outside the supported cohort"),
                "22023": (422, "unsupported_scope", "The filters or location are unsupported"),
                "57014": (503, "query_timeout", "The query timed out; try again"),
                "55P03": (503, "data_busy", "Data is temporarily busy; try again"),
            }
            status, code, message = mapping.get(
                error.sqlstate or "",
                (503, "data_unavailable", "Published data is temporarily unavailable"),
            )
            raise ApiError(status, code, message, refresh=status == 410) from None

"""Versioned, release-pinned public reads; health never wakes the database."""

from __future__ import annotations

import base64
import hashlib
import json
import logging
import math
import re
import tempfile
import threading
import time
from collections import OrderedDict
from pathlib import Path
from typing import Annotated, Any, Literal
from uuid import UUID, uuid4

from fastapi import FastAPI, Query, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from starlette.exceptions import HTTPException

from urbanlens import __version__
from urbanlens.api.contracts import (
    ActivityPage,
    ActivityQuery,
    Area,
    AreaSummary,
    CompaniesPage,
    CompaniesQuery,
    CompanyDetail,
    CompareQuery,
    Comparison,
    Envelope,
    ErrorResponse,
    Exploration,
    ExploreQuery,
    Meta,
    PinQuery,
    Postcode,
    SummaryQuery,
    VenueDetail,
)
from urbanlens.api.postcodes import Postcodes
from urbanlens.api.repository import ApiError, PublishedRepository
from urbanlens.config import Settings
from urbanlens.ingestion.postcodes import POSTCODE, normalize

logger = logging.getLogger("urbanlens.api")
if not logger.handlers:
    logger.addHandler(logging.StreamHandler())
logger.setLevel(logging.INFO)
logger.propagate = False


class HealthResponse(BaseModel):
    status: Literal["ok"] = "ok"
    service: Literal["urbanlens-api"] = "urbanlens-api"


def encode_cursor(identifier: UUID, filters: str, last: str) -> str:
    return (
        base64.urlsafe_b64encode(
            json.dumps(
                dict(v=1, release_id=str(identifier), filters=filters, last=last),
                separators=(",", ":"),
            ).encode()
        )
        .decode()
        .rstrip("=")
    )


def create_app(
    settings: Settings | None = None,
    repository: PublishedRepository | None = None,
    postcode_service: Postcodes | None = None,
) -> FastAPI:
    config = settings or Settings()
    repo = repository or PublishedRepository(config)
    cache_path = config.postcode_cache_path
    if config.environment == "production" and cache_path == Path(".cache/api-postcodes.sqlite"):
        cache_path = Path(tempfile.gettempdir()) / "urbanlens-api-postcodes.sqlite"
    postcodes = postcode_service or Postcodes(cache_path)
    app = FastAPI(
        title="UrbanLens API",
        version=__version__,
        responses={n: {"model": ErrorResponse} for n in (404, 410, 422, 429, 503)},
    )
    clients: OrderedDict[str, list[float]] = OrderedDict()
    throttle = threading.Lock()

    def error_response(request: Request, error: ApiError) -> JSONResponse:
        return JSONResponse(
            status_code=error.status,
            content={
                "error": dict(
                    code=error.code,
                    message=error.message,
                    request_id=getattr(request.state, "request_id", ""),
                    refresh_required=error.refresh,
                )
            },
            headers={
                "X-Error-Code": error.code,
                **({"Retry-After": "1"} if error.status in (429, 503) else {}),
            },
        )

    @app.middleware("http")
    async def request_context(request: Request, call_next: Any) -> Any:
        supplied = request.headers.get("X-Request-ID", "")
        identifier = supplied if re.fullmatch(r"[A-Za-z0-9_-]{1,64}", supplied) else uuid4().hex
        request.state.request_id = identifier
        started = time.monotonic()
        host = request.client.host if request.client else "unknown"
        limited = False
        retry_after = 1
        if request.url.path.startswith("/api/"):
            with throttle:
                stamps = [s for s in clients.pop(host, []) if started - s < 60]
                limited = len(stamps) >= config.api_requests_per_minute
                if limited:
                    retry_after = max(1, math.ceil(60 - (started - stamps[0])))
                if not limited:
                    stamps.append(started)
                clients[host] = stamps
                while len(clients) > 2048:
                    clients.popitem(last=False)
        try:
            response = (
                error_response(
                    request, ApiError(429, "rate_limited", "Too many requests; try again shortly")
                )
                if limited
                else await call_next(request)
            )
        except Exception:
            response = error_response(
                request,
                ApiError(503, "service_unavailable", "The service is temporarily unavailable"),
            )
        response.headers["X-Request-ID"] = identifier
        if limited:
            response.headers["Retry-After"] = str(retry_after)
        response.headers["Cache-Control"] = "no-store"
        logger.info(
            json.dumps(
                dict(
                    request_id=identifier,
                    method=request.method,
                    route=getattr(request.scope.get("route"), "path", "unmatched"),
                    status=response.status_code,
                    error_code=response.headers.get("X-Error-Code"),
                    duration_ms=round((time.monotonic() - started) * 1000, 2),
                )
            )
        )
        return response

    @app.exception_handler(ApiError)
    async def api_error(request: Request, error: ApiError) -> JSONResponse:
        return error_response(request, error)

    @app.exception_handler(RequestValidationError)
    async def invalid(request: Request, error: RequestValidationError) -> JSONResponse:
        return error_response(
            request,
            ApiError(422, "invalid_request", "Check the request identifiers, filters and limits"),
        )

    @app.exception_handler(HTTPException)
    async def http_error(request: Request, error: HTTPException) -> JSONResponse:
        return error_response(
            request,
            ApiError(error.status_code, "http_error", "The requested operation is unavailable"),
        )

    def read(
        request: Request, operation: str, pin: UUID | None, filters: dict[str, Any] | None = None
    ) -> dict[str, Any]:
        result = repo.read(operation, pin, filters)
        result["request_id"] = request.state.request_id
        return result

    @app.get("/health", response_model=HealthResponse)
    def health() -> HealthResponse:
        return HealthResponse()

    @app.get("/api/v1/meta", response_model=Envelope[Meta])
    def meta(request: Request, query: Annotated[PinQuery, Query()]) -> dict[str, Any]:
        return read(request, "meta", query.release_id)

    @app.get("/api/v1/areas", response_model=Envelope[list[Area]])
    def areas(request: Request, query: Annotated[PinQuery, Query()]) -> dict[str, Any]:
        return read(request, "areas", query.release_id)

    @app.get("/api/v1/areas/{code}/summary", response_model=Envelope[AreaSummary])
    def area_summary(
        code: str, request: Request, query: Annotated[SummaryQuery, Query()]
    ) -> dict[str, Any]:
        if not re.fullmatch(r"E01[0-9]{6}", code):
            raise ApiError(422, "invalid_area", "Choose a supported LSOA code")
        filters = query.model_dump(
            mode="json", exclude={"release_id", "empty_categories"}, exclude_none=True
        )
        return read(request, "summary", query.release_id, dict(filters, area_code=code))

    @app.get("/api/v1/compare", response_model=Envelope[Comparison])
    def compare(request: Request, query: Annotated[CompareQuery, Query()]) -> dict[str, Any]:
        filters = query.model_dump(
            mode="json", exclude={"release_id", "empty_categories"}, exclude_none=True
        )
        return read(request, "compare", query.release_id, filters)

    def context_page(request: Request, operation: str, query: Any) -> dict[str, Any]:
        filters = query.model_dump(mode="json", exclude={"release_id", "cursor"}, exclude_none=True)
        semantic = {k: v for k, v in filters.items() if k != "limit"}
        digest = hashlib.sha256(
            json.dumps([operation, semantic], sort_keys=True).encode()
        ).hexdigest()
        pin = query.release_id
        if query.cursor:
            try:
                cursor = json.loads(
                    base64.urlsafe_b64decode(query.cursor + "=" * (-len(query.cursor) % 4))
                )
                cursor_release = UUID(cursor["release_id"])
                if (
                    cursor["v"] != 1
                    or cursor["filters"] != digest
                    or (pin is not None and pin != cursor_release)
                ):
                    raise ValueError("Cursor context differs")
                last = cursor["last"]
                if operation == "companies":
                    if not re.fullmatch(r"[A-Z0-9]{8}", last):
                        raise ValueError("Invalid number")
                    filters["after_id"] = last
                else:
                    from datetime import datetime

                    observed, identifier = json.loads(last)
                    stamp = datetime.fromisoformat(observed)
                    if stamp.tzinfo is None:
                        raise ValueError("Timestamp needs a timezone")
                    filters.update(after_time=stamp.isoformat(), after_id=str(UUID(identifier)))
                pin = cursor_release
            except (ValueError, KeyError, TypeError):
                raise ApiError(
                    422, "invalid_cursor", "Use the cursor with its original release and filters"
                ) from None
        result = read(request, operation, pin, filters)
        data = result["data"]
        if data["has_more"] and data["items"]:
            item = data["items"][-1]
            last = (
                item["company_number"]
                if operation == "companies"
                else json.dumps([item["observed_at"], item["event_id"]])
            )
            data["next_cursor"] = encode_cursor(UUID(result["release_id"]), digest, last)
        return result

    @app.get("/api/v1/companies", response_model=Envelope[CompaniesPage])
    def companies(request: Request, query: Annotated[CompaniesQuery, Query()]) -> dict[str, Any]:
        return context_page(request, "companies", query)

    @app.get("/api/v1/companies/{number}", response_model=Envelope[CompanyDetail])
    def company(
        number: str, request: Request, query: Annotated[PinQuery, Query()]
    ) -> dict[str, Any]:
        if not re.fullmatch(r"[A-Z0-9]{8}", number):
            raise ApiError(422, "invalid_company_number", "Use an eight-character company number")
        return read(request, "company", query.release_id, dict(company_number=number))

    @app.get("/api/v1/activity", response_model=Envelope[ActivityPage])
    def activity(request: Request, query: Annotated[ActivityQuery, Query()]) -> dict[str, Any]:
        return context_page(request, "activity", query)

    @app.get("/api/v1/establishments", response_model=Envelope[Exploration])
    def establishments(request: Request, query: Annotated[ExploreQuery, Query()]) -> dict[str, Any]:
        filters = query.model_dump(
            mode="json", exclude={"release_id", "cursor", "empty_categories"}, exclude_none=True
        )
        semantic = {k: v for k, v in filters.items() if k not in {"limit", "map_limit"}}
        digest = hashlib.sha256(
            json.dumps(semantic, sort_keys=True, separators=(",", ":")).encode()
        ).hexdigest()
        pin = query.release_id
        if query.cursor:
            try:
                cursor = json.loads(
                    base64.urlsafe_b64decode(query.cursor + "=" * (-len(query.cursor) % 4))
                )
                cursor_release = UUID(cursor["release_id"])
                if (
                    cursor["v"] != 1
                    or cursor["filters"] != digest
                    or (pin is not None and pin != cursor_release)
                ):
                    raise ValueError("Cursor context differs")
                filters["after_id"] = str(UUID(cursor["last"]))
                pin = cursor_release
            except (ValueError, KeyError, TypeError):
                raise ApiError(
                    422, "invalid_cursor", "Use the cursor with its original release and filters"
                ) from None
        result = read(request, "establishments", pin, filters)
        data = result["data"]
        if data["has_more"] and data["items"]:
            data["next_cursor"] = encode_cursor(
                UUID(result["release_id"]), digest, data["items"][-1]["establishment_id"]
            )
        return result

    @app.get("/api/v1/establishments/{establishment_id}", response_model=Envelope[VenueDetail])
    def detail(
        establishment_id: UUID, request: Request, query: Annotated[PinQuery, Query()]
    ) -> dict[str, Any]:
        return read(
            request, "detail", query.release_id, dict(establishment_id=str(establishment_id))
        )

    @app.get("/api/v1/postcodes/{postcode}", response_model=Envelope[Postcode])
    def postcode_lookup(
        postcode: str, request: Request, query: Annotated[PinQuery, Query()]
    ) -> dict[str, Any]:
        key = normalize(postcode)
        if not POSTCODE.fullmatch(key):
            raise ApiError(422, "invalid_postcode", "Enter a valid full UK postcode")
        result = read(request, "postcode", query.release_id, dict(postcode=key))
        cached = result["data"]
        if cached:
            value = dict(
                postcode=key,
                state=cached["state"].replace("-", "_"),
                longitude=cached.get("longitude"),
                latitude=cached.get("latitude"),
                area_code=cached.get("area_code"),
                cache_hit=True,
                fetched_at=cached["observed_at"],
                expires_at=cached["expires_at"],
                supported_area_code=cached.get("supported_area_code"),
                authority=cached.get("authority_code"),
            )
        else:
            value = postcodes.lookup(key)
            if value["state"] == "resolved":
                membership = read(request, "postcode_check", UUID(result["release_id"]), value)
                value["supported_area_code"] = membership["data"].get("supported_area_code")
        supported = value.pop("supported_area_code", None)
        authority = value.pop("authority", None)
        if value["state"] == "resolved":
            if authority != "E06000043" or supported is None:
                value["state"] = "outside"
            elif value.get("area_code") != supported:
                value["state"] = "incompatible_geography"
            else:
                value.update(can_define_search=True, area_code=supported)
        result["data"] = Postcode.model_validate(value).model_dump(mode="json")
        return result

    return app


app = create_app()

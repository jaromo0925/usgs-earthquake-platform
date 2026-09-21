import time
from contextlib import asynccontextmanager
from datetime import UTC, datetime
from typing import Literal

from fastapi import FastAPI, HTTPException, Query, Request, Response
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest

from app.config import get_settings
from app.database import MongoDatabase
from app.logging_config import configure_logging
from app.observability import API_DURATION, API_REQUESTS
from app.repositories import EarthquakeRepository, MetricRepository, ReportRepository

settings = get_settings()
configure_logging(settings.log_level)
database = MongoDatabase(settings)


@asynccontextmanager
async def lifespan(_: FastAPI):
    await database.connect()
    await database.create_indexes()
    yield
    await database.close()


app = FastAPI(
    title=settings.app_name,
    version="1.0.0",
    description="API para consulta de eventos sísmicos, métricas y reportes horarios.",
    lifespan=lifespan,
)


@app.middleware("http")
async def observe_requests(request: Request, call_next):
    started = time.perf_counter()
    status = 500
    try:
        response = await call_next(request)
        status = response.status_code
        return response
    finally:
        path = request.url.path
        API_REQUESTS.labels(request.method, path, str(status)).inc()
        API_DURATION.labels(request.method, path).observe(time.perf_counter() - started)


def get_db():
    if database.db is None:
        raise HTTPException(status_code=503, detail="Database connection is not ready")
    return database.db


def as_utc(value: datetime | None) -> datetime | None:
    if value is None:
        return None
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


@app.get("/", tags=["system"])
async def root():
    return {
        "service": settings.app_name,
        "version": "1.0.0",
        "documentation": "/docs",
    }


@app.get("/health/live", tags=["system"])
async def liveness():
    return {"status": "ok"}


@app.get("/health/ready", tags=["system"])
async def readiness():
    if not await database.healthcheck():
        raise HTTPException(status_code=503, detail="MongoDB is not available")
    return {"status": "ready"}


@app.get("/observability/metrics", include_in_schema=False)
async def prometheus_metrics():
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)


@app.get("/earthquakes", tags=["earthquakes"])
async def list_earthquakes(
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=100),
    min_magnitude: float | None = Query(None, ge=-2, le=12),
    max_magnitude: float | None = Query(None, ge=-2, le=12),
    start_time: datetime | None = None,
    end_time: datetime | None = None,
    location: str | None = Query(None, min_length=2, max_length=100),
    sort_by: Literal["event_time", "magnitude"] = "event_time",
    sort_order: Literal["asc", "desc"] = "desc",
):
    if min_magnitude is not None and max_magnitude is not None and min_magnitude > max_magnitude:
        raise HTTPException(status_code=422, detail="min_magnitude cannot exceed max_magnitude")
    start_time = as_utc(start_time)
    end_time = as_utc(end_time)
    if start_time is not None and end_time is not None and start_time >= end_time:
        raise HTTPException(status_code=422, detail="start_time must be earlier than end_time")
    repository = EarthquakeRepository(get_db())
    return await repository.list(
        page=page,
        page_size=page_size,
        min_magnitude=min_magnitude,
        max_magnitude=max_magnitude,
        start_time=start_time,
        end_time=end_time,
        location=location,
        sort_by=sort_by,
        sort_order=sort_order,
    )


@app.get("/metrics", tags=["metrics"])
async def list_metrics(
    page: int = Query(1, ge=1), page_size: int = Query(20, ge=1, le=100)
):
    return await MetricRepository(get_db()).list(page, page_size)


@app.get("/reports", tags=["reports"])
async def list_reports(
    page: int = Query(1, ge=1), page_size: int = Query(20, ge=1, le=100)
):
    return await ReportRepository(get_db()).list(page, page_size)

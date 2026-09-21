import asyncio
import logging
from datetime import UTC, datetime
from typing import Any

import httpx
from pydantic import ValidationError

from app.config import Settings
from app.models import EarthquakeCreate

logger = logging.getLogger(__name__)


class UsgsClientError(RuntimeError):
    pass


class UsgsClient:
    def __init__(self, settings: Settings) -> None:
        self._url = settings.usgs_url
        self._timeout = settings.usgs_timeout_seconds
        self._max_retries = settings.usgs_max_retries

    async def fetch_events(self) -> list[EarthquakeCreate]:
        last_error: Exception | None = None
        async with httpx.AsyncClient(timeout=self._timeout) as client:
            for attempt in range(1, self._max_retries + 1):
                try:
                    response = await client.get(
                        self._url,
                        headers={
                            "Accept": "application/geo+json",
                            "User-Agent": "quipux-tech-test/1.0",
                        },
                    )
                    response.raise_for_status()
                    data = response.json()
                    features = data.get("features")
                    if not isinstance(features, list):
                        raise UsgsClientError("USGS response does not contain a features list")
                    return self._parse_features(features)
                except (httpx.HTTPError, ValueError, UsgsClientError) as exc:
                    last_error = exc
                    logger.warning(
                        "usgs_request_failed",
                        extra={
                            "attempt": attempt,
                            "max_retries": self._max_retries,
                            "error": str(exc),
                        },
                    )
                    if attempt < self._max_retries:
                        await asyncio.sleep(min(2 ** (attempt - 1), 8))
        raise UsgsClientError(f"USGS request failed after retries: {last_error}")

    def _parse_features(self, features: list[dict[str, Any]]) -> list[EarthquakeCreate]:
        parsed: list[EarthquakeCreate] = []
        for feature in features:
            try:
                properties = feature.get("properties") or {}
                geometry = feature.get("geometry") or {}
                coordinates = geometry.get("coordinates") or []
                if len(coordinates) < 3 or properties.get("mag") is None:
                    raise ValueError("missing magnitude or coordinates")
                parsed.append(
                    EarthquakeCreate(
                        event_id=feature["id"],
                        magnitude=properties["mag"],
                        location=properties.get("place") or "Unknown location",
                        longitude=coordinates[0],
                        latitude=coordinates[1],
                        depth=coordinates[2],
                        event_time=datetime.fromtimestamp(properties["time"] / 1000, tz=UTC),
                        source_url=properties.get("url"),
                    )
                )
            except (KeyError, TypeError, ValueError, ValidationError) as exc:
                logger.warning(
                    "usgs_feature_discarded",
                    extra={"event_id": feature.get("id"), "error": str(exc)},
                )
        return parsed

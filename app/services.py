import logging
from datetime import datetime, timedelta

from app.observability import (
    EVENTS_DUPLICATED,
    EVENTS_FAILED,
    EVENTS_INSERTED,
    EVENTS_RECEIVED,
    INGESTION_DURATION,
    INGESTION_RUNS,
    PENDING_EVENTS,
)
from app.repositories import EarthquakeRepository, MetricRepository
from app.usgs_client import UsgsClient

logger = logging.getLogger(__name__)


def hour_window(value: datetime) -> tuple[datetime, datetime]:
    start = value.replace(minute=0, second=0, microsecond=0)
    return start, start + timedelta(hours=1)


class ProcessingService:
    def __init__(
        self, earthquakes: EarthquakeRepository, metrics: MetricRepository
    ) -> None:
        self._earthquakes = earthquakes
        self._metrics = metrics

    async def process(self, event: dict) -> None:
        start, end = hour_window(event["event_time"])
        await self._metrics.recompute_window(start, end)
        await self._earthquakes.mark_processed(event["event_id"])
        logger.info(
            "earthquake_metrics_updated",
            extra={"event_id": event["event_id"], "window_start": start.isoformat()},
        )

    async def retry_pending(self, limit: int = 500) -> int:
        pending = await self._earthquakes.pending(limit=limit)
        PENDING_EVENTS.set(len(pending))
        processed = 0
        for event in pending:
            try:
                await self.process(event)
                processed += 1
            except Exception:
                EVENTS_FAILED.inc()
                logger.exception(
                    "pending_event_processing_failed", extra={"event_id": event.get("event_id")}
                )
        return processed


class IngestionService:
    def __init__(
        self,
        client: UsgsClient,
        earthquakes: EarthquakeRepository,
        processor: ProcessingService,
    ) -> None:
        self._client = client
        self._earthquakes = earthquakes
        self._processor = processor

    async def run_once(self) -> dict[str, int]:
        inserted = duplicates = failed = 0
        with INGESTION_DURATION.time():
            try:
                events = await self._client.fetch_events()
                EVENTS_RECEIVED.inc(len(events))
                for event in events:
                    try:
                        is_new = await self._earthquakes.insert_if_new(event)
                        if not is_new:
                            duplicates += 1
                            EVENTS_DUPLICATED.inc()
                            continue
                        inserted += 1
                        EVENTS_INSERTED.inc()
                        await self._processor.process(event.model_dump())
                    except Exception:
                        failed += 1
                        EVENTS_FAILED.inc()
                        logger.exception(
                            "earthquake_ingestion_failed", extra={"event_id": event.event_id}
                        )
                recovered = await self._processor.retry_pending()
                INGESTION_RUNS.labels(status="success").inc()
                result = {
                    "received": len(events),
                    "inserted": inserted,
                    "duplicates": duplicates,
                    "failed": failed,
                    "recovered": recovered,
                }
                logger.info("ingestion_completed", extra=result)
                return result
            except Exception:
                INGESTION_RUNS.labels(status="failure").inc()
                logger.exception("ingestion_run_failed")
                raise

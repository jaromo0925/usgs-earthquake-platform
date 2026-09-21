import asyncio
import logging

from prometheus_client import start_http_server

from app.config import get_settings
from app.database import MongoDatabase
from app.logging_config import configure_logging
from app.repositories import EarthquakeRepository, MetricRepository
from app.services import IngestionService, ProcessingService
from app.usgs_client import UsgsClient


async def run() -> None:
    settings = get_settings()
    configure_logging(settings.log_level)
    logger = logging.getLogger(__name__)
    database = MongoDatabase(settings)
    await database.connect()
    await database.create_indexes()
    start_http_server(9101)

    if database.db is None:
        raise RuntimeError("MongoDB was not initialized")
    earthquakes = EarthquakeRepository(database.db)
    metrics = MetricRepository(database.db)
    processor = ProcessingService(earthquakes, metrics)
    ingestion = IngestionService(UsgsClient(settings), earthquakes, processor)

    logger.info(
        "ingestion_worker_started",
        extra={"interval_seconds": settings.ingestion_interval_seconds},
    )
    try:
        while True:
            try:
                await ingestion.run_once()
            except Exception:
                logger.exception("ingestion_iteration_failed")
            await asyncio.sleep(settings.ingestion_interval_seconds)
    finally:
        await database.close()


if __name__ == "__main__":
    asyncio.run(run())


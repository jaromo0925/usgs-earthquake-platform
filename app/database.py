import logging
from collections.abc import AsyncIterator

from motor.motor_asyncio import AsyncIOMotorClient, AsyncIOMotorDatabase

from app.config import Settings

logger = logging.getLogger(__name__)


class MongoDatabase:
    def __init__(self, settings: Settings) -> None:
        self._settings = settings
        self.client: AsyncIOMotorClient | None = None
        self.db: AsyncIOMotorDatabase | None = None

    async def connect(self) -> None:
        self.client = AsyncIOMotorClient(
            self._settings.mongodb_uri,
            maxPoolSize=50,
            minPoolSize=2,
            serverSelectionTimeoutMS=5000,
            tz_aware=True,
        )
        self.db = self.client[self._settings.mongodb_database]
        await self.client.admin.command("ping")
        logger.info("mongodb_connected", extra={"database": self._settings.mongodb_database})

    async def close(self) -> None:
        if self.client:
            self.client.close()
            logger.info("mongodb_connection_closed")

    async def create_indexes(self) -> None:
        if self.db is None:
            raise RuntimeError("Database is not connected")
        await self.db.earthquakes.create_index("event_id", unique=True, name="uq_event_id")
        await self.db.earthquakes.create_index(
            [("event_time", -1), ("magnitude", -1)], name="ix_event_time_magnitude"
        )
        await self.db.earthquakes.create_index(
            [("metrics_processed", 1), ("event_time", 1)],
            name="ix_pending_metrics",
            partialFilterExpression={"metrics_processed": False},
        )
        await self.db.metrics.create_index("window_start", unique=True, name="uq_metric_window")
        await self.db.hourly_reports.create_index(
            "report_date", unique=True, name="uq_report_date"
        )
        logger.info("mongodb_indexes_ready")

    async def healthcheck(self) -> bool:
        if self.client is None:
            return False
        try:
            await self.client.admin.command("ping")
            return True
        except Exception:
            return False


async def require_database(database: MongoDatabase) -> AsyncIterator[AsyncIOMotorDatabase]:
    if database.db is None:
        raise RuntimeError("Database is not connected")
    yield database.db

import math
import re
from datetime import UTC, datetime
from typing import Any, Literal

from motor.motor_asyncio import AsyncIOMotorDatabase

from app.models import EarthquakeCreate, magnitude_bucket


def serialize_document(document: dict[str, Any]) -> dict[str, Any]:
    document = dict(document)
    document.pop("_id", None)
    return document


class EarthquakeRepository:
    def __init__(self, database: AsyncIOMotorDatabase) -> None:
        self.collection = database.earthquakes

    async def insert_if_new(self, event: EarthquakeCreate) -> bool:
        now = datetime.now(UTC)
        document = event.model_dump()
        document.update(
            {
                "magnitude_range": magnitude_bucket(event.magnitude).value,
                "ingested_at": now,
                "metrics_processed": False,
            }
        )
        result = await self.collection.update_one(
            {"event_id": event.event_id}, {"$setOnInsert": document}, upsert=True
        )
        return result.upserted_id is not None

    async def mark_processed(self, event_id: str) -> None:
        await self.collection.update_one(
            {"event_id": event_id},
            {"$set": {"metrics_processed": True, "metrics_processed_at": datetime.now(UTC)}},
        )

    async def pending(self, limit: int = 500) -> list[dict[str, Any]]:
        cursor = (
            self.collection.find({"metrics_processed": False})
            .sort("event_time", 1)
            .limit(limit)
        )
        return [serialize_document(item) async for item in cursor]

    async def list(
        self,
        *,
        page: int,
        page_size: int,
        min_magnitude: float | None,
        max_magnitude: float | None,
        start_time: datetime | None,
        end_time: datetime | None,
        location: str | None,
        sort_by: Literal["event_time", "magnitude"],
        sort_order: Literal["asc", "desc"],
    ) -> dict[str, Any]:
        query: dict[str, Any] = {}
        if min_magnitude is not None or max_magnitude is not None:
            query["magnitude"] = {}
            if min_magnitude is not None:
                query["magnitude"]["$gte"] = min_magnitude
            if max_magnitude is not None:
                query["magnitude"]["$lte"] = max_magnitude
        if start_time is not None or end_time is not None:
            query["event_time"] = {}
            if start_time is not None:
                query["event_time"]["$gte"] = start_time
            if end_time is not None:
                query["event_time"]["$lt"] = end_time
        if location:
            query["location"] = {"$regex": re.escape(location), "$options": "i"}

        total = await self.collection.count_documents(query)
        direction = 1 if sort_order == "asc" else -1
        cursor = (
            self.collection.find(query)
            .sort([(sort_by, direction), ("event_id", 1)])
            .skip((page - 1) * page_size)
            .limit(page_size)
        )
        items = [serialize_document(item) async for item in cursor]
        return {
            "items": items,
            "page": page,
            "page_size": page_size,
            "total": total,
            "pages": math.ceil(total / page_size) if total else 0,
        }


class MetricRepository:
    def __init__(self, database: AsyncIOMotorDatabase) -> None:
        self.earthquakes = database.earthquakes
        self.metrics = database.metrics

    async def recompute_window(
        self, window_start: datetime, window_end: datetime
    ) -> dict[str, Any]:
        pipeline = [
            {"$match": {"event_time": {"$gte": window_start, "$lt": window_end}}},
            {
                "$group": {
                    "_id": None,
                    "earthquake_count": {"$sum": 1},
                    "avg_magnitude": {"$avg": "$magnitude"},
                    "max_magnitude": {"$max": "$magnitude"},
                    "ranges": {"$push": "$magnitude_range"},
                }
            },
        ]
        rows = await self.earthquakes.aggregate(pipeline).to_list(length=1)
        row = rows[0] if rows else {}
        ranges = row.get("ranges", [])
        distribution = {
            "lt_2": ranges.count("lt_2"),
            "2_to_3_9": ranges.count("2_to_3_9"),
            "4_to_5_9": ranges.count("4_to_5_9"),
            "gte_6": ranges.count("gte_6"),
        }
        document = {
            "window_start": window_start,
            "window_end": window_end,
            "earthquake_count": row.get("earthquake_count", 0),
            "avg_magnitude": row.get("avg_magnitude"),
            "max_magnitude": row.get("max_magnitude"),
            "magnitude_distribution": distribution,
            "updated_at": datetime.now(UTC),
        }
        await self.metrics.update_one(
            {"window_start": window_start}, {"$set": document}, upsert=True
        )
        return document

    async def list(self, page: int, page_size: int) -> dict[str, Any]:
        total = await self.metrics.count_documents({})
        cursor = (
            self.metrics.find({})
            .sort("window_start", -1)
            .skip((page - 1) * page_size)
            .limit(page_size)
        )
        items = [serialize_document(item) async for item in cursor]
        return {
            "items": items,
            "page": page,
            "page_size": page_size,
            "total": total,
            "pages": math.ceil(total / page_size) if total else 0,
        }


class ReportRepository:
    def __init__(self, database: AsyncIOMotorDatabase) -> None:
        self.collection = database.hourly_reports

    async def list(self, page: int, page_size: int) -> dict[str, Any]:
        total = await self.collection.count_documents({})
        cursor = (
            self.collection.find({})
            .sort("report_date", -1)
            .skip((page - 1) * page_size)
            .limit(page_size)
        )
        items = [serialize_document(item) async for item in cursor]
        return {
            "items": items,
            "page": page,
            "page_size": page_size,
            "total": total,
            "pages": math.ceil(total / page_size) if total else 0,
        }

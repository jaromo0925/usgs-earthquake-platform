import logging
import os
from datetime import UTC, datetime

from pymongo import MongoClient

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context

logger = logging.getLogger(__name__)


def build_report(database, window_start: datetime, window_end: datetime) -> dict:
    match = {"event_time": {"$gte": window_start, "$lt": window_end}}
    summary = list(
        database.earthquakes.aggregate(
            [
                {"$match": match},
                {
                    "$group": {
                        "_id": None,
                        "total_events": {"$sum": 1},
                        "average_magnitude": {"$avg": "$magnitude"},
                        "max_magnitude": {"$max": "$magnitude"},
                        "ranges": {"$push": "$magnitude_range"},
                    }
                },
            ]
        )
    )
    top_locations = list(
        database.earthquakes.aggregate(
            [
                {"$match": match},
                {"$group": {"_id": "$location", "events": {"$sum": 1}}},
                {"$sort": {"events": -1, "_id": 1}},
                {"$limit": 3},
            ]
        )
    )
    row = summary[0] if summary else {}
    ranges = row.get("ranges", [])
    return {
        "report_date": window_start,
        "window_end": window_end,
        "total_events": row.get("total_events", 0),
        "average_magnitude": row.get("average_magnitude"),
        "max_magnitude": row.get("max_magnitude"),
        "top_locations": [item["_id"] for item in top_locations],
        "magnitude_distribution": {
            "lt_2": ranges.count("lt_2"),
            "2_to_3_9": ranges.count("2_to_3_9"),
            "4_to_5_9": ranges.count("4_to_5_9"),
            "gte_6": ranges.count("gte_6"),
        },
        "generated_at": datetime.now(UTC),
    }


@dag(
    dag_id="hourly_earthquake_report",
    schedule="@hourly",
    start_date=datetime(2026, 1, 1, tzinfo=UTC),
    catchup=False,
    max_active_runs=1,
    default_args={"owner": "data-engineering", "retries": 2},
    tags=["usgs", "earthquakes", "reporting"],
)
def hourly_earthquake_report():
    @task
    def generate_and_persist() -> dict:
        context = get_current_context()
        window_start = context["data_interval_start"].in_timezone("UTC")
        window_end = context["data_interval_end"].in_timezone("UTC")
        client = MongoClient(os.environ["MONGODB_URI"], tz_aware=True)
        try:
            database = client[os.getenv("MONGODB_DATABASE", "earthquakes")]
            report = build_report(database, window_start, window_end)
            database.hourly_reports.create_index("report_date", unique=True)
            database.hourly_reports.update_one(
                {"report_date": window_start}, {"$set": report}, upsert=True
            )
            logger.info(
                "hourly_report_persisted",
                extra={
                    "window_start": window_start.isoformat(),
                    "total_events": report["total_events"],
                },
            )
            return {
                "report_date": window_start.isoformat(),
                "total_events": report["total_events"],
            }
        finally:
            client.close()

    generate_and_persist()


hourly_earthquake_report()

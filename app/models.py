from datetime import UTC, datetime
from enum import Enum

from pydantic import BaseModel, ConfigDict, Field, field_validator


class MagnitudeRange(str, Enum):
    MICRO = "lt_2"
    MINOR = "2_to_3_9"
    MODERATE = "4_to_5_9"
    STRONG = "gte_6"


def magnitude_bucket(magnitude: float) -> MagnitudeRange:
    if magnitude < 2:
        return MagnitudeRange.MICRO
    if magnitude < 4:
        return MagnitudeRange.MINOR
    if magnitude < 6:
        return MagnitudeRange.MODERATE
    return MagnitudeRange.STRONG


class EarthquakeCreate(BaseModel):
    event_id: str = Field(min_length=1, max_length=120)
    magnitude: float = Field(ge=-2.0, le=12.0)
    location: str = Field(min_length=1, max_length=500)
    latitude: float = Field(ge=-90, le=90)
    longitude: float = Field(ge=-180, le=180)
    depth: float = Field(ge=-20, le=1000)
    event_time: datetime
    source: str = "USGS"
    source_url: str | None = None

    @field_validator("event_time")
    @classmethod
    def ensure_utc(cls, value: datetime) -> datetime:
        if value.tzinfo is None:
            return value.replace(tzinfo=UTC)
        return value.astimezone(UTC)


class Earthquake(EarthquakeCreate):
    model_config = ConfigDict(from_attributes=True)

    magnitude_range: MagnitudeRange
    ingested_at: datetime
    metrics_processed: bool = False


class Metric(BaseModel):
    window_start: datetime
    window_end: datetime
    earthquake_count: int = 0
    avg_magnitude: float | None = None
    max_magnitude: float | None = None
    magnitude_distribution: dict[str, int] = Field(default_factory=dict)
    updated_at: datetime


class HourlyReport(BaseModel):
    report_date: datetime
    window_end: datetime
    total_events: int = 0
    average_magnitude: float | None = None
    max_magnitude: float | None = None
    top_locations: list[str] = Field(default_factory=list)
    magnitude_distribution: dict[str, int] = Field(default_factory=dict)
    generated_at: datetime


class PaginatedResponse(BaseModel):
    items: list[dict]
    page: int
    page_size: int
    total: int
    pages: int


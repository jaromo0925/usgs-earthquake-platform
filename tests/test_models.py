from datetime import UTC, datetime

import pytest
from pydantic import ValidationError

from app.models import EarthquakeCreate, MagnitudeRange, magnitude_bucket


@pytest.mark.parametrize(
    ("magnitude", "expected"),
    [
        (1.9, MagnitudeRange.MICRO),
        (2.0, MagnitudeRange.MINOR),
        (3.9, MagnitudeRange.MINOR),
        (4.0, MagnitudeRange.MODERATE),
        (5.9, MagnitudeRange.MODERATE),
        (6.0, MagnitudeRange.STRONG),
    ],
)
def test_magnitude_bucket_boundaries(magnitude, expected):
    assert magnitude_bucket(magnitude) == expected


def test_event_time_is_normalized_to_utc():
    event = EarthquakeCreate(
        event_id="test-1",
        magnitude=3.2,
        location="Medellin",
        latitude=6.25,
        longitude=-75.56,
        depth=10,
        event_time=datetime(2026, 6, 17, 10, 30),
    )
    assert event.event_time.tzinfo == UTC


def test_invalid_coordinates_are_rejected():
    with pytest.raises(ValidationError):
        EarthquakeCreate(
            event_id="test-2",
            magnitude=3.2,
            location="Invalid",
            latitude=100,
            longitude=-75.56,
            depth=10,
            event_time=datetime.now(UTC),
        )


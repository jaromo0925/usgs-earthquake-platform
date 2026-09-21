from datetime import UTC, datetime

from app.services import hour_window


def test_hour_window_uses_half_open_interval():
    start, end = hour_window(datetime(2026, 6, 17, 10, 59, 59, tzinfo=UTC))
    assert start == datetime(2026, 6, 17, 10, 0, tzinfo=UTC)
    assert end == datetime(2026, 6, 17, 11, 0, tzinfo=UTC)


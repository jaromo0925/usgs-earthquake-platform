from app.config import Settings
from app.usgs_client import UsgsClient


def test_parse_valid_feature():
    client = UsgsClient(Settings())
    events = client._parse_features(
        [
            {
                "id": "us7000xxxx",
                "properties": {
                    "mag": 4.2,
                    "place": "20 km NW of California",
                    "time": 1781692200000,
                    "url": "https://example.test/event",
                },
                "geometry": {"coordinates": [-120.12, 35.44, 10.5]},
            }
        ]
    )
    assert len(events) == 1
    assert events[0].event_id == "us7000xxxx"
    assert events[0].longitude == -120.12
    assert events[0].latitude == 35.44


def test_parse_discards_invalid_feature():
    client = UsgsClient(Settings())
    assert client._parse_features([{"id": "invalid", "properties": {}}]) == []


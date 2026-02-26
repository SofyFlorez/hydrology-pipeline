import pytest
from src.hydrology_pipeline.transform import normalize_station, normalize_reading


def test_normalize_station_missing_id():
    with pytest.raises(ValueError):
        normalize_station({"label": "foo", "lat": 1.0, "long": 2.0})


def test_normalize_station_invalid_latlong():
    with pytest.raises(ValueError):
        normalize_station({"notation": "X", "label": "foo", "lat": "bad", "long": 2.0})


def test_normalize_reading_missing_datetime():
    with pytest.raises(ValueError):
        normalize_reading({"value": 1.0, "quality": "Good"}, "X", "conductivity", "M1")


def test_normalize_reading_invalid_value():
    reading = {"dateTime": "2024-01-01T00:00:00Z", "value": "bad"}
    row = normalize_reading(reading, "X", "conductivity", "M1")
    assert row.value is None

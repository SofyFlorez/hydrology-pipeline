import pytest
from unittest.mock import patch

from src.hydrology_pipeline.extract import (
    resolve_measures_from_station,
    fetch_latest_readings_for_measure,
)

def test_resolve_measures_invalid_param():
    station = {
        "measures": [
            {"@id": "https://example.com/measures/cond-123"},
            {"@id": "https://example.com/measures/do-mgl-456"},
        ]
    }
    with pytest.raises(ValueError):
        resolve_measures_from_station(station, ["conductivity"])  # only one param
    with pytest.raises(ValueError):
        resolve_measures_from_station(station, ["conductivity", "foo"])  # unsupported

def test_resolve_measures_missing_measures():
    station = {}
    with pytest.raises(ValueError):
        resolve_measures_from_station(station, ["conductivity", "dissolved-oxygen"])

def test_fetch_latest_readings_uses_sort_and_limit():
    """Ensure API call uses _sort and respects _limit."""
    fake = {"items": [{"dateTime": "2026-02-26T12:01:12", "value": 1.0, "quality": "Unchecked"}]}

    with patch("src.hydrology_pipeline.extract.get_json", return_value=fake) as mock_get:
        out = fetch_latest_readings_for_measure("MEASURE_ID", limit=10)

    assert len(out) == 1
    _, kwargs = mock_get.call_args
    assert kwargs["params"]["_limit"] == 10
    assert kwargs["params"]["_sort"] == "-dateTime"

def test_fetch_latest_readings_limit_must_be_positive():
    with pytest.raises(ValueError):
        fetch_latest_readings_for_measure("MEASURE_ID", limit=0)

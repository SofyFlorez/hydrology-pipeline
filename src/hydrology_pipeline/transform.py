from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, Optional

logger = logging.getLogger(__name__)

ALLOWED_QUALITY = {"Good", "Estimated", "Suspect", "Unchecked", "Missing"}


@dataclass(frozen=True)
class StationRow:
    station_id: str
    label: str
    lat: float
    long: float
    river_name: Optional[str]
    date_opened: Optional[str]


@dataclass(frozen=True)
class MeasurementRow:
    station_id: str
    observed_property: str
    measure_id: str
    date_time: str  # ISO-8601 string
    value: Optional[float]
    quality: Optional[str]


def normalize_station(item: Dict[str, Any]) -> StationRow:
    """
    Normalize a station payload into a StationRow used by the SQLite schema.

    Raises ValueError if the station identifier or coordinates are missing/invalid.
    """
    station_id = item.get("notation") or item.get("stationGuid") or item.get("@id")
    if not station_id:
        raise ValueError("Station payload missing an identifier (notation/stationGuid/@id)")

    try:
        lat = float(item["lat"])
        long = float(item["long"])
    except KeyError as exc:
        raise ValueError(f"Station payload missing required field: {exc}") from exc
    except (TypeError, ValueError) as exc:
        raise ValueError("Station payload has invalid lat/long") from exc

    return StationRow(
        station_id=str(station_id),
        label=item.get("label", ""),
        lat=lat,
        long=long,
        river_name=item.get("riverName"),
        date_opened=item.get("dateOpened"),
    )

def _to_iso(dt_str: str) -> str:
    """Validate and normalize ISO-8601 timestamps. Raises on invalid values."""
    cleaned = dt_str.replace("Z", "+00:00")
    try:
        datetime.fromisoformat(cleaned)
    except ValueError as exc:
        raise ValueError(f"Invalid datetime format: {dt_str}") from exc
    return cleaned


def normalize_reading(
    reading: Dict[str, Any],
    station_id: str,
    observed_property: str,
    measure_id: str,
) -> MeasurementRow:
    """
    Normalize a reading payload into a MeasurementRow used by the SQLite schema.

    Requires a timestamp (dateTime or date). Converts value to float when possible,
    otherwise stores it as None.
    """
    dt = reading.get("dateTime") or reading.get("date")
    if not dt:
        raise ValueError(f"Reading missing dateTime/date for measure_id={measure_id}")

    raw_val = reading.get("value")
    try:
        value = float(raw_val) if raw_val is not None else None
    except (TypeError, ValueError):
        value = None

    quality = reading.get("quality")
    if quality is not None and quality not in ALLOWED_QUALITY:
        logger.warning("Unexpected quality flag '%s' for measure_id=%s", quality, measure_id)

    return MeasurementRow(
        station_id=station_id,
        observed_property=observed_property,
        measure_id=measure_id,
        date_time=_to_iso(str(dt)),
        value=value,
        quality=quality,
    )
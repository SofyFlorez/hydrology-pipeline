from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, Optional


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
    date_time: str  # ISO string
    value: Optional[float]
    quality: Optional[str]
    completeness: Optional[str]


def normalize_station(item: Dict[str, Any]) -> StationRow:
    station_id = item.get("notation") or item.get("stationGuid") or item.get("@id")
    if not station_id:
        raise ValueError("Station payload missing an identifier (notation/stationGuid/@id)")

    return StationRow(
        station_id=station_id,
        label=item.get("label", ""),
        lat=float(item["lat"]),
        long=float(item["long"]),
        river_name=item.get("riverName"),
        date_opened=item.get("dateOpened"),
    )


def _to_iso(dt_str: str) -> str:
    # Validate and normalize ISO timestamps; keep timezone if present
    cleaned = dt_str.replace("Z", "+00:00")
    try:
        datetime.fromisoformat(cleaned)
        return cleaned
    except Exception:
        # If parsing fails, keep original to avoid crashing on unexpected formats
        return dt_str


def normalize_reading(
    reading: Dict[str, Any],
    station_id: str,
    observed_property: str,
    measure_id: str,
) -> MeasurementRow:
    dt = reading.get("dateTime") or reading.get("date")
    if not dt:
        raise ValueError(f"Reading missing dateTime/date for measure_id={measure_id}")

    raw_val = reading.get("value", None)
    value: Optional[float] = None
    if raw_val is not None:
        try:
            value = float(raw_val)
        except Exception:
            value = None

    return MeasurementRow(
        station_id=station_id,
        observed_property=observed_property,
        measure_id=measure_id,
        date_time=_to_iso(dt),
        value=value,
        quality=reading.get("quality"),
        completeness=reading.get("completeness"),
    )
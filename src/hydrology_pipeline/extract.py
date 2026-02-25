from __future__ import annotations

def fetch_station_by_notation(notation: str, timeout: int = 30) -> Dict[str, Any]:
from typing import Any, Dict, List
import logging
from .api_client import get_json
from .config import BASE_URL

logger = logging.getLogger(__name__)

def fetch_station_by_notation(notation: str, timeout: int = 30) -> Dict[str, Any]:
    """
    Fetch station metadata by station notation.
    Args:
        notation (str): Station notation (e.g., E64999A).
        timeout (int): Timeout for the API request.
    Returns:
        Dict[str, Any]: Station metadata.
    Raises:
        ValueError: If no station is found.
    """
    url = f"{BASE_URL}/id/stations/{notation}.json"
    logger.info(f"Fetching station metadata for notation={notation}")
    data = get_json(url, timeout=timeout)
    items = data.get("items", [])
    if not items:
        logger.error(f"No station found for notation={notation}")
        raise ValueError(f"No station found for notation={notation}")
    logger.debug(f"Station metadata fetched for notation={notation}")
    return items[0]


def _measure_id_from_uri(uri: str) -> str:
    """Extracts the measure ID from a measure URI."""
    return uri.split("/measures/")[-1]


def resolve_measures_from_station(station_item: Dict[str, Any], requested_params: List[str]) -> Dict[str, str]:
    """
    Resolve measure IDs for the requested parameters from the station metadata.
    Args:
        station_item (Dict[str, Any]): Station metadata.
        requested_params (List[str]): List of parameter names (must be exactly two: conductivity, dissolved-oxygen).
    Returns:
        Dict[str, str]: Mapping from parameter name to measure ID.
    Raises:
        ValueError: If parameters are invalid or not found.
    """
    allowed = {"conductivity", "dissolved-oxygen"}
    normalized = [p.lower() for p in requested_params]

    if len(normalized) != 2:
        logger.error("Exactly two parameters are required (e.g. conductivity dissolved-oxygen).")
        raise ValueError("Exactly two parameters are required (e.g. conductivity dissolved-oxygen).")

    if any(p not in allowed for p in normalized):
        logger.error(f"Only these parameters are supported for this task: {sorted(allowed)}")
        raise ValueError(f"Only these parameters are supported for this task: {sorted(allowed)}")

    measures = station_item.get("measures", [])
    if not measures:
        logger.error("Station payload has no measures list")
        raise ValueError("Station payload has no measures list")

    conductivity_candidates: List[str] = []
    do_candidates: List[str] = []

    for m in measures:
        mid = _measure_id_from_uri(m.get("@id", ""))
        low = mid.lower()
        if "-cond-" in low:
            conductivity_candidates.append(mid)
        if "-do-" in low:
            do_candidates.append(mid)

    if not conductivity_candidates:
        logger.error("Could not resolve a conductivity measure from station measures")
        raise ValueError("Could not resolve a conductivity measure from station measures")
    if not do_candidates:
        logger.error("Could not resolve a dissolved-oxygen measure from station measures")
        raise ValueError("Could not resolve a dissolved-oxygen measure from station measures")

    # Prefer dissolved-oxygen in mg/L
    do_mgl = [x for x in do_candidates if "mgl" in x.lower()]
    do_measure = do_mgl[0] if do_mgl else do_candidates[0]

    logger.info(f"Resolved measures: conductivity={conductivity_candidates[0]}, dissolved-oxygen={do_measure}")
    return {
        "conductivity": conductivity_candidates[0],
        "dissolved-oxygen": do_measure,
    }

def fetch_latest_readings_for_measure(measure_id: str, limit: int = 10, timeout: int = 30) -> List[Dict[str, Any]]:
    """
    Fetch the latest readings for a given measure.
    Args:
        measure_id (str): The measure ID.
        limit (int): Number of readings to fetch.
        timeout (int): Timeout for the API request.
    Returns:
        List[Dict[str, Any]]: List of reading records.
    Raises:
        ValueError: If no readings are found.
    """
    url = f"{BASE_URL}/id/measures/{measure_id}/readings.json"
    params = {"latest": "", "_limit": limit}
    logger.info(f"Fetching latest {limit} readings for measure_id={measure_id}")
    data = get_json(url, params=params, timeout=timeout)
    readings = data.get("items", [])
    if not readings:
        logger.warning(f"No readings found for measure_id={measure_id}")
    return readings
    items = data.get("items", [])
    if not isinstance(items, list):
        raise ValueError(f"Unexpected readings payload for measure_id={measure_id}")
    return items
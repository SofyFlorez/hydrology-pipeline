import logging
from typing import Any, Dict, List
from .api_client import get_json
from .config import BASE_URL
from .config import BASE_URL, ALLOWED_PARAMETERS

logger: logging.Logger = logging.getLogger(__name__)

def fetch_station_by_notation(notation: str, timeout: int = 30) -> Dict[str, Any]:
    """
    Retrieve metadata for a monitoring station by its notation identifier.

    Parameters
    ----------
    notation : str
        Station notation (e.g., E64999A).
    timeout : int
        Request timeout in seconds.

    Returns
    -------
    Dict[str, Any]
        Station metadata payload.

    Raises
    ------
    ValueError
        If the station cannot be found.
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

def resolve_measures_from_station(
    station_item: Dict[str, Any],
    requested_params: List[str],
) -> Dict[str, str]:
    """
    Resolve measure identifiers for the requested parameters from a station payload.

    Parameters
    ----------
    station_item : Dict[str, Any]
        Station metadata returned by the API.
    requested_params : List[str]
        List containing exactly two parameter names.

    Returns
    -------
    Dict[str, str]
        Mapping of parameter name to resolved measure ID.

    Raises
    ------
    ValueError
        If parameters are invalid or required measures cannot be resolved.
    """

    normalized: List[str] = [p.lower() for p in requested_params]

    if len(normalized) != 2:
        logger.error("Exactly two parameters are required (e.g. conductivity dissolved-oxygen).")
        raise ValueError("Exactly two parameters are required (e.g. conductivity dissolved-oxygen).")

    if any(p not in ALLOWED_PARAMETERS for p in normalized):
        logger.error(f"Only these parameters are supported: {sorted(ALLOWED_PARAMETERS)}")
        raise ValueError(f"Only these parameters are supported: {sorted(ALLOWED_PARAMETERS)}")

    measures: List[str] = station_item.get("measures", [])
    if not measures:
        logger.error("Station payload has no measures list")
        raise ValueError("Station payload has no measures list")

    conductivity_candidates: List[str] = []
    do_candidates: List[str] = []

    for m in measures:
        mid: str = _measure_id_from_uri(m.get("@id", ""))
        low: str = mid.lower()

        if "-cond-" in low:
            conductivity_candidates.append(mid)

        if "-do-" in low:
            do_candidates.append(mid)

    if not conductivity_candidates:
        logger.error("Could not resolve a conductivity measure from station measures")
        raise ValueError("Could not resolve a conductivity measure")

    if not do_candidates:
        logger.error("Could not resolve a dissolved-oxygen measure from station measures")
        raise ValueError("Could not resolve a dissolved-oxygen measure")

    # Prefer mg/L dissolved oxygen if available
    do_mgl: List[str] = [x for x in do_candidates if "mgl" in x.lower()]
    do_measure: str = do_mgl[0] if do_mgl else do_candidates[0]

    logger.info(
        f"Resolved measures: conductivity={conductivity_candidates[0]}, dissolved-oxygen={do_measure}"
    )

    return {
        "conductivity": conductivity_candidates[0],
        "dissolved-oxygen": do_measure,
    }

def fetch_latest_readings_for_measure(
    measure_id: str,
    limit: int = 10,
    timeout: int = 30,
) -> List[Dict[str, Any]]:
    """
    Fetch the most recent N readings for a measure.

    The API's `latest` parameter returns only a single record.
    To retrieve multiple recent readings, the request sorts by
    `dateTime` in descending order and applies `_limit`.

    Parameters
    ----------
    measure_id : str
        Unique identifier of the measure.
    limit : int
        Number of most recent readings to retrieve.
    timeout : int
        Request timeout in seconds.

    Returns
    -------
    List[Dict[str, Any]]
        List of reading records sorted chronologically.
    """
    if limit <= 0:
        raise ValueError("limit must be a positive integer")

    url: str = f"{BASE_URL}/id/measures/{measure_id}/readings.json"

    # Ask the API for the most recent N readings (newest first).
    params: Dict[str, Any] = {"_limit": limit, "_sort": "-dateTime"}

    logger.info(f"Fetching latest {limit} readings for measure_id={measure_id}")
    data: Dict[str, Any] = get_json(url, params=params, timeout=timeout)
    readings: List[Dict[str, Any]] = data.get("items", []) or []

    if not readings:
        logger.warning(f"No readings found for measure_id={measure_id}")
        return []

    # Return in chronological order (oldest -> newest) for nicer storage/analysis.
    readings_sorted = sorted(readings, key=lambda r: r.get("dateTime", ""))
    return readings_sorted
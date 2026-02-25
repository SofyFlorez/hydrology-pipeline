from __future__ import annotations

def get_json(
from typing import Any, Dict, Optional
import logging
import requests

logger = logging.getLogger(__name__)

class HydrologyApiError(RuntimeError):
    """
    Raised when the Hydrology API request fails or returns invalid JSON.
    """
    pass

def get_json(
    url: str,
    params: Optional[Dict[str, Any]] = None,
    timeout: int = 30,
) -> Dict[str, Any]:
    """
    HTTP GET wrapper for the Hydrology API.

    Args:
        url (str): The API endpoint URL.
        params (Optional[Dict[str, Any]]): Query parameters for the request.
        timeout (int): Timeout for the request in seconds.

    Returns:
        Dict[str, Any]: Parsed JSON response.

    Raises:
        HydrologyApiError: If the request fails or the response is not valid JSON.
    """
    logger.debug(f"Requesting URL: {url} with params: {params} and timeout: {timeout}")
    try:
        response = requests.get(url, params=params, timeout=timeout)
        response.raise_for_status()
        logger.info(f"Successful GET request: {url}")
    except requests.RequestException as exc:
        logger.error(f"HTTP request failed: url={url} params={params} error={exc}")
        raise HydrologyApiError(f"HTTP request failed: url={url} params={params} error={exc}") from exc

    try:
        json_data = response.json()
        logger.debug(f"Received JSON response from {url}")
        return json_data
    except ValueError as exc:
        logger.error(f"Invalid JSON response: url={url} params={params}")
        raise HydrologyApiError(f"Invalid JSON response: url={url} params={params}") from exc
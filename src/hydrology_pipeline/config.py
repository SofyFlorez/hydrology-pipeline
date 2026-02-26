from dataclasses import dataclass
from pathlib import Path
from typing import List, Optional

# Base endpoint for the Environment Agency Hydrology API
BASE_URL = "https://environment.data.gov.uk/hydrology"

# Default station identifier (Hydrology API notation)
DEFAULT_STATION_NOTATION = "E64999A"

# Default parameters for data extraction 
DEFAULT_PARAMS = ["conductivity", "dissolved-oxygen"]

# Parameters supported by the pipeline validation layer
ALLOWED_PARAMETERS = {"conductivity", "dissolved-oxygen"}

# Number of latest readings to fetch per parameter
DEFAULT_LIMIT = 10
@dataclass(frozen=True)
class PipelineConfig:
    """
    Configuration object for the ETL pipeline.

    Holds runtime parameters such as station, selected measures,
    database path and timeout configuration.
    """

    station_notation: str = DEFAULT_STATION_NOTATION
    params: Optional[List[str]] = None
    limit: int = DEFAULT_LIMIT
    db_path: Path = Path("data") / "hydrology.db"
    timeout_seconds: int = 30

    def __post_init__(self) -> None:
        # Ensure default parameters are applied if none are provided
        object.__setattr__(self, "params", self.params or list(DEFAULT_PARAMS))
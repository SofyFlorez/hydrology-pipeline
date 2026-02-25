from dataclasses import dataclass
from pathlib import Path
from typing import List, Optional

BASE_URL = "https://environment.data.gov.uk/hydrology"

# Station: HIPPER_PARK ROAD BRIDGE_E_202312
DEFAULT_STATION_NOTATION = "E64999A"

# Task example parameters (and they exist on this station)
DEFAULT_PARAMS = ["conductivity", "dissolved-oxygen"]

DEFAULT_LIMIT = 10


@dataclass(frozen=True)
class PipelineConfig:
    station_notation: str = DEFAULT_STATION_NOTATION
    params: Optional[List[str]] = None
    limit: int = DEFAULT_LIMIT
    db_path: Path = Path("data") / "hydrology.db"
    timeout_seconds: int = 30

    def __post_init__(self):
        object.__setattr__(self, "params", self.params or list(DEFAULT_PARAMS))
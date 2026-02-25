import logging

from .db import connect, init_db, upsert_station, insert_measurements
from .extract import (
    fetch_station_by_notation,
    resolve_measures_from_station,
    fetch_latest_readings_for_measure,
)
from .transform import normalize_station, normalize_reading

logger: logging.Logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


def run(config) -> None:
    """Main ETL pipeline"""

    conn = connect(config.db_path)
    init_db(conn)

    station_item = fetch_station_by_notation(config.station_notation)
    station = normalize_station(station_item)
    upsert_station(conn, station)

    measure_map = resolve_measures_from_station(station_item, config.params)

    total_inserted = 0

    for param, measure_id in measure_map.items():
        readings = fetch_latest_readings_for_measure(
            measure_id=measure_id,
            limit=config.limit,
        )

        rows = [
            normalize_reading(
                reading=r,
                station_id=station.station_id,
                observed_property=param,
                measure_id=measure_id,
            )
            for r in readings
        ]

        inserted = insert_measurements(conn, rows)
        total_inserted += inserted

        logger.info(f"Inserted {inserted}/{len(rows)} rows for {param} ({measure_id})")

    logger.info(f"Done. Station={station.station_id}. Total inserted={total_inserted}. DB={config.db_path}")
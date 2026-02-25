from __future__ import annotations

def run(cfg: PipelineConfig) -> None:
import logging
from .config import PipelineConfig
from .db import connect, init_db, insert_measurements, upsert_station
from .extract import (
    fetch_latest_readings_for_measure,
    fetch_station_by_notation,
    resolve_measures_from_station,
)
from .transform import normalize_reading, normalize_station

logger = logging.getLogger(__name__)

def run(cfg: PipelineConfig) -> None:
    """
    Run the ETL pipeline: extract from API, transform, and load into SQLite DB.
    Args:
        cfg (PipelineConfig): Pipeline configuration.
    """
    logger.info(f"Starting pipeline for station: {cfg.station_notation}")
    try:
        station_item = fetch_station_by_notation(cfg.station_notation, timeout=cfg.timeout_seconds)
        station = normalize_station(station_item)
        logger.info(f"Fetched and normalized station: {station.station_id}")

        measures_map = resolve_measures_from_station(station_item, cfg.params)
        logger.info(f"Resolved measures: {measures_map}")

        conn = connect(cfg.db_path)
        init_db(conn)
        upsert_station(conn, station)

        total_inserted = 0
        for observed_property, measure_id in measures_map.items():
            readings = fetch_latest_readings_for_measure(
                measure_id=measure_id,
                limit=cfg.limit,
                timeout=cfg.timeout_seconds,
            )
            logger.info(f"Fetched {len(readings)} readings for {observed_property} (measure_id={measure_id})")

            rows = [
                normalize_reading(
                    reading=r,
                    station_id=station.station_id,
                    observed_property=observed_property,
                    measure_id=measure_id,
                )
                for r in readings
            ]
            inserted = insert_measurements(conn, rows)
            logger.info(f"Inserted {inserted} {observed_property} measurements.")
            total_inserted += inserted

        logger.info(f"Pipeline complete. Total measurements inserted: {total_inserted}")
    except Exception as exc:
        logger.error(f"Pipeline failed: {exc}")
        raise
        inserted = insert_measurements(conn, rows)
        total_inserted += inserted
        print(f"Inserted {inserted}/{len(rows)} rows for {observed_property} ({measure_id})")

    print(f"Done. Station={station.station_id}. Total inserted={total_inserted}. DB={cfg.db_path}")
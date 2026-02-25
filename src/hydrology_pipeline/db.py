import logging
import sqlite3
from pathlib import Path
from typing import Iterable

from .transform import MeasurementRow, StationRow

logger = logging.getLogger(__name__)


def connect(db_path: Path) -> sqlite3.Connection:
    """
    Connect to the SQLite database, creating parent directories if needed.
    """
    db_path.parent.mkdir(parents=True, exist_ok=True)
    try:
        conn = sqlite3.connect(str(db_path))
        conn.execute("PRAGMA foreign_keys = ON;")
        logger.info(f"Connected to SQLite DB at {db_path}")
        return conn
    except Exception as exc:
        logger.error(f"Failed to connect to DB at {db_path}: {exc}")
        raise


def init_db(conn: sqlite3.Connection) -> None:
    """
    Initialize the database schema for stations and measurements tables.
    """
    schema = """
    CREATE TABLE IF NOT EXISTS stations (
        station_id   TEXT PRIMARY KEY,
        label        TEXT NOT NULL,
        lat          REAL NOT NULL,
        long         REAL NOT NULL,
        river_name   TEXT,
        date_opened  TEXT,
        extracted_at TEXT DEFAULT (datetime('now'))
    );

    CREATE TABLE IF NOT EXISTS measurements (
        station_id        TEXT NOT NULL,
        observed_property TEXT NOT NULL,
        measure_id        TEXT NOT NULL,
        date_time         TEXT NOT NULL,
        value             REAL,
        quality           TEXT,
        completeness      TEXT,
        ingested_at       TEXT DEFAULT (datetime('now')),
        PRIMARY KEY (measure_id, date_time),
        FOREIGN KEY (station_id) REFERENCES stations(station_id)
    );

    CREATE INDEX IF NOT EXISTS idx_measurements_station_prop_dt
    ON measurements(station_id, observed_property, date_time);
    """

    try:
        with conn:
            conn.executescript(schema)
        logger.info("Database schema initialized.")
    except Exception as exc:
        logger.error(f"Failed to initialize database schema: {exc}")
        raise


def upsert_station(conn: sqlite3.Connection, station: StationRow) -> None:
    """
    Insert or update a station record in the database.
    """
    sql = """
    INSERT INTO stations(station_id, label, lat, long, river_name, date_opened)
    VALUES(?,?,?,?,?,?)
    ON CONFLICT(station_id) DO UPDATE SET
        label=excluded.label,
        lat=excluded.lat,
        long=excluded.long,
        river_name=excluded.river_name,
        date_opened=excluded.date_opened;
    """

    try:
        with conn:
            conn.execute(
                sql,
                (
                    station.station_id,
                    station.label,
                    station.lat,
                    station.long,
                    station.river_name,
                    station.date_opened,
                ),
            )
        logger.info(f"Upserted station: {station.station_id}")
    except Exception as exc:
        logger.error(f"Failed to upsert station {station.station_id}: {exc}")
        raise


def insert_measurements(conn: sqlite3.Connection, rows: Iterable[MeasurementRow]) -> int:
    """
    Insert measurement records into the database, skipping duplicates based on
    PRIMARY KEY (measure_id, date_time).
    """
    sql = """
    INSERT INTO measurements(
        station_id, observed_property, measure_id, date_time,
        value, quality, completeness
    )
    VALUES(?,?,?,?,?,?,?)
    ON CONFLICT(measure_id, date_time) DO NOTHING;
    """

    inserted = 0
    try:
        with conn:
            cur = conn.cursor()
            for r in rows:
                cur.execute(
                    sql,
                    (
                        r.station_id,
                        r.observed_property,
                        r.measure_id,
                        r.date_time,
                        r.value,
                        r.quality,
                        r.completeness,
                    ),
                )
                inserted += cur.rowcount  # 1 if inserted, 0 if skipped
        logger.info(f"Inserted {inserted} new measurements.")
        return inserted
    except Exception as exc:
        logger.error(f"Failed to insert measurements batch: {exc}")
        raise
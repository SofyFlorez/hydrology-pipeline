from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Iterable

from .transform import MeasurementRow, StationRow


def connect(db_path: Path) -> sqlite3.Connection:
    """
    Connect to the SQLite database, creating parent directories if needed.
    Args:
        db_path (Path): Path to the SQLite database file.
    Returns:
        sqlite3.Connection: SQLite connection object.
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
    Args:
        conn (sqlite3.Connection): SQLite connection object.
    """
    try:
        conn.executescript(
            """
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
                id                INTEGER PRIMARY KEY AUTOINCREMENT,
                station_id        TEXT NOT NULL,
                observed_property TEXT NOT NULL,
                measure_id        TEXT NOT NULL,
                date_time         TEXT NOT NULL,
                value             REAL,
                quality           TEXT,
                completeness      TEXT,
                ingested_at       TEXT DEFAULT (datetime('now')),
                FOREIGN KEY (station_id) REFERENCES stations(station_id)
            );

            CREATE INDEX IF NOT EXISTS idx_measurements_station_prop_dt
            ON measurements(station_id, observed_property, date_time);

            CREATE UNIQUE INDEX IF NOT EXISTS ux_measure_dt
            ON measurements(measure_id, date_time);
            """
        )
        conn.commit()
        logger.info("Database schema initialized.")
    except Exception as exc:
        logger.error(f"Failed to initialize database schema: {exc}")
        raise


def upsert_station(conn: sqlite3.Connection, station: StationRow) -> None:
    """
    Insert or update a station record in the database.
    Args:
        conn (sqlite3.Connection): SQLite connection object.
        station (StationRow): Station data to upsert.
    """
    try:
        conn.execute(
            """
            INSERT INTO stations(station_id, label, lat, long, river_name, date_opened)
            VALUES(?,?,?,?,?,?)
            ON CONFLICT(station_id) DO UPDATE SET
                label=excluded.label,
                lat=excluded.lat,
                long=excluded.long,
                river_name=excluded.river_name,
                date_opened=excluded.date_opened;
            """,
            (station.station_id, station.label, station.lat, station.long, station.river_name, station.date_opened),
        )
        conn.commit()
        logger.info(f"Upserted station: {station.station_id}")
    except Exception as exc:
        logger.error(f"Failed to upsert station {station.station_id}: {exc}")
        raise


def insert_measurements(conn: sqlite3.Connection, rows: Iterable[MeasurementRow]) -> int:
    """
    Insert measurement records into the database, skipping duplicates.
    Args:
        conn (sqlite3.Connection): SQLite connection object.
        rows (Iterable[MeasurementRow]): Measurement rows to insert.
    Returns:
        int: Number of inserted records.
    """
    cur = conn.cursor()
    inserted = 0
    for r in rows:
        try:
            cur.execute(
                """
                INSERT INTO measurements(
                    station_id, observed_property, measure_id, date_time,
                    value, quality, completeness
                )
                VALUES(?,?,?,?,?,?,?)
                """,
                (r.station_id, r.observed_property, r.measure_id, r.date_time, r.value, r.quality, r.completeness),
            )
            inserted += 1
        except sqlite3.IntegrityError:
            logger.info(f"Duplicate measurement skipped: measure_id={r.measure_id}, date_time={r.date_time}")
            continue
        except Exception as exc:
            logger.error(f"Failed to insert measurement: {exc}")
            continue
    conn.commit()
    logger.info(f"Inserted {inserted} new measurements.")
    return inserted
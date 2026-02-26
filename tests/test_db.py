import sqlite3
import tempfile
import os
from pathlib import Path
from src.hydrology_pipeline.db import connect, init_db, upsert_station, insert_measurements
from src.hydrology_pipeline.transform import StationRow, MeasurementRow

def test_db_upsert_and_insert():
    """Ensure station upsert is idempotent and duplicate measurements are ignored."""
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / "test.db"
        conn = connect(db_path)
        init_db(conn)
        station = StationRow("S1", "Test", 1.0, 2.0, None, None)
        upsert_station(conn, station)
        # Upsert again (should not error)
        upsert_station(conn, station)
        row = MeasurementRow("S1", "conductivity", "M1", "2024-01-01T00:00:00+00:00", 1.23, None)
        inserted = insert_measurements(conn, [row, row])  # duplicate
        assert inserted == 1
        # Check data
        cur = conn.execute("SELECT COUNT(*) FROM measurements WHERE station_id='S1'")
        assert cur.fetchone()[0] == 1

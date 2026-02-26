# Hydrology ETL Pipeline

This project implements a modular ETL pipeline that retrieves hydrological time-series data from the Environment Agency Hydrology API and stores it in a local SQLite database using a star-schema design.  
The goal of this project is to demonstrate clean, modular, and testable data engineering practices.

---

## Overview

The pipeline performs the following steps:

1. **Extract** – Connects to the Hydrology API and retrieves measurements  
2. **Transform** – Validates and normalizes station and reading data  
3. **Load** – Inserts the transformed data into a SQLite database  

It downloads the **10 most recent measurements** for:

- Conductivity  
- Dissolved Oxygen  

From station:

**HIPPER_PARK ROAD BRIDGE_E_202312 (E64999A)**

---

## Architecture

The project follows a simple star-style schema:

- `stations` → dimension table (station metadata)  
- `measurements` → fact table (time-series readings)  

The SQLite database file is created locally at `data/hydrology.db`.

The `measurements` table uses `(measure_id, date_time)` as a composite primary key to prevent duplicate inserts and guarantee idempotent pipeline execution.

---

## Assumptions

- The API request uses `_sort=-dateTime` and `_limit` to retrieve the most recent readings.
- The schema reflects the fields returned by the selected measures.
- The pipeline can be safely re-run without creating duplicates.

---

## Project Structure

```
hydrology-pipeline/
├── data/                        # Output SQLite database
├── src/
│   ├── main.py                  # CLI entrypoint for running the pipeline
│   └── hydrology_pipeline/
│       ├── __init__.py
│       ├── api_client.py        # API requests
│       ├── config.py            # Configuration
│       ├── db.py                # SQLite schema & inserts
│       ├── extract.py           # API extraction logic
│       ├── pipeline.py          # ETL orchestration
│       └── transform.py         # Data validation and normalization
├── tests/
│   ├── test_db.py               # Test for database operations
│   ├── test_extract.py          # Tests for extraction logic
│   └── test_transform.py        # Test for transformation and validation
├── requirements.txt             # Project dependencies
├── pytest.ini                   # Pytest configuration
└── README.md
```

---

## Setup

Create and activate a virtual environment:

### Windows

```bash
python -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt
```

### macOS / Linux

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

---

## Run the Pipeline

Basic usage:

```bash
python -m src.main --station E64999A --params conductivity dissolved-oxygen --limit 10
```

This will:

- Connect to the Hydrology API  
- Retrieve the latest measurements  
- Transform and validate the data  
- Store results in a local SQLite database (`data/hydrology.db`)  

---

## CLI Options

| Option | Description | Default |
|--------|-------------|--------|
| `--station` | Station notation | `E64999A` |
| `--params` | Two parameters to download | `conductivity dissolved-oxygen` |
| `--limit` | Number of recent readings per parameter | `10` |
| `--db` | Path to SQLite database file | `data/hydrology.db` |

The pipeline validates that the station label matches:

`HIPPER_PARK ROAD BRIDGE_E_202312`

---

## Testing

Run unit tests:

```bash
pytest
```

The test suite covers:
- Database schema and inserts
- Extraction validation logic
- Transformation logic and edge cases

---

## Data Handling Notes

- The schema reflects the actual fields returned by the selected API measures.
- Optional fields described in the API documentation (e.g., completeness) were not included as they were not present in the selected payload.
- Timestamps are stored in ISO 8601 string format as returned by the API.
- Ingestion time is recorded via `ingested_at` for traceability.

---

## Idempotency

The measurements table uses a composite primary key `(measure_id, date_time)`.  
This guarantees that rerunning the pipeline will not insert duplicate records.

---

## Design Decisions

- ETL architecture chosen for clarity and simplicity
- Functional modular structure
- Idempotent database writes
- Minimal dependencies (requests and pytest)
- Cross-platform compatibility (Windows, macOS, Linux)

---

## API Reference

Hydrology Data Explorer API documentation:

`https://environment.data.gov.uk/hydrology/doc/reference`
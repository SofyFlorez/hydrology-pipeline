# Hydrology ETL Pipeline

This project implements a simple ETL data engineering pipeline that retrieves hydrological measurements from the Environment Agency Hydrological Data Explorer API, transforms the data, and stores it in a file-based SQLite database.

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

The SQLite database file is created locally: `data/hydrology.db`


The `measurements` table uses `(measure_id, date_time)` as a composite primary key to prevent duplicate inserts and ensure idempotency.

---

## Project Structure

hydrology-pipeline/
├─ src/
│ ├─ main.py
│ └─ hydrology_pipeline/
│ ├─ api_client.py
│ ├─ config.py
│ ├─ db.py
│ ├─ extract.py
│ ├─ transform.py
│ └─ pipeline.py
├─ tests/
├─ requirements.txt
├─ pytest.ini
└─ README.md

## Setup

Create and activate a virtual environment:

### Windows

python -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt

## Run the Pipeline

`python -m src.main --station E64999A --params conductivity dissolved-oxygen --limit 10`

This command will:

- Call the API
- Transform the retrieved data
- Insert the results into data/hydrology.db

## CLI Options

`--station` – Station notation (default: `E64999A`)

`--params` – Exactly two parameters (default: `conductivity dissolved-oxygen`)

`--limit` – Number of most recent readings per parameter (default: `10`)

`--db` – Path to SQLite database (default: `data/hydrology.db`)

## Testing

Run unit tests:

`pytest`

- The test suite covers:
- Database schema and inserts
- Extraction validation logic
- Transformation logic and edge cases

## Data Notes

Data Notes

- `quality` and `completeness` are stored exactly as provided by the API.
- Some readings do not include `completeness`; in those cases it is stored as `NULL`.
- Duplicate readings are prevented using the composite primary key (`measure_id`, `date_time`).

## Design Decisions

- ETL architecture chosen for clarity and simplicity
- Functional modular structure
- Idempotent database writes
- Minimal dependencies (requests and pytest)
- Cross-platform compatibility (Windows, macOS, Linux)

## API Reference

Hydrology Data Explorer API documentation:

`https://environment.data.gov.uk/hydrology/doc/reference`
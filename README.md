# Hydrology ETL Pipeline

This project implements a simple data engineering pipeline to retrieve recent hydrological measurements from the [Environment Agency Hydrological Data Explorer API](https://environment.data.gov.uk/hydrology/doc/reference), transform the data, and store it in a file-based SQLite database using a star schema.

## Features
- Connects to the Hydrological Data Explorer API
- Downloads the 10 most recent measurements for two parameters (e.g., dissolved oxygen and conductivity) from a specified station
- Transforms and validates the data
- Stores data in a SQLite database (star schema: stations and measurements tables)
- Runs via a single command line
- Includes unit tests for core logic and edge cases

## Setup
```sh
python -m venv .venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate
pip install -r requirements.txt
```

## Usage
```sh
python -m src.main --station E64999A --params conductivity dissolved-oxygen --limit 10
```

### CLI Options
- `--station`: Station notation (default: E64999A)
- `--params`: Exactly two parameters (default: conductivity dissolved-oxygen)
- `--limit`: Number of latest readings per parameter (default: 10)
- `--db`: Path to SQLite database (default: data/hydrology.db)

## Testing
```sh
pytest
```

## Notes
- The project is cross-platform and works on Windows, macOS, and Linux.
- All code follows best practices, is modular, and includes robust error handling and logging.
- For more details, see the code and docstrings in each module.
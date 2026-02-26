import logging
import argparse
from pathlib import Path
from src.hydrology_pipeline.config import PipelineConfig
from src.hydrology_pipeline.pipeline import run

def parse_args() -> argparse.Namespace:
    """Parse CLI arguments for the hydrology ETL pipeline."""
    p = argparse.ArgumentParser(description="Hydrology ETL: API -> SQLite (stations + measurements)")
    p.add_argument("--station", default="E64999A", help="Station notation (e.g., E64999A)")
    p.add_argument("--db", default="data/hydrology.db", help="SQLite path")
    p.add_argument("--limit", type=int, default=10, help="Number of latest readings per parameter")
    p.add_argument(
        "--params",
        nargs="+",
        default=["conductivity", "dissolved-oxygen"],
        help="Exactly two parameters: conductivity dissolved-oxygen",
    )
    return p.parse_args()

def main() -> None:
    """Entrypoint for running the ETL pipeline from the command line."""
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    args = parse_args()
    cfg = PipelineConfig(
        station_notation=args.station,
        db_path=Path(args.db),
        limit=args.limit,
        params=args.params,
    )
    try:
        run(cfg)
    except Exception as exc:
        logging.error(f"Pipeline execution failed: {exc}")
        raise

if __name__ == "__main__":
    main()
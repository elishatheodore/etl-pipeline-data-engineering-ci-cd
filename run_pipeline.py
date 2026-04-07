"""
MAIN PIPELINE ORCHESTRATOR
============================
Run this file to execute the full ETL pipeline:
  bronze → silver → gold → data quality report

Usage:
    python run_pipeline.py

Or with a custom data directory:
    python run_pipeline.py --data-dir /path/to/csvs

Author: Elisha Theodore
"""

import argparse
import logging
import sys
import time
from pathlib import Path

# Add project root to path so imports work
sys.path.insert(0, str(Path(__file__).parent))

from pipeline.bronze_layer import load_bronze
from pipeline.silver_layer import transform_silver
from pipeline.gold_layer import build_gold
from pipeline.data_quality import run_quality_report

# --- Logging setup ---
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("pipeline.log", mode="w"),   # also save to file
    ]
)
log = logging.getLogger(__name__)


def main(data_dir: str, db_path: str):
    start = time.time()

    log.info("🚀 Starting Olist ETL Pipeline")
    log.info(f"   Data directory : {data_dir}")
    log.info(f"   Database       : {db_path}")

    # STEP 1: Bronze
    t0 = time.time()
    bronze_results = load_bronze(data_dir=data_dir, db_path=db_path)
    log.info(f"⏱  Bronze completed in {time.time() - t0:.1f}s")

    # STEP 2: Silver
    t0 = time.time()
    silver_results = transform_silver(db_path=db_path)
    log.info(f"⏱  Silver completed in {time.time() - t0:.1f}s")

    # STEP 3: Gold
    t0 = time.time()
    gold_results = build_gold(db_path=db_path)
    log.info(f"⏱  Gold completed in {time.time() - t0:.1f}s")

    # STEP 4: Data Quality Report
    run_quality_report(db_path=db_path)

    elapsed = time.time() - start
    log.info(f"✅ Full pipeline completed in {elapsed:.1f}s")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run the Olist ETL Pipeline")
    parser.add_argument(
        "--data-dir",
        default=str(Path(__file__).parent / "data"),
        help="Path to the folder containing Olist CSV files"
    )
    parser.add_argument(
        "--db-path",
        default=str(Path(__file__).parent / "olist.duckdb"),
        help="Path to the DuckDB database file"
    )
    args = parser.parse_args()

    main(data_dir=args.data_dir, db_path=args.db_path)

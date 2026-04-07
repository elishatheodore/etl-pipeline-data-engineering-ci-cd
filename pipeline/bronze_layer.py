"""
BRONZE LAYER - Raw Data Ingestion
==================================
This is the first layer of the Medallion Architecture.
Bronze = raw data, exactly as it came from the source.
We validate it exists and log basic stats, but do NOT transform it yet.

Author: Elisha Theodore
Dataset: Brazilian E-Commerce (Olist) - https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce
"""

import os
import logging
import duckdb
import pandas as pd
from pathlib import Path
from datetime import datetime

# --- Setup logging so we can see what's happening ---
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
log = logging.getLogger(__name__)


# --- File map: logical name → CSV filename ---
OLIST_FILES = {
    "customers":     "olist_customers_dataset.csv",
    "geolocation":   "olist_geolocation_dataset.csv",
    "order_items":   "olist_order_items_dataset.csv",
    "order_payments":"olist_order_payments_dataset.csv",
    "order_reviews": "olist_order_reviews_dataset.csv",
    "orders":        "olist_orders_dataset.csv",
    "products":      "olist_products_dataset.csv",
    "sellers":       "olist_sellers_dataset.csv",
    "translations":  "product_category_name_translation.csv",
}


def load_bronze(data_dir: str, db_path: str) -> dict:
    """
    Load all raw CSVs into DuckDB as bronze tables.
    Returns a dict of {table_name: row_count} for reporting.
    """
    data_dir = Path(data_dir)
    results = {}

    log.info("=" * 60)
    log.info("BRONZE LAYER — Starting raw data ingestion")
    log.info(f"Data directory : {data_dir.resolve()}")
    log.info(f"Database path  : {db_path}")
    log.info("=" * 60)

    # Connect to DuckDB (creates the file if it doesn't exist)
    con = duckdb.connect(db_path)

    # Create a schema to keep things organised
    con.execute("CREATE SCHEMA IF NOT EXISTS bronze")

    for table_name, filename in OLIST_FILES.items():
        filepath = data_dir / filename

        if not filepath.exists():
            log.error(f"MISSING FILE: {filepath}")
            log.error("  → Download the Olist dataset from Kaggle and place CSVs in the data/ folder.")
            raise FileNotFoundError(f"Expected file not found: {filepath}")

        # Read CSV with pandas
        df = pd.read_csv(filepath, low_memory=False)
        row_count = len(df)

        # Write to DuckDB bronze schema — replace on each run (idempotent)
        con.execute(f"DROP TABLE IF EXISTS bronze.{table_name}")
        con.register("_tmp_df", df)
        con.execute(f"CREATE TABLE bronze.{table_name} AS SELECT * FROM _tmp_df")
        con.unregister("_tmp_df")

        # Add metadata columns so we know when the data was loaded
        con.execute(f"""
            ALTER TABLE bronze.{table_name}
            ADD COLUMN _ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        """)

        results[table_name] = row_count
        log.info(f"  ✓ bronze.{table_name:<20} {row_count:>8,} rows  ← {filename}")

    con.close()

    log.info("-" * 60)
    log.info(f"Bronze layer complete. {len(results)} tables loaded.")
    return results


if __name__ == "__main__":
    # Allow running this file directly for testing
    root = Path(__file__).parent.parent
    load_bronze(
        data_dir=str(root / "data"),
        db_path=str(root / "olist.duckdb")
    )

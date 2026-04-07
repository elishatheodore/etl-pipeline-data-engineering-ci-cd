"""
SILVER LAYER - Cleaned & Enriched Data
========================================
Silver = bronze data with quality issues fixed.
We apply business rules, fill nulls, cast types, and join related tables.
This is the layer analysts can trust.

Author: Elisha Theodore
"""

import logging
import duckdb
from pathlib import Path

log = logging.getLogger(__name__)


def transform_silver(db_path: str) -> dict:
    """
    Read from bronze, clean & enrich, write to silver schema.
    Returns dict of {table_name: row_count}.
    """
    log.info("=" * 60)
    log.info("SILVER LAYER — Starting data transformation")
    log.info("=" * 60)

    con = duckdb.connect(db_path)
    con.execute("CREATE SCHEMA IF NOT EXISTS silver")

    results = {}

    # ------------------------------------------------------------------
    # 1. SILVER ORDERS
    #    - Cast date strings to proper timestamps
    #    - Add a derived column: delivery_days (how long did delivery take?)
    #    - Filter out rows where order_id is null (data quality)
    # ------------------------------------------------------------------
    log.info("  → Transforming orders...")
    con.execute("DROP TABLE IF EXISTS silver.orders")
    con.execute("""
        CREATE TABLE silver.orders AS
        SELECT
            order_id,
            customer_id,
            order_status,
            -- Cast string dates to proper timestamps
            TRY_CAST(order_purchase_timestamp   AS TIMESTAMP) AS order_purchase_timestamp,
            TRY_CAST(order_approved_at          AS TIMESTAMP) AS order_approved_at,
            TRY_CAST(order_delivered_carrier_date AS TIMESTAMP) AS order_delivered_carrier_date,
            TRY_CAST(order_delivered_customer_date AS TIMESTAMP) AS order_delivered_customer_date,
            TRY_CAST(order_estimated_delivery_date AS TIMESTAMP) AS order_estimated_delivery_date,

            -- Business rule: how many days did delivery actually take?
            CASE
                WHEN order_delivered_customer_date IS NOT NULL
                 AND order_purchase_timestamp IS NOT NULL
                THEN DATEDIFF(
                    'day',
                    TRY_CAST(order_purchase_timestamp AS TIMESTAMP),
                    TRY_CAST(order_delivered_customer_date AS TIMESTAMP)
                )
                ELSE NULL
            END AS delivery_days,

            -- Was it delivered on time?
            CASE
                WHEN order_delivered_customer_date <= order_estimated_delivery_date THEN 'on_time'
                WHEN order_delivered_customer_date >  order_estimated_delivery_date THEN 'late'
                ELSE 'unknown'
            END AS delivery_status

        FROM bronze.orders
        WHERE order_id IS NOT NULL  -- data quality filter
    """)
    row_count = con.execute("SELECT COUNT(*) FROM silver.orders").fetchone()[0]
    results["orders"] = row_count
    log.info(f"  ✓ silver.orders              {row_count:>8,} rows")

    # ------------------------------------------------------------------
    # 2. SILVER ORDER ITEMS
    #    - Join with products to get category info
    #    - Join with translations so category names are in English
    #    - Add total_item_value = price + freight
    # ------------------------------------------------------------------
    log.info("  → Transforming order_items...")
    con.execute("DROP TABLE IF EXISTS silver.order_items")
    con.execute("""
        CREATE TABLE silver.order_items AS
        SELECT
            oi.order_id,
            oi.order_item_id,
            oi.product_id,
            oi.seller_id,
            CAST(oi.price     AS DOUBLE) AS price,
            CAST(oi.freight_value AS DOUBLE) AS freight_value,
            CAST(oi.price AS DOUBLE) + CAST(oi.freight_value AS DOUBLE) AS total_item_value,

            -- Bring in product category (English name)
            COALESCE(t.product_category_name_english, p.product_category_name, 'unknown') AS product_category,
            CAST(p.product_weight_g AS DOUBLE) AS product_weight_g

        FROM bronze.order_items oi

        -- Join to get product details
        LEFT JOIN bronze.products p
            ON oi.product_id = p.product_id

        -- Join to get English category name
        LEFT JOIN bronze.translations t
            ON p.product_category_name = t.product_category_name

        WHERE oi.order_id IS NOT NULL
    """)
    row_count = con.execute("SELECT COUNT(*) FROM silver.order_items").fetchone()[0]
    results["order_items"] = row_count
    log.info(f"  ✓ silver.order_items         {row_count:>8,} rows")

    # ------------------------------------------------------------------
    # 3. SILVER CUSTOMERS
    #    - Standardise state codes to uppercase
    #    - Clean city names (trim whitespace)
    # ------------------------------------------------------------------
    log.info("  → Transforming customers...")
    con.execute("DROP TABLE IF EXISTS silver.customers")
    con.execute("""
        CREATE TABLE silver.customers AS
        SELECT
            customer_id,
            customer_unique_id,
            customer_zip_code_prefix,
            TRIM(LOWER(customer_city))  AS customer_city,
            UPPER(customer_state)       AS customer_state
        FROM bronze.customers
        WHERE customer_id IS NOT NULL
    """)
    row_count = con.execute("SELECT COUNT(*) FROM silver.customers").fetchone()[0]
    results["customers"] = row_count
    log.info(f"  ✓ silver.customers           {row_count:>8,} rows")

    # ------------------------------------------------------------------
    # 4. SILVER ORDER PAYMENTS
    #    - Cast payment_value to double
    #    - Flag high-value payments (> R500 equivalent / 500 BRL)
    # ------------------------------------------------------------------
    log.info("  → Transforming order_payments...")
    con.execute("DROP TABLE IF EXISTS silver.order_payments")
    con.execute("""
        CREATE TABLE silver.order_payments AS
        SELECT
            order_id,
            payment_sequential,
            payment_type,
            payment_installments,
            CAST(payment_value AS DOUBLE) AS payment_value,
            CASE WHEN CAST(payment_value AS DOUBLE) > 500 THEN true ELSE false END AS is_high_value
        FROM bronze.order_payments
        WHERE order_id IS NOT NULL
    """)
    row_count = con.execute("SELECT COUNT(*) FROM silver.order_payments").fetchone()[0]
    results["order_payments"] = row_count
    log.info(f"  ✓ silver.order_payments      {row_count:>8,} rows")

    # ------------------------------------------------------------------
    # 5. SILVER REVIEWS
    #    - Keep only reviews with a score
    #    - Classify sentiment: positive (4-5), neutral (3), negative (1-2)
    # ------------------------------------------------------------------
    log.info("  → Transforming order_reviews...")
    con.execute("DROP TABLE IF EXISTS silver.order_reviews")
    con.execute("""
        CREATE TABLE silver.order_reviews AS
        SELECT
            review_id,
            order_id,
            CAST(review_score AS INTEGER) AS review_score,
            CASE
                WHEN CAST(review_score AS INTEGER) >= 4 THEN 'positive'
                WHEN CAST(review_score AS INTEGER) = 3  THEN 'neutral'
                ELSE 'negative'
            END AS sentiment,
            TRY_CAST(review_creation_date AS TIMESTAMP) AS review_creation_date
        FROM bronze.order_reviews
        WHERE review_score IS NOT NULL
    """)
    row_count = con.execute("SELECT COUNT(*) FROM silver.order_reviews").fetchone()[0]
    results["order_reviews"] = row_count
    log.info(f"  ✓ silver.order_reviews       {row_count:>8,} rows")

    # ------------------------------------------------------------------
    # 6. SILVER SELLERS
    # ------------------------------------------------------------------
    log.info("  → Transforming sellers...")
    con.execute("DROP TABLE IF EXISTS silver.sellers")
    con.execute("""
        CREATE TABLE silver.sellers AS
        SELECT
            seller_id,
            seller_zip_code_prefix,
            TRIM(LOWER(seller_city)) AS seller_city,
            UPPER(seller_state)      AS seller_state
        FROM bronze.sellers
        WHERE seller_id IS NOT NULL
    """)
    row_count = con.execute("SELECT COUNT(*) FROM silver.sellers").fetchone()[0]
    results["sellers"] = row_count
    log.info(f"  ✓ silver.sellers             {row_count:>8,} rows")

    con.close()

    log.info("-" * 60)
    log.info(f"Silver layer complete. {len(results)} tables transformed.")
    return results


if __name__ == "__main__":
    root = Path(__file__).parent.parent
    transform_silver(db_path=str(root / "olist.duckdb"))

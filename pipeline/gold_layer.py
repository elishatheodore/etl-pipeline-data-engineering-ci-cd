"""
Gold layer - builds analytics schema with facts and dimensions
"""

import logging
import duckdb
from pathlib import Path

log = logging.getLogger(__name__)


def build_gold(db_path: str) -> dict:
    """
    Build gold layer star schema from silver tables.
    Returns dict of {table_name: row_count}.
    """
    log.info("Building gold star schema...")

    con = duckdb.connect(db_path)
    con.execute("CREATE SCHEMA IF NOT EXISTS gold")

    results = {}

    # Date dimension
    log.info("  Creating date dimension...")
    con.execute("DROP TABLE IF EXISTS gold.dim_date")
    con.execute("""
        CREATE TABLE gold.dim_date AS
        SELECT
            CAST(STRFTIME(d, '%Y%m%d') AS INTEGER)  AS date_key,
            d                                         AS full_date,
            YEAR(d)                                   AS year,
            MONTH(d)                                  AS month,
            DAY(d)                                    AS day,
            DAYOFWEEK(d)                              AS day_of_week,
            STRFTIME(d, '%A')                         AS day_name,
            STRFTIME(d, '%B')                         AS month_name,
            QUARTER(d)                                AS quarter,
            CASE WHEN DAYOFWEEK(d) IN (1,7) THEN true ELSE false END AS is_weekend
        FROM (
            SELECT UNNEST(GENERATE_SERIES(
                DATE '2016-01-01',
                DATE '2019-12-31',
                INTERVAL '1 day'
            )) AS d
        )
    """)
    row_count = con.execute("SELECT COUNT(*) FROM gold.dim_date").fetchone()[0]
    results["dim_date"] = row_count
    log.info(f"  Date dimension: {row_count:,} rows")

    # Customer dimension
    log.info("  Creating customer dimension...")
    con.execute("DROP TABLE IF EXISTS gold.dim_customers")
    con.execute("""
        CREATE TABLE gold.dim_customers AS
        SELECT
            customer_id         AS customer_key,
            customer_unique_id,
            customer_city,
            customer_state,
            customer_zip_code_prefix
        FROM silver.customers
    """)
    row_count = con.execute("SELECT COUNT(*) FROM gold.dim_customers").fetchone()[0]
    results["dim_customers"] = row_count
    log.info(f"  Customers: {row_count:,} rows")

    # Seller dimension
    log.info("  Creating seller dimension...")
    con.execute("DROP TABLE IF EXISTS gold.dim_sellers")
    con.execute("""
        CREATE TABLE gold.dim_sellers AS
        SELECT
            seller_id   AS seller_key,
            seller_city,
            seller_state,
            seller_zip_code_prefix
        FROM silver.sellers
    """)
    row_count = con.execute("SELECT COUNT(*) FROM gold.dim_sellers").fetchone()[0]
    results["dim_sellers"] = row_count
    log.info(f"  Sellers: {row_count:,} rows")

    # Orders fact table
    log.info("  Creating fact_orders...")
    con.execute("DROP TABLE IF EXISTS gold.fact_orders")
    con.execute("""
        CREATE TABLE gold.fact_orders AS
        SELECT
            o.order_id,
            o.customer_id                                               AS customer_key,
            o.order_status,
            o.order_purchase_timestamp,
            o.delivery_days,
            o.delivery_status,

            -- Date key for joining to dim_date
            CAST(STRFTIME(CAST(o.order_purchase_timestamp AS DATE), '%Y%m%d') AS INTEGER) AS date_key,

            -- Payment totals (sum across payment methods for same order)
            COALESCE(p.total_revenue, 0)                                AS total_revenue,
            COALESCE(p.payment_installments, 1)                         AS payment_installments,
            COALESCE(p.payment_type, 'unknown')                         AS payment_type,

            -- Review
            r.review_score,
            r.sentiment,

            -- Item stats
            COALESCE(i.item_count, 0)                                   AS item_count,
            COALESCE(i.unique_products, 0)                              AS unique_products,
            COALESCE(i.unique_sellers, 0)                               AS unique_sellers,
            COALESCE(i.total_freight, 0)                                AS total_freight,
            i.top_category

        FROM silver.orders o

        -- Payment summary per order
        LEFT JOIN (
            SELECT
                order_id,
                SUM(payment_value)          AS total_revenue,
                MAX(payment_installments)   AS payment_installments,
                FIRST(payment_type ORDER BY payment_value DESC) AS payment_type
            FROM silver.order_payments
            GROUP BY order_id
        ) p ON o.order_id = p.order_id

        -- Review per order (take the latest if multiple)
        LEFT JOIN (
            SELECT DISTINCT ON (order_id)
                order_id,
                review_score,
                sentiment
            FROM silver.order_reviews
            ORDER BY order_id, review_creation_date DESC
        ) r ON o.order_id = r.order_id

        -- Item summary per order
        LEFT JOIN (
            SELECT
                order_id,
                COUNT(*)                    AS item_count,
                COUNT(DISTINCT product_id)  AS unique_products,
                COUNT(DISTINCT seller_id)   AS unique_sellers,
                SUM(freight_value)          AS total_freight,
                FIRST(product_category ORDER BY price DESC) AS top_category
            FROM silver.order_items
            GROUP BY order_id
        ) i ON o.order_id = i.order_id
    """)
    row_count = con.execute("SELECT COUNT(*) FROM gold.fact_orders").fetchone()[0]
    results["fact_orders"] = row_count
    log.info(f"  Fact orders: {row_count:,} rows")

    # Monthly revenue aggregation
    log.info("  Creating monthly revenue table...")
    con.execute("DROP TABLE IF EXISTS gold.agg_monthly_revenue")
    con.execute("""
        CREATE TABLE gold.agg_monthly_revenue AS
        SELECT
            YEAR(order_purchase_timestamp)              AS year,
            MONTH(order_purchase_timestamp)             AS month,
            STRFTIME(CAST(order_purchase_timestamp AS DATE), '%Y-%m') AS year_month,
            COUNT(DISTINCT order_id)                    AS total_orders,
            COUNT(DISTINCT customer_key)                AS unique_customers,
            ROUND(SUM(total_revenue), 2)                AS total_revenue,
            ROUND(AVG(total_revenue), 2)                AS avg_order_value,
            ROUND(AVG(delivery_days), 1)                AS avg_delivery_days,
            ROUND(AVG(CAST(review_score AS DOUBLE)), 2) AS avg_review_score,
            SUM(CASE WHEN delivery_status = 'on_time' THEN 1 ELSE 0 END) AS on_time_deliveries,
            SUM(CASE WHEN delivery_status = 'late'    THEN 1 ELSE 0 END) AS late_deliveries
        FROM gold.fact_orders
        WHERE order_purchase_timestamp IS NOT NULL
        GROUP BY 1, 2, 3
        ORDER BY 1, 2
    """)
    row_count = con.execute("SELECT COUNT(*) FROM gold.agg_monthly_revenue").fetchone()[0]
    results["agg_monthly_revenue"] = row_count
    log.info(f"  Monthly revenue: {row_count:,} rows")

    # Category performance
    log.info("  Creating category performance table...")
    con.execute("DROP TABLE IF EXISTS gold.agg_category_performance")
    con.execute("""
        CREATE TABLE gold.agg_category_performance AS
        SELECT
            product_category,
            COUNT(DISTINCT oi.order_id)             AS total_orders,
            COUNT(*)                                AS total_items_sold,
            ROUND(SUM(oi.price), 2)                 AS total_revenue,
            ROUND(AVG(oi.price), 2)                 AS avg_item_price,
            ROUND(AVG(oi.freight_value), 2)         AS avg_freight,
            ROUND(AVG(CAST(r.review_score AS DOUBLE)), 2) AS avg_review_score
        FROM silver.order_items oi
        LEFT JOIN silver.order_reviews r ON oi.order_id = r.order_id
        WHERE product_category IS NOT NULL AND product_category != 'unknown'
        GROUP BY 1
        ORDER BY total_revenue DESC
    """)
    row_count = con.execute("SELECT COUNT(*) FROM gold.agg_category_performance").fetchone()[0]
    results["agg_category_performance"] = row_count
    log.info(f"  Category performance: {row_count:,} rows")

    # Seller performance
    log.info("  Creating seller performance table...")
    con.execute("DROP TABLE IF EXISTS gold.agg_seller_performance")
    con.execute("""
        CREATE TABLE gold.agg_seller_performance AS
        SELECT
            oi.seller_id,
            s.seller_city,
            s.seller_state,
            COUNT(DISTINCT oi.order_id)             AS total_orders,
            COUNT(*)                                AS total_items_sold,
            ROUND(SUM(oi.price), 2)                 AS total_revenue,
            ROUND(AVG(oi.price), 2)                 AS avg_item_price,
            ROUND(AVG(CAST(r.review_score AS DOUBLE)), 2) AS avg_review_score,
            ROUND(AVG(o.delivery_days), 1)          AS avg_delivery_days
        FROM silver.order_items oi
        LEFT JOIN silver.sellers    s ON oi.seller_id   = s.seller_id
        LEFT JOIN silver.order_reviews r ON oi.order_id = r.order_id
        LEFT JOIN silver.orders     o ON oi.order_id    = o.order_id
        GROUP BY 1, 2, 3
        ORDER BY total_revenue DESC
    """)
    row_count = con.execute("SELECT COUNT(*) FROM gold.agg_seller_performance").fetchone()[0]
    results["agg_seller_performance"] = row_count
    log.info(f"  Seller performance: {row_count:,} rows")

    con.close()

    log.info(f"Gold layer done. {len(results)} tables")
    return results


if __name__ == "__main__":
    root = Path(__file__).parent.parent
    build_gold(db_path=str(root / "olist.duckdb"))
    build_gold(db_path=str(root / "olist.duckdb"))

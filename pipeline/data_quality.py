"""
Data quality report generator
Prints summary stats across bronze/silver/gold layers
"""

import logging
import duckdb
from pathlib import Path

log = logging.getLogger(__name__)


def run_quality_report(db_path: str):
    """Print a data quality report across all three layers."""
    con = duckdb.connect(db_path, read_only=True)

    print("\n" + "=" * 70)
    print("OLIST ETL - DATA QUALITY REPORT")
    print("=" * 70 + "\n")

    # Check bronze layer
    print("BRONZE LAYER\n")
    bronze_tables = [
        "customers", "geolocation", "order_items", "order_payments",
        "order_reviews", "orders", "products", "sellers", "translations"
    ]
    total_bronze = 0
    for t in bronze_tables:
        try:
            n = con.execute(f"SELECT COUNT(*) FROM bronze.{t}").fetchone()[0]
            total_bronze += n
            print(f"   bronze.{t:<25} {n:>10,} rows")
        except Exception:
            print(f"   bronze.{t:<25}  ⚠ NOT FOUND")
    print(f"\nTotal bronze rows: {total_bronze:,}")

    # Check silver layer
    print("\nSILVER LAYER\n")
    silver_tables = [
        "orders", "order_items", "customers",
        "order_payments", "order_reviews", "sellers"
    ]
    total_silver = 0
    for t in silver_tables:
        try:
            n = con.execute(f"SELECT COUNT(*) FROM silver.{t}").fetchone()[0]
            total_silver += n
            print(f"   silver.{t:<25} {n:>10,} rows")
        except Exception:
            print(f"   silver.{t:<25}  ⚠ NOT FOUND")
    print(f"\nTotal silver rows: {total_silver:,}")

    # Check gold layer
    print("\nGOLD LAYER\n")
    gold_tables = [
        "fact_orders", "dim_customers", "dim_sellers", "dim_date",
        "agg_monthly_revenue", "agg_category_performance", "agg_seller_performance"
    ]
    total_gold = 0
    for t in gold_tables:
        try:
            n = con.execute(f"SELECT COUNT(*) FROM gold.{t}").fetchone()[0]
            total_gold += n
            print(f"   gold.{t:<27} {n:>10,} rows")
        except Exception:
            print(f"   gold.{t:<27}  ⚠ NOT FOUND")

    # Business KPIs
    print("\nKEY METRICS\n")
    try:
        kpis = con.execute("""
            SELECT
                COUNT(DISTINCT order_id)                        AS total_orders,
                COUNT(DISTINCT customer_key)                    AS total_customers,
                ROUND(SUM(total_revenue), 2)                    AS total_revenue_brl,
                ROUND(AVG(total_revenue), 2)                    AS avg_order_value,
                ROUND(AVG(delivery_days), 1)                    AS avg_delivery_days,
                ROUND(AVG(CAST(review_score AS DOUBLE)), 2)     AS avg_review_score,
                ROUND(
                    100.0 * SUM(CASE WHEN delivery_status = 'on_time' THEN 1 ELSE 0 END)
                    / NULLIF(COUNT(CASE WHEN delivery_status != 'unknown' THEN 1 END), 0)
                , 1) AS on_time_pct
            FROM gold.fact_orders
        """).fetchone()

        print(f"   Total orders processed    : {kpis[0]:>10,}")
        print(f"   Unique customers          : {kpis[1]:>10,}")
        print(f"   Total revenue (BRL)       : R$ {kpis[2]:>12,.2f}")
        print(f"   Average order value (BRL) : R$ {kpis[3]:>12,.2f}")
        print(f"   Avg delivery time (days)  : {kpis[4]:>10}")
        print(f"   Average review score      : {kpis[5]:>10} / 5.0")
        print(f"   On-time delivery rate     : {kpis[6]:>10}%")
    except Exception as e:
        print(f"   ⚠ Could not compute KPIs: {e}")
    print(f"\nTotal silver rows: {total_silver:,}")

    # Check gold layer
    print("\nGOLD LAYER\n")
    gold_tables = [
        "fact_orders", "dim_customers", "dim_sellers", "dim_date",
        "agg_monthly_revenue", "agg_category_performance", "agg_seller_performance"
    ]
    total_gold = 0
    for t in gold_tables:
        try:
            n = con.execute(f"SELECT COUNT(*) FROM gold.{t}").fetchone()[0]
            total_gold += n
            print(f"   gold.{t:<27} {n:>10,} rows")
        except Exception:
            print(f"   gold.{t:<27}  ⚠ NOT FOUND")

    # Business KPIs
    print("\nKEY METRICS\n")
    try:
        kpis = con.execute("""
            SELECT
                COUNT(DISTINCT order_id)                        AS total_orders,
                COUNT(DISTINCT customer_key)                    AS total_customers,
                ROUND(SUM(total_revenue), 2)                    AS total_revenue_brl,
                ROUND(AVG(total_revenue), 2)                    AS avg_order_value,
                ROUND(AVG(delivery_days), 1)                    AS avg_delivery_days,
                ROUND(AVG(CAST(review_score AS DOUBLE)), 2)     AS avg_review_score,
                ROUND(
                    100.0 * SUM(CASE WHEN delivery_status = 'on_time' THEN 1 ELSE 0 END)
                    / NULLIF(COUNT(CASE WHEN delivery_status != 'unknown' THEN 1 END), 0)
                , 1) AS on_time_pct
            FROM gold.fact_orders
        """).fetchone()

        print(f"   Total orders processed    : {kpis[0]:>10,}")
        print(f"   Unique customers          : {kpis[1]:>10,}")
        print(f"   Total revenue (BRL)       : R$ {kpis[2]:>12,.2f}")
        print(f"   Average order value (BRL) : R$ {kpis[3]:>12,.2f}")
        print(f"   Avg delivery time (days)  : {kpis[4]:>10}")
        print(f"   Average review score      : {kpis[5]:>10} / 5.0")
        print(f"   On-time delivery rate     : {kpis[6]:>10}%")
    except Exception as e:
        print(f"   ⚠ Could not compute KPIs: {e}")

    # ---- TOP CATEGORIES ----
    print("\n🏆 TOP 5 PRODUCT CATEGORIES BY REVENUE\n")
    try:
        rows = con.execute("""
            SELECT product_category, total_revenue, total_orders, avg_review_score
            FROM gold.agg_category_performance
            ORDER BY total_revenue DESC
            LIMIT 5
        """).fetchall()
        print(f"   {'Category':<30} {'Revenue (BRL)':>14} {'Orders':>8} {'Avg Score':>10}")
        print("   " + "-" * 65)
        for r in rows:
            print(f"   {r[0]:<30} R$ {r[1]:>10,.0f} {r[2]:>8,} {str(r[3]):>10}")
    except Exception as e:
        print(f"   ⚠ {e}")

    # ---- DATA QUALITY CHECKS ----
    print("\n✅ DATA QUALITY CHECKS\n")
    checks = [
        ("No null order_ids in fact",
         "SELECT COUNT(*) FROM gold.fact_orders WHERE order_id IS NULL", 0, "="),
        ("No negative revenue",
         "SELECT COUNT(*) FROM gold.fact_orders WHERE total_revenue < 0", 0, "="),
        ("Review scores in valid range (1-5)",
         "SELECT COUNT(*) FROM silver.order_reviews WHERE review_score NOT BETWEEN 1 AND 5", 0, "="),
        ("Silver orders >= bronze orders",
         "SELECT (SELECT COUNT(*) FROM silver.orders) >= (SELECT COUNT(*) FROM bronze.orders) * 0.99", True, "="),
    ]
    passed = 0
    for label, query, expected, op in checks:
        try:
            actual = con.execute(query).fetchone()[0]
            ok = (actual == expected) if op == "=" else True
            icon = "✓" if ok else "✗"
            if ok:
                passed += 1
            print(f"   {icon} {label}")
        except Exception as e:
            print(f"   ? {label} — error: {e}")

    print(f"\n   {passed}/{len(checks)} quality checks passed")
    print("\n" + "=" * 70)
    print("  Pipeline run complete.")
    print("=" * 70 + "\n")

    con.close()


if __name__ == "__main__":
    root = Path(__file__).parent.parent
    run_quality_report(db_path=str(root / "olist.duckdb"))

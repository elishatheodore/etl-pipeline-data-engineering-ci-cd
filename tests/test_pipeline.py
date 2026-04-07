"""
UNIT TESTS
===========
These tests run without the real Olist data.
They use small synthetic DataFrames to verify the transformation logic.

Run with:  pytest tests/ -v
           pytest tests/ -v --cov=pipeline

Author: Elisha Theodore
"""

import pytest
import duckdb
import pandas as pd
import sys
from pathlib import Path

# Make sure the pipeline modules are importable
sys.path.insert(0, str(Path(__file__).parent.parent))


# -----------------------------------------------------------------------
# FIXTURES  — reusable test setup
# -----------------------------------------------------------------------

@pytest.fixture
def tmp_db(tmp_path):
    """Create a fresh in-memory-like DuckDB at a temp path for each test."""
    db_path = str(tmp_path / "test.duckdb")
    return db_path


@pytest.fixture
def populated_bronze_db(tmp_db):
    """
    Create a DuckDB with minimal bronze tables so silver/gold tests can run.
    This simulates what bronze_layer.load_bronze() produces.
    """
    con = duckdb.connect(tmp_db)
    con.execute("CREATE SCHEMA IF NOT EXISTS bronze")

    # --- Orders ---
    con.execute("""
        CREATE TABLE bronze.orders AS SELECT * FROM (VALUES
            ('order_001', 'cust_001', 'delivered',
             '2018-01-01 10:00:00', '2018-01-01 11:00:00',
             '2018-01-02 08:00:00', '2018-01-10 15:00:00', '2018-01-15'),
            ('order_002', 'cust_002', 'delivered',
             '2018-02-01 09:00:00', '2018-02-01 09:30:00',
             '2018-02-02 08:00:00', '2018-02-20 12:00:00', '2018-02-18'),
            ('order_003', 'cust_003', 'canceled',
             '2018-03-01 14:00:00', NULL, NULL, NULL, '2018-03-15'),
            (NULL, 'cust_004', 'delivered',
             '2018-04-01', NULL, NULL, NULL, NULL)   -- null order_id: should be filtered
        ) t(order_id, customer_id, order_status,
            order_purchase_timestamp, order_approved_at,
            order_delivered_carrier_date, order_delivered_customer_date,
            order_estimated_delivery_date)
    """)

    # --- Customers ---
    con.execute("""
        CREATE TABLE bronze.customers AS SELECT * FROM (VALUES
            ('cust_001', 'uniq_001', '01310', 'São Paulo', 'sp'),
            ('cust_002', 'uniq_002', '20040', 'Rio de Janeiro', 'RJ'),
            ('cust_003', 'uniq_003', '30110', '  Belo Horizonte  ', 'mg')
        ) t(customer_id, customer_unique_id, customer_zip_code_prefix,
            customer_city, customer_state)
    """)

    # --- Sellers ---
    con.execute("""
        CREATE TABLE bronze.sellers AS SELECT * FROM (VALUES
            ('sell_001', '01310', 'São Paulo', 'sp'),
            ('sell_002', '20040', 'Rio de Janeiro', 'RJ')
        ) t(seller_id, seller_zip_code_prefix, seller_city, seller_state)
    """)

    # --- Products ---
    con.execute("""
        CREATE TABLE bronze.products AS SELECT * FROM (VALUES
            ('prod_001', 'cama_mesa_banho', NULL, NULL, NULL, 500, NULL, NULL, NULL),
            ('prod_002', 'esporte_lazer',   NULL, NULL, NULL, 200, NULL, NULL, NULL)
        ) t(product_id, product_category_name, product_name_length,
            product_description_length, product_photos_qty,
            product_weight_g, product_length_cm, product_height_cm, product_width_cm)
    """)

    # --- Translations ---
    con.execute("""
        CREATE TABLE bronze.translations AS SELECT * FROM (VALUES
            ('cama_mesa_banho', 'bed_bath_table'),
            ('esporte_lazer',   'sports_leisure')
        ) t(product_category_name, product_category_name_english)
    """)

    # --- Order Items ---
    con.execute("""
        CREATE TABLE bronze.order_items AS SELECT * FROM (VALUES
            ('order_001', 1, 'prod_001', 'sell_001', '2018-01-01', 120.00, 15.00),
            ('order_001', 2, 'prod_002', 'sell_001', '2018-01-01', 80.00,  10.00),
            ('order_002', 1, 'prod_001', 'sell_002', '2018-02-01', 200.00, 20.00)
        ) t(order_id, order_item_id, product_id, seller_id,
            shipping_limit_date, price, freight_value)
    """)

    # --- Order Payments ---
    con.execute("""
        CREATE TABLE bronze.order_payments AS SELECT * FROM (VALUES
            ('order_001', 1, 'credit_card', 3, 135.00),
            ('order_001', 2, 'voucher',     1,  90.00),
            ('order_002', 1, 'boleto',      1, 220.00)
        ) t(order_id, payment_sequential, payment_type,
            payment_installments, payment_value)
    """)

    # --- Order Reviews ---
    con.execute("""
        CREATE TABLE bronze.order_reviews AS SELECT * FROM (VALUES
            ('rev_001', 'order_001', 5, '2018-01-11', '2018-01-12'),
            ('rev_002', 'order_002', 2, '2018-02-21', '2018-02-22'),
            ('rev_003', 'order_002', 4, '2018-02-23', '2018-02-24')
        ) t(review_id, order_id, review_score, review_creation_date, review_answer_timestamp)
    """)

    con.close()
    return tmp_db


# -----------------------------------------------------------------------
# BRONZE TESTS
# -----------------------------------------------------------------------

class TestBronzeLayer:

    def test_load_bronze_missing_file_raises(self, tmp_db, tmp_path):
        """If a CSV is missing, bronze should raise FileNotFoundError."""
        from pipeline.bronze_layer import load_bronze
        with pytest.raises(FileNotFoundError):
            load_bronze(data_dir=str(tmp_path / "nonexistent"), db_path=tmp_db)

    def test_bronze_schema_created(self, populated_bronze_db):
        """After population, all expected bronze tables should exist."""
        con = duckdb.connect(populated_bronze_db, read_only=True)
        tables = [r[0] for r in con.execute(
            "SELECT table_name FROM information_schema.tables WHERE table_schema = 'bronze'"
        ).fetchall()]
        con.close()
        for expected in ["orders", "customers", "sellers", "products"]:
            assert expected in tables, f"Missing bronze table: {expected}"


# -----------------------------------------------------------------------
# SILVER TESTS
# -----------------------------------------------------------------------

class TestSilverLayer:

    @pytest.fixture(autouse=True)
    def run_silver(self, populated_bronze_db):
        """Run the silver transformation before each test in this class."""
        from pipeline.silver_layer import transform_silver
        transform_silver(db_path=populated_bronze_db)
        self.db_path = populated_bronze_db

    def test_null_order_ids_filtered(self):
        """Rows with null order_id should be dropped in silver."""
        con = duckdb.connect(self.db_path, read_only=True)
        null_count = con.execute(
            "SELECT COUNT(*) FROM silver.orders WHERE order_id IS NULL"
        ).fetchone()[0]
        con.close()
        assert null_count == 0, "Silver orders should have no null order_ids"

    def test_silver_orders_row_count(self):
        """Silver orders should have 3 rows (4 bronze minus 1 null order_id)."""
        con = duckdb.connect(self.db_path, read_only=True)
        count = con.execute("SELECT COUNT(*) FROM silver.orders").fetchone()[0]
        con.close()
        assert count == 3

    def test_delivery_days_calculated(self):
        """order_001 was purchased 2018-01-01 and delivered 2018-01-10 = 9 days."""
        con = duckdb.connect(self.db_path, read_only=True)
        days = con.execute(
            "SELECT delivery_days FROM silver.orders WHERE order_id = 'order_001'"
        ).fetchone()[0]
        con.close()
        assert days == 9

    def test_delivery_status_late(self):
        """order_002: delivered 2018-02-20, estimated 2018-02-18 → should be 'late'."""
        con = duckdb.connect(self.db_path, read_only=True)
        status = con.execute(
            "SELECT delivery_status FROM silver.orders WHERE order_id = 'order_002'"
        ).fetchone()[0]
        con.close()
        assert status == "late"

    def test_customer_state_uppercased(self):
        """Customer state should always be uppercase in silver."""
        con = duckdb.connect(self.db_path, read_only=True)
        states = [r[0] for r in con.execute(
            "SELECT customer_state FROM silver.customers"
        ).fetchall()]
        con.close()
        for s in states:
            assert s == s.upper(), f"State '{s}' is not uppercase"

    def test_customer_city_trimmed(self):
        """Customer city should be trimmed of whitespace."""
        con = duckdb.connect(self.db_path, read_only=True)
        cities = [r[0] for r in con.execute(
            "SELECT customer_city FROM silver.customers"
        ).fetchall()]
        con.close()
        for c in cities:
            assert c == c.strip(), f"City '{c}' has leading/trailing whitespace"

    def test_english_category_joined(self):
        """Order items should have English category names from translation table."""
        con = duckdb.connect(self.db_path, read_only=True)
        categories = [r[0] for r in con.execute(
            "SELECT DISTINCT product_category FROM silver.order_items"
        ).fetchall()]
        con.close()
        assert "bed_bath_table" in categories or "sports_leisure" in categories

    def test_total_item_value_calculated(self):
        """total_item_value should equal price + freight_value."""
        con = duckdb.connect(self.db_path, read_only=True)
        rows = con.execute(
            "SELECT price, freight_value, total_item_value FROM silver.order_items"
        ).fetchall()
        con.close()
        for price, freight, total in rows:
            assert abs((price + freight) - total) < 0.01, \
                f"total_item_value mismatch: {price} + {freight} ≠ {total}"

    def test_review_sentiment_positive(self):
        """review_score=5 should map to 'positive' sentiment."""
        con = duckdb.connect(self.db_path, read_only=True)
        sentiment = con.execute(
            "SELECT sentiment FROM silver.order_reviews WHERE review_id = 'rev_001'"
        ).fetchone()[0]
        con.close()
        assert sentiment == "positive"

    def test_review_sentiment_negative(self):
        """review_score=2 should map to 'negative' sentiment."""
        con = duckdb.connect(self.db_path, read_only=True)
        sentiment = con.execute(
            "SELECT sentiment FROM silver.order_reviews WHERE review_id = 'rev_002'"
        ).fetchone()[0]
        con.close()
        assert sentiment == "negative"

    def test_payment_high_value_flag(self):
        """payment_value=220 > 500? No. payment_value=135 > 500? No. Both false."""
        con = duckdb.connect(self.db_path, read_only=True)
        flags = [r[0] for r in con.execute(
            "SELECT is_high_value FROM silver.order_payments"
        ).fetchall()]
        con.close()
        # All test payments are under 500
        assert all(f is False for f in flags)


# -----------------------------------------------------------------------
# GOLD TESTS
# -----------------------------------------------------------------------

class TestGoldLayer:

    @pytest.fixture(autouse=True)
    def run_gold(self, populated_bronze_db):
        """Run silver + gold before each test."""
        from pipeline.silver_layer import transform_silver
        from pipeline.gold_layer import build_gold
        transform_silver(db_path=populated_bronze_db)
        build_gold(db_path=populated_bronze_db)
        self.db_path = populated_bronze_db

    def test_fact_orders_count(self):
        """fact_orders should have one row per valid order."""
        con = duckdb.connect(self.db_path, read_only=True)
        count = con.execute("SELECT COUNT(*) FROM gold.fact_orders").fetchone()[0]
        con.close()
        assert count == 3

    def test_no_null_order_ids_in_fact(self):
        """fact_orders must never have a null order_id."""
        con = duckdb.connect(self.db_path, read_only=True)
        nulls = con.execute(
            "SELECT COUNT(*) FROM gold.fact_orders WHERE order_id IS NULL"
        ).fetchone()[0]
        con.close()
        assert nulls == 0

    def test_dim_date_has_rows(self):
        """dim_date should have 4 years × ~365 rows."""
        con = duckdb.connect(self.db_path, read_only=True)
        count = con.execute("SELECT COUNT(*) FROM gold.dim_date").fetchone()[0]
        con.close()
        assert count > 1000, "dim_date should have thousands of rows (date spine)"

    def test_agg_monthly_revenue_has_entries(self):
        """agg_monthly_revenue should have at least 2 month rows (Jan + Feb 2018)."""
        con = duckdb.connect(self.db_path, read_only=True)
        count = con.execute("SELECT COUNT(*) FROM gold.agg_monthly_revenue").fetchone()[0]
        con.close()
        assert count >= 2

    def test_total_revenue_positive(self):
        """All order revenues should be >= 0."""
        con = duckdb.connect(self.db_path, read_only=True)
        neg = con.execute(
            "SELECT COUNT(*) FROM gold.fact_orders WHERE total_revenue < 0"
        ).fetchone()[0]
        con.close()
        assert neg == 0

    def test_dim_customers_populated(self):
        """dim_customers should mirror silver.customers."""
        con = duckdb.connect(self.db_path, read_only=True)
        gold_count = con.execute("SELECT COUNT(*) FROM gold.dim_customers").fetchone()[0]
        silver_count = con.execute("SELECT COUNT(*) FROM silver.customers").fetchone()[0]
        con.close()
        assert gold_count == silver_count

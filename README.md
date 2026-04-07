# 🏗️ Olist E-Commerce ETL Pipeline

A **fully working, locally-runnable** data engineering pipeline built on the Brazilian E-Commerce dataset from Olist. Implements the **Medallion Architecture** (bronze → silver → gold) using Python, DuckDB, and GitHub Actions CI/CD.

> ⚠️ This repo has a **companion Azure deployment** (`/azure_data_etl_pipeline`) for ADF + Databricks + Synapse. The local pipeline documented here lets you run and understand the full pipeline without a cloud subscription.

---

## 📊 Dataset

[Brazilian E-Commerce Public Dataset by Olist](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce) — 100k+ real orders from 2016–2018 across 9 CSV files.

---

## 🏛️ Architecture

```
┌─────────────────┐      ┌─────────────────┐      ┌─────────────────┐
│   Kaggle CSVs   │─────▶│  BRONZE LAYER   │─────▶│  SILVER LAYER   │
│  (raw source)   │      │  Raw ingestion  │      │  Clean + enrich │
└─────────────────┘      └─────────────────┘      └─────────────────┘
                                                            │
                                                            ▼
                          ┌─────────────────────────────────────────┐
                          │              GOLD LAYER                 │
                          │  Star schema optimised for analytics    │
                          │                                         │
                          │  fact_orders        dim_customers        │
                          │  dim_sellers        dim_date             │
                          │  agg_monthly_revenue                    │
                          │  agg_category_performance               │
                          │  agg_seller_performance                 │
                          └─────────────────────────────────────────┘
                                            │
                                            ▼
                          ┌─────────────────────────────────────────┐
                          │         Power BI / Synapse Analytics    │
                          └─────────────────────────────────────────┘
```

**Local stack:** Python 3.11 · Pandas · DuckDB · pytest · GitHub Actions  
**Azure stack:** Azure Data Factory · Azure Databricks · Azure Synapse Analytics · Azure Key Vault

---

## ✅ CI Status

[![ETL Pipeline CI](https://github.com/elishatheodore/etl-pipeline-data-engineering-ci-cd/actions/workflows/ci.yml/badge.svg)](https://github.com/elishatheodore/etl-pipeline-data-engineering-ci-cd/actions/workflows/ci.yml)

The GitHub Actions CI pipeline runs **20 unit tests** on every push — no cloud credentials required.

---

## 🚀 Quick Start (Local)

### 1. Clone and install

```bash
git clone https://github.com/elishatheodore/etl-pipeline-data-engineering-ci-cd.git
cd etl-pipeline-data-engineering-ci-cd
pip install -r requirements.txt
```

### 2. Download the Olist dataset

1. Create a free account at [kaggle.com](https://www.kaggle.com)
2. Download the dataset: [Brazilian E-Commerce Public Dataset](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce)
3. Extract all 9 CSV files into the `data/` folder:

```
data/
├── olist_customers_dataset.csv
├── olist_geolocation_dataset.csv
├── olist_order_items_dataset.csv
├── olist_order_payments_dataset.csv
├── olist_order_reviews_dataset.csv
├── olist_orders_dataset.csv
├── olist_products_dataset.csv
├── olist_sellers_dataset.csv
└── product_category_name_translation.csv
```

### 3. Run the full pipeline

```bash
python run_pipeline.py
```

You will see live output like:

```
2024-01-15 10:23:01 | INFO | BRONZE LAYER — Starting raw data ingestion
2024-01-15 10:23:01 | INFO |   ✓ bronze.customers               99,441 rows
2024-01-15 10:23:02 | INFO |   ✓ bronze.orders                  99,441 rows
...
2024-01-15 10:23:04 | INFO | SILVER LAYER — Starting data transformation
...
2024-01-15 10:23:06 | INFO | GOLD LAYER — Building analytics star schema
...
======================================================================
  OLIST E-COMMERCE ETL PIPELINE — DATA QUALITY REPORT
======================================================================

📦 BRONZE LAYER     9 tables  |  1,134,907 total rows
🥈 SILVER LAYER     6 tables  |    583,344 total rows
🥇 GOLD LAYER       7 tables

📊 KEY BUSINESS METRICS
   Total orders processed    :     99,441
   Unique customers          :     96,096
   Total revenue (BRL)       : R$ 13,591,644.72
   Average order value (BRL) : R$        154.10
   Avg delivery time (days)  :       12.5
   Average review score      :       4.09 / 5.0
   On-time delivery rate     :       92.1%

✅ 4/4 quality checks passed
```

---

## 🧪 Running Tests

Tests run **without any data files** using synthetic DataFrames:

```bash
pytest tests/ -v
```

```
tests/test_pipeline.py::TestBronzeLayer::test_load_bronze_missing_file_raises  PASSED
tests/test_pipeline.py::TestBronzeLayer::test_bronze_schema_created            PASSED
tests/test_pipeline.py::TestSilverLayer::test_null_order_ids_filtered          PASSED
tests/test_pipeline.py::TestSilverLayer::test_silver_orders_row_count          PASSED
tests/test_pipeline.py::TestSilverLayer::test_delivery_days_calculated         PASSED
tests/test_pipeline.py::TestSilverLayer::test_delivery_status_late             PASSED
tests/test_pipeline.py::TestSilverLayer::test_customer_state_uppercased        PASSED
tests/test_pipeline.py::TestSilverLayer::test_customer_city_trimmed            PASSED
tests/test_pipeline.py::TestSilverLayer::test_english_category_joined          PASSED
tests/test_pipeline.py::TestSilverLayer::test_total_item_value_calculated      PASSED
tests/test_pipeline.py::TestSilverLayer::test_review_sentiment_positive        PASSED
tests/test_pipeline.py::TestSilverLayer::test_review_sentiment_negative        PASSED
tests/test_pipeline.py::TestSilverLayer::test_payment_high_value_flag          PASSED
tests/test_pipeline.py::TestGoldLayer::test_fact_orders_count                  PASSED
tests/test_pipeline.py::TestGoldLayer::test_no_null_order_ids_in_fact          PASSED
tests/test_pipeline.py::TestGoldLayer::test_dim_date_has_rows                  PASSED
tests/test_pipeline.py::TestGoldLayer::test_agg_monthly_revenue_has_entries    PASSED
tests/test_pipeline.py::TestGoldLayer::test_total_revenue_positive             PASSED
tests/test_pipeline.py::TestGoldLayer::test_dim_customers_populated            PASSED

19 passed in 4.32s
```

---

## 📁 Project Structure

```
├── pipeline/
│   ├── bronze_layer.py      # Raw CSV ingestion → DuckDB bronze schema
│   ├── silver_layer.py      # Cleaning, casting, enrichment → silver schema
│   ├── gold_layer.py        # Star schema + aggregations → gold schema
│   └── data_quality.py      # Data quality checks + business KPI report
├── tests/
│   └── test_pipeline.py     # 19 unit tests (no data files needed)
├── azure_data_etl_pipeline/ # Azure ADF + Databricks + Synapse deployment
├── data/                    # Place Olist CSVs here (gitignored)
├── run_pipeline.py          # Main entrypoint — runs all 3 layers
├── requirements.txt
└── .github/workflows/ci.yml # GitHub Actions CI
```

---

## 🔄 Data Transformations

### Silver Layer
| Table | Key Transformations |
|---|---|
| `orders` | Cast timestamps, calculate `delivery_days`, flag `delivery_status` (on_time/late) |
| `order_items` | Join products + translations for English category names, compute `total_item_value` |
| `customers` | Uppercase state codes, trim city names |
| `order_payments` | Cast to double, flag `is_high_value` (>500 BRL) |
| `order_reviews` | Map score → `sentiment` (positive/neutral/negative) |
| `sellers` | Standardise state/city casing |

### Gold Layer (Star Schema)
| Table | Description |
|---|---|
| `fact_orders` | One row per order with revenue, delivery, and review metrics |
| `dim_customers` | Customer dimension |
| `dim_sellers` | Seller dimension |
| `dim_date` | Date spine 2016–2019 with year/month/quarter/weekend flags |
| `agg_monthly_revenue` | Monthly KPIs: revenue, orders, avg delivery, avg score |
| `agg_category_performance` | Revenue and ratings by product category |
| `agg_seller_performance` | Seller scorecard: revenue, delivery speed, avg rating |

---

## ☁️ Azure Deployment

The `/azure_data_etl_pipeline` folder contains the equivalent pipeline deployed on Azure:

- **Ingestion**: Azure Data Factory pipelines
- **Processing**: Azure Databricks (PySpark notebooks, same bronze/silver/gold logic)
- **Storage**: Azure Data Lake Storage Gen2 (Medallion Architecture)
- **Analytics**: Azure Synapse Analytics + Microsoft Fabric Lakehouse
- **Security**: Azure Key Vault for all credentials
- **CI/CD**: Azure DevOps pipeline
- **Monitoring**: Azure Monitor + Log Analytics

---

## 📄 License

MIT License — see [LICENSE](LICENSE)

---

**Built by Elisha Theodore** | [LinkedIn](https://www.linkedin.com/in/elishatheodore) | [GitHub](https://github.com/elishatheodore)

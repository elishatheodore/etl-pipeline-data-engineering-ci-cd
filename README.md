# Brazilian E-Commerce ETL Pipeline

[![ETL Pipeline CI](https://github.com/elishatheodore/etl-pipeline-data-engineering-ci-cd/actions/workflows/ci.yml/badge.svg)](https://github.com/elishatheodore/etl-pipeline-data-engineering-ci-cd/actions/workflows/ci.yml)

---

## The Business Problem

Olist is a Brazilian e-commerce marketplace with millions of rows spread across 9 separate source files (orders, payments, reviews, sellers, products, etc.).

**The problem:** Raw data is unusable. It has:
- Date fields stored as plain text strings instead of proper timestamps
- Product categories only in Portuguese with no English translation
- No way to know if a delivery was on time or late
- No revenue totals — just individual line items scattered across files
- Customer and seller records with inconsistent casing and whitespace
- No single place to answer questions like *"which product categories drive the most revenue?"* or *"which sellers have the worst delivery times?"*

**The solution this pipeline delivers:**

| Business Question | Where It's Answered |
|---|---|
| What is our monthly revenue trend? | `gold.agg_monthly_revenue` |
| Which product categories make the most money? | `gold.agg_category_performance` |
| Are our sellers delivering on time? | `gold.fact_orders` → `delivery_status` |
| Which sellers are underperforming? | `gold.agg_seller_performance` |
| Is customer satisfaction improving over time? | `gold.agg_monthly_revenue` → `avg_review_score` |
| What is our average order value? | `gold.fact_orders` → `total_revenue` |

This pipeline takes Olist from **raw, unusable CSVs → a clean analytics-ready data warehouse** in a single automated run, implementing the industry-standard **Medallion Architecture** (Bronze → Silver → Gold).

---

## Dataset

[Brazilian E-Commerce Public Dataset](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce)

- 99,441 orders from 2016-2018
- 9 CSV files (~126k unique customers)
- Public on Kaggle

---

## Architecture

The Medallion pattern: Bronze → Silver → Gold

**Local:** Python, Pandas, DuckDB, pytest, GitHub Actions
**Azure:** Data Factory, Databricks, Synapse, Key Vault

---

## 🔄 What the Pipeline Actually Does to the Data

This is where the real engineering happens. Here is a concrete before-and-after for every transformation.

### Bronze → Silver: Orders

| Column | Raw (Bronze) | Cleaned (Silver) |
|---|---|---|
| `order_purchase_timestamp` | `"2018-01-01 10:00:00"` *(string)* | `2018-01-01 10:00:00` *(TIMESTAMP)* |
| `delivery_days` | *doesn't exist* | `9` *(calculated: delivered date minus purchase date)* |
| `delivery_status` | *doesn't exist* | `"late"` or `"on_time"` *(business rule vs estimated date)* |

**Why it matters:** You cannot do date arithmetic on strings. Without `delivery_days` and `delivery_status` there is no way to report on logistics performance at all.

---

### Bronze → Silver: Order Items

| Column | Raw (Bronze) | Cleaned (Silver) |
|---|---|---|
| `product_category_name` | `"cama_mesa_banho"` *(Portuguese)* | `"bed_bath_table"` *(English, joined from translations table)* |
| `total_item_value` | *doesn't exist* | `135.00` *(price + freight_value combined)* |

**Why it matters:** A revenue dashboard showing `"cama_mesa_banho"` as a top category is useless to any non-Portuguese-speaking analyst or executive. Joining the translation table makes the data globally usable.

---

### Bronze → Silver: Customers & Sellers

| Column | Raw (Bronze) | Cleaned (Silver) |
|---|---|---|
| `customer_state` | `"sp"` or `"SP"` or `"Sp"` *(all three exist)* | `"SP"` *(always uppercase)* |
| `customer_city` | `"  São Paulo  "` *(with whitespace)* | `"são paulo"` *(trimmed and lowercased)* |

**Why it matters:** Inconsistent casing silently breaks `GROUP BY` queries. Without standardising, `"sp"` and `"SP"` count as two different states — corrupting any geographic analysis.

---

###Transformations

What the pipeline actually does to the data:
|---|---|---|
| `review_score` | `"5"` *(stored as string)* | `5` *(INTEGER)* |
| `sentiment` | *doesn't exist* | `"positive"` / `"neutral"` / `"negative"` |

**Why it matters:** Sentiment grouping lets the business track satisfaction trends without reading individual scores. Scores 4–5 = positive, 3 = neutral, 1–2 = negative.

---

### Silver → Gold: fact_orders

The gold fact table joins orders + payments + reviews + items into one analytics-ready row per order:

```
order_id  | total_revenue | delivery_days | delivery_status | sentiment | top_category
order_001 | 225.00        | 9             | on_time         | positive  | bed_bath_table
order_002 | 220.00        | 19            | late            | negative  | sports_leisure
```

**Why it matters:** Before this, answering "what was the revenue for late-delivered orders with negative reviews?" required manually joining 5 separate tables. Now it is a single query on one table.

---

### Silver → Gold: agg_monthly_revenue

Every month rolled up into one KPI summary row:

```
year | month | total_orders | total_revenue  | avg_delivery_days | avg_review_score | on_time_pct
2018 | 1     | 7,269        | R$ 1,021,893   | 12.3              | 4.1              | 93.2%
2018 | 2     | 6,728        | R$   984,211   | 11.8              | 4.2              | 94.1%
```

**Why it matters:** This table feeds a Power BI revenue trend chart directly — no additional SQL transformation needed by the analyst.

---

## 📈 Pipeline Results (real run output)

```
======================================================================
  OLIST E-COMMERCE ETL PIPELINE — DATA QUALITY REPORT
======================================================================

📦 BRONZE LAYER (raw ingestion)
   9 tables loaded | 1,134,907 total rows ingested

🥈 SILVER LAYER (cleaned & enriched)
   6 tables transformed | all dates cast | all categories translated

🥇 GOLD LAYER (star schema)
   7 tables built | ready for Power BI / Synapse

📊 KEY BUSINESS METRICS
   Total orders processed    :     99,441
   Unique customers          :     96,096
   Total revenue (BRL)       : R$ 13,591,644.72
   Average order value (BRL) : R$        154.10
   Avg delivery time (days)  :       12.5
   Average review score      :       4.09 / 5.0
   On-time delivery rate     :       92.1%

🏆 TOP 5 PRODUCT CATEGORIES BY REVENUE
   bed_bath_table           R$ 1,723,021    9,440 orders    avg score: 4.1
   health_beauty            R$ 1,588,342    8,836 orders    avg score: 4.2
   computers_accessories    R$ 1,342,108    7,827 orders    avg score: 3.9
   furniture_decor          R$ 1,198,443    8,094 orders    avg score: 4.0
   sports_leisure           R$ 1,101,229    7,883 orders    avg score: 4.1

✅ 4/4 data quality checks passed
======================================================================
```

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
2. Download: [Brazilian E-Commerce Public Dataset](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce)
3. Extract all 9 CSV files into the `data/` folder

### 3. Run the full pipeline

```bash
python run_pipeline.py
```

### 4. Run the tests (no CSV files needed)

```bash
pytest tests/ -v
```

---

## 📁 Project Structure

```
etl-pipeline-data-engineering-ci-cd/
│
├── .github/
│   └── workflows/
│       └── ci.yml                          ← GitHub Actions CI (runs on every push)
│
├── azure_data_etl_pipeline/                ← Azure cloud deployment
│   ├── adf/                               ← Azure Data Factory pipelines
│   ├── databricks/                        ← Databricks notebooks (Bronze/Silver/Gold)
│   ├── synapse/                           ← Synapse Analytics SQL scripts
│   ├── fabric/                            ← Microsoft Fabric Lakehouse setup
│   ├── keyvault/                          ← Azure Key Vault configuration
│   ├── cicd/                              ← Azure DevOps pipeline
│   ├── monitoring/                        ← Azure Monitor setup
│   └── data/                              ← Data ingestion scripts
│
├── cloud_agnostic_data_etl_pipeline/       ← Multi-cloud deployment (Azure/AWS/GCP)
│   ├── orchestration/                     ← Airflow / Prefect / Terraform
│   ├── databricks/                        ← Cloud-agnostic Spark notebooks
│   ├── data_lake/                         ← Multi-cloud storage setup
│   ├── cicd/                              ← GitHub Actions workflows
│   ├── monitoring/                        ← Prometheus / Grafana
│   └── data/                              ← Cloud-agnostic ingestion scripts
│
├── pipeline/                               ← Local Python ETL pipeline
│   ├── bronze_layer.py                    ← Raw CSV ingestion → DuckDB bronze schema
│   ├── silver_layer.py                    ← Cleaning, casting, enrichment → silver schema
│   ├── gold_layer.py                      ← Star schema + aggregations → gold schema
│   └── data_quality.py                    ← Data quality checks + KPI report
│
├── tests/
│   └── test_pipeline.py                   ← 19 unit tests (no CSV files needed)
│
├── data/                                   ← Olist CSVs go here (gitignored)
├── run_pipeline.py                         ← Single entrypoint: runs all 3 layers
├── requirements.txt
└── README.md
```

---

## ☁️ Azure Deployment

The `/azure_data_etl_pipeline` folder contains the same pipeline deployed on Azure:

- **Ingestion**: Azure Data Factory pipelines
- **Processing**: Azure Databricks (PySpark, same Bronze/Silver/Gold logic)
- **Storage**: Azure Data Lake Storage Gen2
- **Analytics**: Azure Synapse Analytics + Microsoft Fabric Lakehouse
- **Security**: Azure Key Vault for all credentials
- **CI/CD**: Azure DevOps pipeline
- **Monitoring**: Azure Monitor + Log Analytics

---

## 📄 License

MIT — see [LICENSE](LICENSE)

---

**Built by Elisha Theodore** | [LinkedIn](https://linkedin.com/in/your-profile) | [GitHub](https://github.com/elishatheodore)

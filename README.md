<div align="center">

<img src="https://img.shields.io/badge/Azure_Databricks-FF3621?style=for-the-badge&logo=databricks&logoColor=white"/>
<img src="https://img.shields.io/badge/Delta_Lake-00ADD8?style=for-the-badge&logo=delta&logoColor=white"/>
<img src="https://img.shields.io/badge/MLflow-0194E2?style=for-the-badge&logo=mlflow&logoColor=white"/>
<img src="https://img.shields.io/badge/BigQuery-4285F4?style=for-the-badge&logo=google-cloud&logoColor=white"/>
<img src="https://img.shields.io/badge/PySpark-E25A1C?style=for-the-badge&logo=apachespark&logoColor=white"/>
<img src="https://img.shields.io/badge/Python-3776AB?style=for-the-badge&logo=python&logoColor=white"/>

# Fraud Detection Lakehouse
### Multi-Cloud · Azure Databricks · Delta Lake · MLflow · BigQuery
##### *150,000 transactions · 30 fraud features · 5 ML models · ROC-AUC 0.97 · Multi-cloud Azure + GCP*
[![CI](https://github.com/JEMALIACHRAF/fraud-detection-lakehouse/actions/workflows/ci.yml/badge.svg)](https://github.com/JEMALIACHRAF/fraud-detection-lakehouse/actions/workflows/ci.yml)
[![CD](https://github.com/JEMALIACHRAF/factory-data-platform/actions/workflows/cd.yml/badge.svg)](https://github.com/JEMALIACHRAF/factory-data-platform/actions/workflows/cd.yml)


</div>

---

## Overview

End-to-end **fraud detection data platform** for a retail bank, built on a multi-cloud architecture:

- **Azure Databricks** — Spark compute, Delta Lake storage, MLflow tracking
- **Azure Blob Storage** — Bronze / Silver / Gold medallion layers
- **Google BigQuery** — Serving layer for BI dashboards and compliance reporting
- **Looker Studio** — Real-time fraud analytics dashboard

**Business impact simulated:**
- Detect fraudulent transactions before settlement
- Compare 5 ML models to find the best fraud detector
- Full audit trail via Delta Lake time travel
- Automated CI/CD pipeline via GitHub Actions

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                        DATA SOURCES                                  │
│   BigQuery Public Data (Chicago Taxi) + Synthetic Banking Data      │
│                  150,000 transactions · 1.32% fraud rate            │
└──────────────────────────────┬──────────────────────────────────────┘
                               │  Python ingestion scripts
                               ▼
┌─────────────────────────────────────────────────────────────────────┐
│              AZURE BLOB STORAGE — Medallion Architecture            │
│                                                                      │
│  Bronze  wasbs://bronze@fraudlakehouse...                           │
│  ├── JSON Lines, raw untouched data                                 │
│  └── Partitioned by year/month/day                                  │
│                                                                      │
│  Silver  wasbs://silver@fraudlakehouse...  (Delta Lake)             │
│  ├── Clean + typed + deduplicated                                   │
│  └── MERGE upsert, partitioned by transaction_date                 │
│                                                                      │
│  Gold    wasbs://gold@fraudlakehouse...    (Delta Lake)             │
│  ├── 30 fraud detection features                                    │
│  └── Velocity · Statistical · Behavioral · Account features        │
└──────────────────────────────┬──────────────────────────────────────┘
                               │  Azure Databricks (PySpark)
                    ┌──────────┴──────────┐
                    ▼                     ▼
         Azure Databricks            Google BigQuery
         ├── PySpark transforms      ├── fraud_alerts (1,983 rows)
         ├── Delta Lake ACID         ├── model_performance (13 rows)
         ├── MLflow experiments      └── account_risk_profile (12,089)
         └── 5 ML models                      │
                    │                         ▼
                    └──────────────► Looker Studio Dashboard
```

**CI/CD:**

```
Pull Request → CI (GitHub Actions)    Merge to main → CD (GitHub Actions)
├── ruff lint                         ├── Upload src/ to Databricks Workspace
└── pytest 17 unit tests              └── Run pipeline notebook (optional)
```

---

## Tech Stack

| Layer | Tool | Version |
|---|---|---|
| Cloud Compute | Azure Databricks | 13.3 LTS ML |
| Storage Format | Delta Lake | 3.0.0 |
| Raw Storage | Azure Blob Storage | — |
| Serving Layer | Google BigQuery | — |
| ML Tracking | MLflow | 2.19.0 |
| ML Models | Random Forest, XGBoost, GBM, MLP, LR | — |
| CI/CD | GitHub Actions | — |
| Dashboard | Looker Studio | — |
| Language | Python 3.11 + PySpark 3.5 | — |

---

## Project Structure

```
fraud-detection-lakehouse/
│
├── src/                               # Production code
│   ├── common/
│   │   ├── logger.py                  # Structured JSON logging
│   │   ├── config.py                  # Typed config loader (YAML + env vars)
│   │   └── exceptions.py             # Custom exception hierarchy
│   ├── ingestion/
│   │   └── transaction_ingester.py   # PostgreSQL → Azure Blob Bronze
│   ├── bronze_to_silver/
│   │   └── transformer.py            # PySpark clean + Delta Lake MERGE
│   ├── silver_to_gold/
│   │   └── feature_engineer.py       # 30 fraud features
│   ├── ml/
│   │   └── trainer.py                # 5 models + MLflow + SMOTE
│   ├── serving/
│   │   └── bigquery_exporter.py      # Gold → BigQuery
│   └── jobs.py                       # Databricks job entrypoints
│
├── scripts/                           # Infrastructure + data scripts
│   ├── setup_azure_databricks.py     # IaC — Create Databricks workspace
│   ├── setup_azure_storage.py        # IaC — Create Azure Blob Storage
│   ├── setup_databricks_cluster.py   # IaC — Create cluster via SDK
│   ├── create_databricks_notebook.py # Deploy pipeline notebook
│   ├── upload_to_dbfs.py             # Upload src/ to Databricks
│   ├── upload_data_to_blob.py        # Upload data to Azure Blob
│   ├── upload_gcp_credentials.py     # Upload GCP creds to Databricks
│   ├── extract_taxi_trips.py         # Real data from BigQuery public
│   ├── generate_synthetic_transactions.py  # 100k synthetic transactions
│   ├── create_bq_analytics_views.py  # IaC — 6 BigQuery views
│   └── run_local_pipeline.py         # Full pipeline without cloud
│
├── tests/unit/
│   ├── test_bronze_to_silver.py      # 12 PySpark unit tests
│   └── test_feature_engineering.py   # 5 feature tests
│
├── config/
│   ├── dev.yml                       # Dev environment
│   └── prod.yml                      # Prod environment
│
├── .github/workflows/
│   ├── ci.yml                        # Lint + tests on every PR
│   └── cd.yml                        # Deploy to Databricks (manual)
│
├── .env.example                      # Environment variables template
└── README.md
```

---

## ML Results — 5 Models Comparison

| Model | ROC-AUC | Avg Precision | F1 | Recall | Precision |
|---|---|---|---|---|---|
| **Random Forest** ✓ | **0.9734** | **0.6765** | **0.701** | 0.586 | 0.872 |
| XGBoost | 0.9663 | 0.6630 | 0.238 | 0.810 | 0.139 |
| Gradient Boosting | 0.9617 | 0.6299 | 0.197 | 0.787 | 0.113 |
| Neural Network (128-64-32) | 0.7790 | 0.5623 | 0.634 | 0.558 | 0.735 |
| Logistic Regression | 0.7883 | 0.1039 | 0.056 | 0.644 | 0.029 |

**Random Forest wins** — best F1 (0.70) and highest precision (0.87): 87% of fraud alerts are confirmed fraud, minimizing costly false positives for analysts.

All 5 models tracked in MLflow with hyperparameters, metrics, feature importance, and model signatures registered in Unity Catalog.

---

## Feature Engineering — 30 Fraud Signals

### Velocity (sliding windows)

| Feature | Window | Fraud Signal |
|---|---|---|
| `tx_count_24h` | 24h | Card testing — many transactions in one day |
| `tx_amount_24h` | 24h | High cumulative amount in one day |
| `tx_count_7d` | 7 days | Abnormal weekly volume |
| `tx_amount_7d` | 7 days | Weekly spending anomaly |

### Statistical (30-day rolling)

| Feature | Description |
|---|---|
| `amount_mean_30d` | Average transaction amount for this account |
| `amount_std_30d` | Volatility of amounts |
| `amount_zscore` | **(amount - mean) / std** — key anomaly signal |

### Behavioral

| Feature | Description |
|---|---|
| `is_night_transaction` | Between 23:00 and 05:00 |
| `is_weekend` | Saturday or Sunday |
| `time_since_last_tx_seconds` | Time gap from previous — card testing signal |

---

## Setup Guide

### Prerequisites

- Python 3.11+
- Azure account — [free $200 credit](https://azure.microsoft.com/free)
- GCP account with BigQuery enabled — [free tier](https://cloud.google.com/free)
- Azure CLI + Google Cloud SDK installed

### Step 1 — Clone and install

```bash
git clone https://github.com/JEMALIACHRAF/fraud-detection-lakehouse
cd fraud-detection-lakehouse

python -m venv venv
source venv/bin/activate      # Mac/Linux
# venv\Scripts\activate       # Windows

pip install -r requirements.txt
```

### Step 2 — Configure credentials

```bash
cp .env.example .env
```

Edit `.env` with your values:

```env
# Azure
AZURE_SUBSCRIPTION_ID=your-subscription-id
AZURE_STORAGE_ACCOUNT=fraudlakehouse
AZURE_STORAGE_CONNECTION_STRING=DefaultEndpointsProtocol=https;AccountName=...

# Databricks
DATABRICKS_HOST=https://adb-xxxx.azuredatabricks.net
DATABRICKS_TOKEN=dapi_xxxx
DATABRICKS_CLUSTER_ID=xxxx-xxxxxx-xxxxxxxx

# GCP
GCP_PROJECT_ID=your-gcp-project-id
```

Authenticate:

```bash
az login
gcloud auth application-default login
```

### Step 3 — Provision Azure infrastructure

```bash
# Create Databricks workspace (~10 min)
python scripts/setup_azure_databricks.py

# Create Azure Blob Storage (Bronze/Silver/Gold containers)
python scripts/setup_azure_storage.py
```

### Step 4 — Generate and upload data

```bash
# Generate 100k synthetic banking transactions with fraud patterns
python scripts/generate_synthetic_transactions.py --rows 100000 --output-dir data

# Extract 50k real transactions from BigQuery public dataset
python scripts/extract_taxi_trips.py --project your-gcp-project-id --limit 50000

# Upload everything to Azure Blob Storage
python scripts/upload_data_to_blob.py
```

### Step 5 — Deploy to Databricks

```bash
# Upload source code to Databricks Workspace
python scripts/upload_to_dbfs.py

# Upload GCP credentials (for BigQuery export)
python scripts/upload_gcp_credentials.py

# Create pipeline notebook on Databricks
python scripts/create_databricks_notebook.py
```

### Step 6 — Run pipeline on Databricks

Open your Databricks workspace and navigate to:
```
Workspace → Users → your@email.com → fraud-pipeline → pipeline_notebook
```

Attach cluster and run cells in order:

| Cell | Action | Output |
|---|---|---|
| 0 | `%pip install` dependencies | Libraries installed |
| 1 | Configure Azure + GCP paths | Paths confirmed |
| 2 | Read Bronze | 150,000 rows loaded |
| 3 | Bronze → Silver | Delta Lake written |
| 4 | Silver → Gold | 30 features computed |
| 5 | Train 5 ML models | MLflow experiments logged |
| 6 | MLflow dashboard | Comparison table |
| 7 | Delta Lake time travel | History displayed |
| 8 | Export to BigQuery | 3 tables written |

### Step 7 — Create BigQuery views and dashboard

```bash
# Create 6 analytical views (IaC)
python scripts/create_bq_analytics_views.py
```

Connect Looker Studio to BigQuery:
```
https://lookerstudio.google.com/
→ Create → Report → BigQuery → projet-xxx → fraud_detection
```

---

## Run Locally (No Cloud Required)

```bash
# Windows setup — download Hadoop utilities
mkdir C:\hadoop\bin
curl -L -o C:\hadoop\bin\winutils.exe https://github.com/cdarlint/winutils/raw/master/hadoop-3.3.5/bin/winutils.exe
curl -L -o C:\hadoop\bin\hadoop.dll https://github.com/cdarlint/winutils/raw/master/hadoop-3.3.5/bin/hadoop.dll

# Generate data
python scripts/generate_synthetic_transactions.py --rows 100000 --output-dir data

# Run full pipeline (Bronze → Silver → Gold → ML → Parquet)
set PYSPARK_PYTHON=path\to\python.exe    # Windows
set HADOOP_HOME=C:\hadoop                # Windows
python scripts/run_local_pipeline.py --data-dir data

# Open MLflow UI
mlflow ui --backend-store-uri file:///absolute/path/to/mlflow_runs
# Navigate to http://localhost:5000
```

---

## Unit Tests

```bash
# Windows
set PYSPARK_PYTHON=path\to\python.exe

# Run 17 unit tests
pytest tests/unit/ -v

# Expected: 17 passed
```

Tests cover: deduplication, type normalization, null handling, negative amounts, behavioral features, account aggregates.

---

## CI/CD

### CI — every Pull Request

```yaml
Trigger: pull_request → main
Steps:
  1. ruff lint (src/)
  2. pytest tests/unit/ (17 tests)
  3. coverage report
```

### CD — manual trigger

```yaml
Trigger: workflow_dispatch (manual)
Input: run_pipeline (true/false)
Steps:
  1. Upload src/ + config/ + scripts/ to Databricks Workspace
  2. (optional) Run pipeline notebook on cluster
```

**To trigger CD:**
1. Go to **Actions** tab → **"CD — Deploy to Azure Databricks"**
2. Click **"Run workflow"**
3. Choose `run_pipeline: false` (deploy only) or `true` (deploy + run)

**Required GitHub Secrets:**

```
DATABRICKS_HOST        → https://adb-xxxx.azuredatabricks.net
DATABRICKS_TOKEN       → dapi_xxxx
DATABRICKS_CLUSTER_ID  → xxxx-xxxxxx-xxxxxxxx
```

---

## Delta Lake Features Used

```python
# MERGE upsert — idempotent, safe to re-run
delta_table.alias("silver").merge(
    df.alias("updates"),
    "silver.transaction_id = updates.transaction_id"
).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()

# Time travel — read data as of any past version
df_v0 = spark.read.format("delta") \
    .option("versionAsOf", 0) \
    .load(SILVER_PATH)

# Partitioning + Z-ordering — reduce query cost by ~70%
df.write.format("delta") \
    .partitionBy("transaction_date") \
    .save(SILVER_PATH)
```

---

## Why Multi-Cloud?

```
Azure Databricks          Google BigQuery
──────────────────────    ─────────────────────────────
Best Spark managed        Best serverless SQL engine
Delta Lake native         Looker Studio native
MLflow integrated         Free tier 1TB/month
$200 Azure free credit    Free tier 10GB storage

Common in European banks: Microsoft stack for compute,
Google for analytics — both coexist in most large enterprises.
```

---

## Author

**Achraf Jemali** — Data & AI Engineer


[![GitHub](https://img.shields.io/badge/GitHub-JEMALIACHRAF-black?logo=github)](https://github.com/JEMALIACHRAF)
[![LinkedIn](https://img.shields.io/badge/LinkedIn-Achraf_Jemali-0077B5?logo=linkedin)](https://linkedin.com/in/achraf-jemali-54a417239)

---

*MIT License — contributions welcome*

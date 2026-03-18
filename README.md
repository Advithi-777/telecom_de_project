# 📡 Telecom Customer Analytics Pipeline

![CI/CD](https://github.com/Advithi-777/telecom_de_project/actions/workflows/ci.yml/badge.svg)
![Python](https://img.shields.io/badge/python-3.12-blue)
![dbt](https://img.shields.io/badge/dbt-1.11-orange)
![Snowflake](https://img.shields.io/badge/Snowflake-blue)
![Azure](https://img.shields.io/badge/Azure-Data%20Factory-0078D4)
![Airflow](https://img.shields.io/badge/Airflow-2.8-green)

An end-to-end data engineering pipeline built on the Azure + Snowflake stack,
processing UK telecom data to deliver customer churn analytics and network
performance insights.

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                        Data Sources                              │
│  CDR (CSV)  │  Customers (CSV)  │  Billing (JSON)  │  KPIs (CSV)│
└──────────────────────────┬──────────────────────────────────────┘
                           │
                           ▼
┌─────────────────────────────────────────────────────────────────┐
│               Azure Data Factory (Ingest)                        │
│         Parameterised pipelines → Azure Blob (raw zone)          │
└──────────────────────────┬──────────────────────────────────────┘
                           │
                           ▼
┌─────────────────────────────────────────────────────────────────┐
│               PySpark (Transform)                                │
│    Clean · Deduplicate · Feature Engineering → Blob (silver)     │
└──────────────────────────┬──────────────────────────────────────┘
                           │
                           ▼
┌─────────────────────────────────────────────────────────────────┐
│               Snowflake + dbt (Medallion Architecture)           │
│   Bronze (raw) → Silver (staged views) → Gold (mart tables)      │
└──────────────────────────┬──────────────────────────────────────┘
                           │
                           ▼
┌─────────────────────────────────────────────────────────────────┐
│                     Serving Layer                                │
│   Streamlit Dashboard  │  Data Quality (22 checks)              │
└─────────────────────────────────────────────────────────────────┘
                           │
                           ▼
┌─────────────────────────────────────────────────────────────────┐
│               Orchestration + CI/CD                              │
│        Apache Airflow DAGs  │  GitHub Actions                    │
└─────────────────────────────────────────────────────────────────┘
```

---

## Tech Stack

| Layer | Technology |
|---|---|
| Ingestion | Azure Data Factory |
| Storage | Azure Blob Storage |
| Transformation | PySpark (Google Colab) |
| Data Warehouse | Snowflake |
| Data Modelling | dbt |
| Orchestration | Apache Airflow |
| Data Quality | Great Expectations |
| Dashboard | Streamlit + Plotly |
| CI/CD | GitHub Actions |
| Language | Python 3.12, SQL |

---

## Project Structure

```
telecom-de-project/
├── ingestion/
│   ├── generate_mock_data.py     # Generates 4 source datasets
│   └── upload_to_blob.py         # Uploads to Azure Blob Storage
├── spark/
│   └── telecom_transformations.ipynb  # PySpark cleaning + features
├── dbt_telecom/
│   ├── models/
│   │   ├── staging/              # Silver layer views
│   │   │   ├── stg_customers.sql
│   │   │   ├── stg_cdr.sql
│   │   │   ├── stg_billing.sql
│   │   │   └── stg_network_kpi.sql
│   │   └── marts/                # Gold layer tables
│   │       ├── customer_monthly_summary.sql
│   │       ├── churn_risk_score.sql
│   │       └── tower_performance.sql
├── airflow/
│   └── dags/
│       └── telecom_pipeline_dag.py   # Master orchestration DAG
├── quality/
│   └── telecom_suite.py          # 22 data quality checks
├── dashboard/
│   └── app.py                    # Streamlit analytics dashboard
└── .github/
    └── workflows/
        └── ci.yml                # GitHub Actions CI/CD
```

---

## Data Model

### Source Data (Mock UK Telecom)

| Dataset | Rows | Description |
|---|---|---|
| customers.csv | 5,000 | Customer master data |
| cdr.csv | 50,000 | Call detail records |
| billing.json | 55,926 | Monthly billing records |
| network_kpi.csv | 10,000 | Network tower KPIs |

### Gold Layer Tables

| Table | Rows | Description |
|---|---|---|
| customer_monthly_summary | 5,000 | Revenue + call aggregations per customer |
| churn_risk_score | 5,000 | ML-ready churn risk scoring (0-100) |
| tower_performance | 4,608 | Network health by tower + region |

---

## Pipeline Flow

### Daily Schedule (6AM UTC via Airflow)

```
1. ADF triggers (parallel)
   ├── ingest customers
   ├── ingest CDR
   ├── ingest billing
   └── ingest network KPIs
2. Wait for ADF completion
3. PySpark transformations (raw → silver)
4. dbt run (bronze → silver → gold)
5. dbt test (data quality)
6. Email alert (success/failure)
```

### CI/CD (GitHub Actions on every push)

```
1. dbt compile + dbt test
2. Data quality checks (22 checks)
3. Python code linting (flake8)
```

---

## Key Features

- **Medallion Architecture** — Bronze / Silver / Gold layers in Snowflake
- **Parameterised ADF Pipelines** — one pipeline handles all 4 sources
- **Churn Risk Scoring** — rule-based scoring model (0-100) across 5,000 customers
- **Data Quality** — 22 automated checks across all Gold tables
- **Incremental Loading** — date-partitioned data in Azure Blob Storage
- **Full Observability** — Airflow monitoring + email alerts + CI/CD badges

---

## Setup Instructions

### Prerequisites

```
Python 3.12+
Azure free account
Snowflake trial account
GitHub account
```

### 1. Clone the repo

```bash
git clone https://github.com/Advithi-777/telecom_de_project.git
cd telecom_de_project
```

### 2. Install dependencies

```bash
pip install azure-storage-blob python-dotenv pyspark \
            dbt-snowflake snowflake-connector-python \
            pandas pyarrow streamlit plotly
```

### 3. Set up environment variables

Create a `.env` file in the project root:

```
AZURE_STORAGE_CONNECTION_STRING=your_connection_string
AZURE_CONTAINER_NAME=raw
SNOWFLAKE_ACCOUNT=your_account
SNOWFLAKE_USER=your_user
SNOWFLAKE_PASSWORD=your_password
```

### 4. Generate mock data

```bash
python ingestion/generate_mock_data.py
python ingestion/upload_to_blob.py
```

### 5. Run dbt models

```bash
cd dbt_telecom
dbt run
dbt test
```

### 6. Run data quality checks

```bash
python quality/telecom_suite.py
```

### 7. Launch dashboard

```bash
streamlit run dashboard/app.py
```

---

## Dashboard

The Streamlit dashboard connects directly to Snowflake Gold layer and shows:

- KPI cards (total customers, revenue, churn rate)
- Churn risk distribution (pie chart)
- Revenue by region (bar chart)
- Tower health by region (stacked bar)
- Top 10 at-risk customers (table)
- Network performance by type (bar charts)

---

## Resume Highlights

> Built an end-to-end ELT pipeline ingesting 120,000+ telecom records daily
> using Azure Data Factory with parameterised incremental loads into Snowflake

> Designed a Medallion architecture (Bronze/Silver/Gold) using dbt on Snowflake
> with 7 data models and automated data quality checks

> Orchestrated the full pipeline using Apache Airflow DAGs with failure alerting

> Implemented CI/CD using GitHub Actions to auto-validate dbt models and data
> quality on every push

---

## Author

**Advithi** — Data Engineer  
[GitHub](https://github.com/Advithi-777)

---

*Built with Python · Azure · Snowflake · dbt · Airflow · GitHub Actions*
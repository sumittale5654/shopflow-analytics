# ShopFlow Analytics

A production-style end-to-end data engineering project built on an e-commerce retail domain.
Covers every major tool used in real data engineering roles.

```
PostgreSQL → Python → PySpark → Databricks → S3 → Snowflake
                 ↑                                       ↑
              Airflow (orchestration)         Data quality checks
```

---

## Tools covered

| Tool | Role in project |
|---|---|
| Python | Ingestion scripts, data generation, data quality |
| PostgreSQL | Source OLTP database, staging schema |
| Docker | Containerises all services — one command to run everything |
| PySpark | Data cleaning, joining, aggregation |
| Databricks | Cloud Spark execution + Delta Lake |
| Apache Airflow | Schedules and monitors the full pipeline |
| AWS S3 | Data lake — stores raw and processed Parquet files |
| Snowflake | Cloud data warehouse — final analytics tables |

---

## Project structure

```
shopflow/
├── docker/
│   ├── docker-compose.yml       # All services
│   ├── Dockerfile.etl           # Python ETL image
│   ├── requirements.txt         # Python dependencies
│   └── init_source.sql          # PostgreSQL schema
│
├── scripts/
│   └── generate_data.py         # Faker-based data seeder
│
├── ingestion/
│   ├── config.py                # Central config (reads .env)
│   └── extractor.py             # PostgreSQL → raw Parquet (incremental)
│
├── processing/
│   ├── spark_jobs.py            # PySpark clean + mart jobs (local)
│   └── databricks_notebook.py  # Same logic as Databricks cells
│
├── storage/
│   ├── s3_manager.py            # Upload/download S3 files
│   ├── snowflake_loader.py      # Load marts into Snowflake
│   └── snowflake_queries.sql    # Business reporting queries
│
├── orchestration/
│   └── dags/
│       └── shopflow_pipeline.py # Airflow DAG (daily schedule)
│
├── data_quality/
│   └── checks.py                # Row count, null, uniqueness checks
│
├── .env.example                 # Copy to .env and fill in secrets
└── Makefile                     # One-command shortcuts
```

---

## Phase 1 — Local setup (Docker)

### Prerequisites
- Docker Desktop installed and running
- Git

### Steps

```bash
# 1. Clone the project
git clone <your-repo-url>
cd shopflow

# 2. Copy and configure environment variables
cp .env.example .env
# Edit .env — the PostgreSQL values are pre-filled for local Docker.
# Fill in AWS + Snowflake values later when you reach those phases.

# 3. Start all services
make up
# or: docker compose up -d

# Wait ~30 seconds for PostgreSQL to initialise.
```

Services started:
- PostgreSQL source DB → `localhost:5433`
- Airflow UI → `http://localhost:8080` (login: admin / admin123)
- PySpark master UI → `http://localhost:8090`

---

## Phase 2 — Generate data

```bash
make generate
# This creates 2,000 customers, 300 products, 15,000 orders,
# and 120,000 clickstream events in your PostgreSQL database.
```

Verify:
```bash
make psql
# Inside psql:
SELECT COUNT(*) FROM customers;
SELECT COUNT(*) FROM orders;
\q
```

---

## Phase 3 — Run ingestion (Python)

```bash
make extract
```

This runs `ingestion/extractor.py` which:
1. Reads the watermark for each table (last extraction timestamp)
2. Extracts only rows newer than the watermark
3. Saves raw Parquet files to `./data/raw/<table>/YYYY/MM/DD/`
4. Updates the watermark

Run it a second time — it extracts 0 rows because nothing changed. This is **incremental loading**.

---

## Phase 4 — Run processing (PySpark)

```bash
make process
```

This runs `processing/spark_jobs.py` which:
1. Reads raw Parquet files
2. Cleans each table (deduplication, null filtering, type casting)
3. Builds three analytics marts:
   - `daily_sales` — revenue and profit by day and category
   - `product_performance` — top products by revenue
   - `customer_ltv` — customer lifetime value and segments

Output files land in `./data/processed/` and `./data/marts/`.

### On Databricks (Community Edition)

1. Go to [community.cloud.databricks.com](https://community.cloud.databricks.com) and sign up free
2. Create a cluster (choose the latest Databricks Runtime)
3. Upload your `./data/raw/` files to DBFS: `File → Upload Data → DBFS`
4. Create a new notebook
5. Copy and paste each cell block from `processing/databricks_notebook.py`
6. Run cells top to bottom

---

## Phase 5 — AWS S3 setup

### One-time setup
1. Create an AWS account (free tier)
2. Go to IAM → Users → Create user → Attach `AmazonS3FullAccess` policy
3. Create access key → copy into your `.env` file
4. Set `S3_BUCKET=shopflow-analytics-lake` (or any globally unique name)

### Upload to S3
```bash
docker compose exec etl-worker python -c "
from storage.s3_manager import S3Manager
from ingestion.config import MARTS_DIR
mgr = S3Manager()
mgr.upload_marts(MARTS_DIR)
"
```

---

## Phase 6 — Snowflake setup

### One-time setup
1. Sign up for a [Snowflake free trial](https://signup.snowflake.com) (30 days, no credit card for trial)
2. Find your account identifier: Admin → Accounts → hover your account → copy the account locator
3. Fill in your `.env` file:
   ```
   SNOWFLAKE_ACCOUNT=your_org-your_account
   SNOWFLAKE_USER=your_username
   SNOWFLAKE_PASSWORD=your_password
   ```

### Load data
```bash
make snowflake-load
```

This creates the database, schema, warehouse, and all three mart tables, then loads the Parquet data.

### Query in Snowflake
Open Snowflake → Worksheets → paste queries from `storage/snowflake_queries.sql`.

---

## Phase 7 — Airflow orchestration

Airflow is already running at `http://localhost:8080`.

1. Log in: admin / admin123
2. Find the `shopflow_daily_pipeline` DAG
3. Toggle it ON
4. Click the ▶ trigger button to run it manually
5. Click the run to see the task graph and logs

The DAG runs automatically at 2 AM daily.

To trigger from command line:
```bash
docker compose exec airflow-webserver airflow dags trigger shopflow_daily_pipeline
```

---

## Phase 8 — Data quality checks

```bash
make quality
```

Checks run automatically as the last step in the Airflow DAG.
Run them manually anytime to validate your marts.

Checks include:
- Minimum row counts per mart
- No nulls on key columns
- No duplicate primary keys
- No negative revenue values
- Valid LTV segment values

---

## Running the full pipeline

```bash
# Start everything and generate data (first time only)
make up
make generate

# Run the full pipeline manually
make full-run   # extract → process → quality

# Or trigger via Airflow
# http://localhost:8080 → shopflow_daily_pipeline → trigger
```

---

## Data model

### Source tables (PostgreSQL)
- `customers` — customer master data
- `products` — product catalog with cost and price
- `orders` — order headers with status and payment method
- `order_items` — line items linking orders to products
- `clickstream` — web event tracking

### Mart tables (Snowflake)
- `DAILY_SALES` — daily revenue, profit, and margin by category
- `PRODUCT_PERFORMANCE` — product rankings by revenue, units sold, unique customers
- `CUSTOMER_LTV` — customer lifetime value with VIP/High/Medium/Low segmentation

---

## What to say in interviews

> "I built an end-to-end retail analytics pipeline. Raw transactional data sits in PostgreSQL —
> orders, customers, products, and clickstream events. A Python ingestion layer uses watermark-based
> incremental extraction to pull only new rows. PySpark jobs clean and join the data and build three
> analytics marts. The processed Parquet files go to S3, and the final marts load into Snowflake where
> the business team can query them. The whole pipeline is orchestrated by Airflow running in Docker,
> with a daily schedule, automatic retries, and data quality checks as a final gate."

---

## Troubleshooting

**PostgreSQL not ready**: Wait 30 seconds after `make up` before running `make generate`.

**PySpark out of memory**: Reduce `NUM_ORDERS` in `scripts/generate_data.py` and regenerate.

**Airflow task fails**: Check logs at `http://localhost:8080` → DAG run → click the failed task → Logs.

**Snowflake connection error**: Double-check your account identifier format: `orgname-accountname` (not a URL).

**S3 upload fails**: Check IAM permissions — the user needs `s3:PutObject`, `s3:GetObject`, `s3:CreateBucket`.

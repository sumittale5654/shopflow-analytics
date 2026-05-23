"""
ShopFlow — central configuration.
All secrets are read from environment variables so nothing is hardcoded.
Copy .env.example to .env and fill in your values.
"""
import os
from dotenv import load_dotenv

load_dotenv()

# ── PostgreSQL source ─────────────────────────────────────────
PG_HOST     = os.getenv("SHOPFLOW_PG_HOST", "localhost")
PG_PORT     = os.getenv("SHOPFLOW_PG_PORT", "5433")
PG_DB       = os.getenv("SHOPFLOW_PG_DB",   "shopflow_db")
PG_USER     = os.getenv("SHOPFLOW_PG_USER", "shopflow")
PG_PASSWORD = os.getenv("SHOPFLOW_PG_PASSWORD", "shopflow123")

DB_URL = f"postgresql+psycopg2://{PG_USER}:{PG_PASSWORD}@{PG_HOST}:{PG_PORT}/{PG_DB}"

# ── AWS ───────────────────────────────────────────────────────
AWS_ACCESS_KEY_ID     = os.getenv("AWS_ACCESS_KEY_ID", "")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY", "")
AWS_REGION            = os.getenv("AWS_REGION", "ap-south-1")
S3_BUCKET             = os.getenv("S3_BUCKET", "shopflow-analytics-lake")
S3_RAW_PREFIX         = "raw"
S3_PROCESSED_PREFIX   = "processed"
S3_MARTS_PREFIX       = "marts"

# ── Snowflake ─────────────────────────────────────────────────
SNOWFLAKE_ACCOUNT   = os.getenv("SNOWFLAKE_ACCOUNT", "")
SNOWFLAKE_USER      = os.getenv("SNOWFLAKE_USER", "")
SNOWFLAKE_PASSWORD  = os.getenv("SNOWFLAKE_PASSWORD", "")
SNOWFLAKE_DATABASE  = os.getenv("SNOWFLAKE_DATABASE", "SHOPFLOW_DWH")
SNOWFLAKE_SCHEMA    = os.getenv("SNOWFLAKE_SCHEMA", "MARTS")
SNOWFLAKE_WAREHOUSE = os.getenv("SNOWFLAKE_WAREHOUSE", "SHOPFLOW_WH")
SNOWFLAKE_ROLE      = os.getenv("SNOWFLAKE_ROLE", "SYSADMIN")

# ── Local paths ───────────────────────────────────────────────
DATA_DIR      = os.getenv("DATA_DIR", "/app/data")
RAW_DIR       = f"{DATA_DIR}/raw"
PROCESSED_DIR = f"{DATA_DIR}/processed"
MARTS_DIR     = f"{DATA_DIR}/marts"

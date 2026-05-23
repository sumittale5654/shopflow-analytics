"""
ShopFlow — Snowflake Storage Layer
Sets up the Snowflake schema and loads the final data mart tables
from processed Parquet files (via S3 or direct upload).
"""

import os
import pandas as pd
from loguru import logger

import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas

from ingestion.config import (
    SNOWFLAKE_ACCOUNT, SNOWFLAKE_USER, SNOWFLAKE_PASSWORD,
    SNOWFLAKE_DATABASE, SNOWFLAKE_SCHEMA, SNOWFLAKE_WAREHOUSE, SNOWFLAKE_ROLE,
    MARTS_DIR, S3_BUCKET, S3_MARTS_PREFIX, AWS_REGION
)

# ── DDL statements ────────────────────────────────────────────────────────────
SETUP_SQL = [
    f"CREATE DATABASE IF NOT EXISTS {SNOWFLAKE_DATABASE}",
    f"USE DATABASE {SNOWFLAKE_DATABASE}",
    f"CREATE SCHEMA IF NOT EXISTS {SNOWFLAKE_SCHEMA}",
    f"USE SCHEMA {SNOWFLAKE_SCHEMA}",
    f"""
    CREATE WAREHOUSE IF NOT EXISTS {SNOWFLAKE_WAREHOUSE}
    WITH WAREHOUSE_SIZE = 'X-SMALL'
         AUTO_SUSPEND = 60
         AUTO_RESUME = TRUE
    """,
]

MART_DDL = {
    "DAILY_SALES": f"""
        CREATE TABLE IF NOT EXISTS {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.DAILY_SALES (
            SALE_DATE       DATE,
            CATEGORY        VARCHAR(100),
            NUM_ORDERS      INTEGER,
            UNITS_SOLD      INTEGER,
            GROSS_REVENUE   FLOAT,
            TOTAL_COST      FLOAT,
            GROSS_PROFIT    FLOAT,
            MARGIN_PCT      FLOAT,
            AVG_DISCOUNT_PCT FLOAT,
            _PROCESSED_AT   TIMESTAMP
        )
    """,
    "PRODUCT_PERFORMANCE": f"""
        CREATE TABLE IF NOT EXISTS {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.PRODUCT_PERFORMANCE (
            PRODUCT_ID          INTEGER,
            PRODUCT_NAME        VARCHAR(255),
            CATEGORY            VARCHAR(100),
            SUBCATEGORY         VARCHAR(100),
            BRAND               VARCHAR(100),
            NUM_ORDERS          INTEGER,
            TOTAL_UNITS_SOLD    INTEGER,
            TOTAL_REVENUE       FLOAT,
            AVG_SELLING_PRICE   FLOAT,
            AVG_DISCOUNT_PCT    FLOAT,
            UNIQUE_CUSTOMERS    INTEGER,
            REVENUE_RANK        INTEGER,
            _PROCESSED_AT       TIMESTAMP
        )
    """,
    "CUSTOMER_LTV": f"""
        CREATE TABLE IF NOT EXISTS {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.CUSTOMER_LTV (
            CUSTOMER_ID         INTEGER,
            FULL_NAME           VARCHAR(200),
            EMAIL               VARCHAR(255),
            CUSTOMER_TIER       VARCHAR(20),
            CITY                VARCHAR(100),
            COUNTRY             VARCHAR(100),
            SIGNUP_DATE         DATE,
            TOTAL_ORDERS        INTEGER,
            LIFETIME_VALUE      FLOAT,
            AVG_ORDER_VALUE     FLOAT,
            FIRST_ORDER_DATE    TIMESTAMP,
            LAST_ORDER_DATE     TIMESTAMP,
            ACTIVE_DAYS         INTEGER,
            LTV_SEGMENT         VARCHAR(20),
            _PROCESSED_AT       TIMESTAMP
        )
    """,
}


class SnowflakeLoader:
    def __init__(self):
        if not SNOWFLAKE_ACCOUNT:
            raise ValueError(
                "Snowflake credentials not configured. "
                "Set SNOWFLAKE_ACCOUNT, SNOWFLAKE_USER, SNOWFLAKE_PASSWORD in your .env file."
            )
        self.conn = snowflake.connector.connect(
            account=SNOWFLAKE_ACCOUNT,
            user=SNOWFLAKE_USER,
            password=SNOWFLAKE_PASSWORD,
            database=SNOWFLAKE_DATABASE,
            schema=SNOWFLAKE_SCHEMA,
            warehouse=SNOWFLAKE_WAREHOUSE,
            role=SNOWFLAKE_ROLE,
        )
        self.cursor = self.conn.cursor()
        logger.info("Connected to Snowflake")

    def setup_schema(self) -> None:
        logger.info("Setting up Snowflake schema...")
        for sql in SETUP_SQL:
            self.cursor.execute(sql)
        for table, ddl in MART_DDL.items():
            self.cursor.execute(ddl)
            logger.info(f"  Created table: {table}")

    def load_mart_from_parquet(self, mart_name: str, parquet_dir: str) -> None:
        """Load a local Parquet directory into a Snowflake table."""
        logger.info(f"Loading {mart_name} → Snowflake...")

        # Read all parquet files in directory
        dfs = []
        for root, _, files in os.walk(parquet_dir):
            for f in files:
                if f.endswith(".parquet"):
                    dfs.append(pd.read_parquet(os.path.join(root, f)))

        if not dfs:
            logger.warning(f"  No Parquet files found in {parquet_dir}")
            return

        df = pd.concat(dfs, ignore_index=True)
        # Snowflake column names must be uppercase
        df.columns = [c.upper() for c in df.columns]

        # Truncate then load (full refresh for marts)
        table = mart_name.upper()
        self.cursor.execute(f"TRUNCATE TABLE IF EXISTS {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.{table}")

        success, num_chunks, num_rows, output = write_pandas(
            self.conn, df, table,
            database=SNOWFLAKE_DATABASE,
            schema=SNOWFLAKE_SCHEMA,
            auto_create_table=False,
            quote_identifiers=False,
        )
        if success:
            logger.success(f"  Loaded {num_rows:,} rows into {table} ({num_chunks} chunks)")
        else:
            logger.error(f"  Failed to load {table}: {output}")

    def load_from_s3(self, mart_name: str, s3_prefix: str) -> None:
        """Load directly from S3 using Snowflake COPY INTO command."""
        if not S3_BUCKET:
            logger.warning("S3_BUCKET not configured — skipping S3 load")
            return

        table = mart_name.upper()
        s3_path = f"s3://{S3_BUCKET}/{s3_prefix}/{mart_name.lower()}/"

        # Requires an existing Snowflake stage pointing to the S3 bucket
        # Alternatively use an external stage created once with your AWS credentials
        copy_sql = f"""
            COPY INTO {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.{table}
            FROM '{s3_path}'
            FILE_FORMAT = (TYPE = 'PARQUET')
            MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
            ON_ERROR = 'CONTINUE'
        """
        self.cursor.execute(copy_sql)
        logger.success(f"  COPY INTO {table} from S3 complete")

    def run(self) -> None:
        self.setup_schema()

        mart_dirs = {
            "daily_sales":          f"{MARTS_DIR}/daily_sales",
            "product_performance":  f"{MARTS_DIR}/product_performance",
            "customer_ltv":         f"{MARTS_DIR}/customer_ltv",
        }

        for mart_name, parquet_dir in mart_dirs.items():
            if os.path.exists(parquet_dir):
                self.load_mart_from_parquet(mart_name, parquet_dir)
            else:
                logger.warning(f"  {parquet_dir} not found — run spark_jobs.py first")

    def close(self):
        self.cursor.close()
        self.conn.close()
        logger.info("Snowflake connection closed")


if __name__ == "__main__":
    loader = SnowflakeLoader()
    try:
        loader.run()
    finally:
        loader.close()

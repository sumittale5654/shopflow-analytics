"""
ShopFlow — Ingestion Layer
Extracts data from the PostgreSQL source DB using incremental watermarks.
Saves raw Parquet files locally and optionally uploads to S3.
"""

import os
from datetime import datetime

import boto3
import pandas as pd
from loguru import logger
from sqlalchemy import create_engine, text

from ingestion.config import (
    DB_URL, DATA_DIR, RAW_DIR,
    AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_REGION,
    S3_BUCKET, S3_RAW_PREFIX
)

# ── Extraction queries (incremental using watermark) ──────────────────────────
EXTRACT_QUERIES = {
    "customers": """
        SELECT * FROM customers
        WHERE updated_at > :last_extracted
        ORDER BY updated_at
    """,
    "products": """
        SELECT * FROM products
        WHERE updated_at > :last_extracted
        ORDER BY updated_at
    """,
    "orders": """
        SELECT * FROM orders
        WHERE updated_at > :last_extracted
        ORDER BY updated_at
    """,
    "order_items": """
        SELECT oi.*
        FROM order_items oi
        JOIN orders o ON oi.order_id = o.order_id
        WHERE o.updated_at > :last_extracted
        ORDER BY oi.created_at
    """,
    "clickstream": """
        SELECT * FROM clickstream
        WHERE event_timestamp > :last_extracted
        ORDER BY event_timestamp
    """,
}


class ShopFlowExtractor:
    def __init__(self):
        self.engine = create_engine(DB_URL)
        self.run_timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        os.makedirs(RAW_DIR, exist_ok=True)

    def get_watermark(self, table_name: str) -> datetime:
        with self.engine.connect() as conn:
            result = conn.execute(
                text("SELECT last_extracted FROM pipeline_watermark WHERE table_name = :t"),
                {"t": table_name}
            ).fetchone()
        watermark = result[0] if result else datetime(1970, 1, 1)
        logger.info(f"  Watermark for {table_name}: {watermark}")
        return watermark

    def update_watermark(self, table_name: str, new_ts: datetime) -> None:
        with self.engine.connect() as conn:
            conn.execute(
                text("""
                    UPDATE pipeline_watermark
                    SET last_extracted = :ts
                    WHERE table_name = :t
                """),
                {"ts": new_ts, "t": table_name}
            )
            

    def extract_table(self, table_name: str) -> pd.DataFrame:
        watermark = self.get_watermark(table_name)
        query = EXTRACT_QUERIES[table_name]
        df = pd.read_sql(text(query), self.engine, params={"last_extracted": watermark})
        logger.info(f"  Extracted {len(df):,} new rows from {table_name}")
        return df

    def save_raw_parquet(self, df: pd.DataFrame, table_name: str) -> str:
        """Save raw data as Parquet partitioned by date."""
        date_str = datetime.now().strftime("%Y/%m/%d")
        out_dir = f"{RAW_DIR}/{table_name}/{date_str}"
        os.makedirs(out_dir, exist_ok=True)
        out_path = f"{out_dir}/{table_name}_{self.run_timestamp}.parquet"
        # df.to_parquet(out_path, index=False, engine="pyarrow")
        df.to_parquet(
            out_path,
            index=False,
            engine="pyarrow",
            coerce_timestamps="us",
            allow_truncated_timestamps=True
        )
        logger.info(f"  Saved raw Parquet → {out_path}")
        return out_path

    def upload_to_s3(self, local_path: str, table_name: str) -> None:
        """Upload Parquet file to S3 data lake."""
        if not AWS_ACCESS_KEY_ID:
            logger.warning("  AWS credentials not set — skipping S3 upload")
            return
        try:
            s3 = boto3.client(
                "s3",
                aws_access_key_id=AWS_ACCESS_KEY_ID,
                aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
                region_name=AWS_REGION,
            )
            date_str = datetime.now().strftime("%Y/%m/%d")
            s3_key = f"{S3_RAW_PREFIX}/{table_name}/{date_str}/{os.path.basename(local_path)}"
            s3.upload_file(local_path, S3_BUCKET, s3_key)
            logger.success(f"  Uploaded to s3://{S3_BUCKET}/{s3_key}")
        except Exception as e:
            logger.error(f"  S3 upload failed: {e}")

    def run(self, tables: list[str] | None = None, upload_s3: bool = False) -> dict:
        tables = tables or list(EXTRACT_QUERIES.keys())
        logger.info(f"=== ShopFlow Extractor — run {self.run_timestamp} ===")
        results = {}

        for table in tables:
            logger.info(f"Processing table: {table}")
            try:
                df = self.extract_table(table)
                if df.empty:
                    logger.info(f"  No new data for {table} — skipping")
                    results[table] = {"rows": 0, "status": "skipped"}
                    continue

                local_path = self.save_raw_parquet(df, table)
                if upload_s3:
                    self.upload_to_s3(local_path, table)

                # Update watermark to the max timestamp in this batch
                ts_col = {
                    "customers":   "updated_at",
                    "products":    "updated_at",
                    "orders":      "updated_at",
                    "order_items": "created_at",
                    "clickstream": "event_timestamp",
                }.get(table)
                if ts_col and ts_col in df.columns:
                    new_watermark = df[ts_col].max()
                    self.update_watermark(table, new_watermark)

                results[table] = {"rows": len(df), "status": "success", "path": local_path}
            except Exception as e:
                logger.error(f"  Failed to extract {table}: {e}")
                results[table] = {"rows": 0, "status": "error", "error": str(e)}

        logger.success("=== Extraction complete ===")
        return results


if __name__ == "__main__":
    extractor = ShopFlowExtractor()
    extractor.run(upload_s3=False)

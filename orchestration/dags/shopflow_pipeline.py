"""
ShopFlow Analytics — Main Airflow DAG
Orchestrates the full daily data pipeline:
  1. Extract (PostgreSQL → raw Parquet)
  2. Process (PySpark clean + transform → processed Parquet + marts)
  3. Upload to S3
  4. Load to Snowflake
  5. Data quality checks
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.dates import days_ago

# ── Default args ──────────────────────────────────────────────────────────────
DEFAULT_ARGS = {
    "owner":            "shopflow",
    "depends_on_past":  False,
    "start_date":       days_ago(1),
    "retries":          2,
    "retry_delay":      timedelta(minutes=5),
    "email_on_failure": False,  # Set to True and configure SMTP for real alerts
    "email_on_retry":   False,
}

# ── Python callables ──────────────────────────────────────────────────────────

def task_extract(**context) -> dict:
    """Pull new data from PostgreSQL source."""
    import sys
    sys.path.insert(0, "/opt/airflow")
    from ingestion.extractor import ShopFlowExtractor

    extractor = ShopFlowExtractor()
    results = extractor.run(upload_s3=False)  # upload handled separately

    # Push stats to XCom so downstream tasks can inspect them
    context["task_instance"].xcom_push(key="extract_results", value=results)
    return results


def task_process(**context) -> None:
    """Run PySpark cleaning and mart-building jobs."""
    import sys
    sys.path.insert(0, "/opt/airflow")
    from processing.spark_jobs import run as spark_run
    spark_run()


def task_upload_s3(**context) -> None:
    """Upload processed Parquet files to S3."""
    import sys
    sys.path.insert(0, "/opt/airflow")
    from ingestion.config import MARTS_DIR, RAW_DIR, PROCESSED_DIR

    try:
        from storage.s3_manager import S3Manager
        mgr = S3Manager()
        mgr.create_bucket_if_not_exists()
        mgr.upload_directory(RAW_DIR,       "raw")
        mgr.upload_directory(PROCESSED_DIR, "processed")
        mgr.upload_directory(MARTS_DIR,     "marts")
    except ValueError as e:
        # AWS not configured — log a warning but don't fail the DAG
        import logging
        logging.getLogger("airflow.task").warning(f"S3 upload skipped: {e}")


def task_load_snowflake(**context) -> None:
    """Load final marts from Parquet into Snowflake."""
    import sys
    sys.path.insert(0, "/opt/airflow")

    try:
        from storage.snowflake_loader import SnowflakeLoader
        loader = SnowflakeLoader()
        try:
            loader.run()
        finally:
            loader.close()
    except ValueError as e:
        import logging
        logging.getLogger("airflow.task").warning(f"Snowflake load skipped: {e}")


def task_data_quality(**context) -> None:
    """Run data quality checks and fail the DAG if critical checks fail."""
    import sys
    sys.path.insert(0, "/opt/airflow")
    from data_quality.checks import run_checks
    run_checks()


def task_notify_success(**context) -> None:
    """Log pipeline success summary."""
    from loguru import logger
    run_date = context["ds"]
    logger.success(f"ShopFlow pipeline completed successfully for {run_date}")


# ── DAG definition ────────────────────────────────────────────────────────────
with DAG(
    dag_id="shopflow_daily_pipeline",
    description="ShopFlow e-commerce analytics pipeline — daily run",
    default_args=DEFAULT_ARGS,
    schedule_interval="0 2 * * *",  # 2 AM daily
    catchup=False,
    tags=["shopflow", "data-engineering", "ecommerce"],
    doc_md="""
## ShopFlow Daily Pipeline

End-to-end e-commerce analytics pipeline.

### Steps
1. **Extract** — Pull incremental data from PostgreSQL (watermark-based)
2. **Process** — PySpark jobs: clean → join → aggregate → build marts
3. **Upload S3** — Push Parquet files to S3 data lake
4. **Load Snowflake** — Load final marts into Snowflake DWH
5. **Quality checks** — Validate row counts, nulls, referential integrity
6. **Notify** — Log success summary

### Schedule
Runs at 02:00 UTC daily.

### Retry policy
2 retries with 5-minute delay on failure.
    """,
) as dag:

    start = EmptyOperator(task_id="start")
    end   = EmptyOperator(task_id="end")

    extract = PythonOperator(
        task_id="extract_from_postgres",
        python_callable=task_extract,
        doc_md="Extracts incremental data from PostgreSQL using watermarks.",
    )

    process = PythonOperator(
        task_id="process_with_pyspark",
        python_callable=task_process,
        execution_timeout=timedelta(hours=2),
        doc_md="Runs PySpark cleaning and mart-building jobs.",
    )

    upload_s3 = PythonOperator(
        task_id="upload_to_s3",
        python_callable=task_upload_s3,
        doc_md="Uploads Parquet files to S3 data lake.",
    )

    load_sf = PythonOperator(
        task_id="load_to_snowflake",
        python_callable=task_load_snowflake,
        doc_md="Loads mart tables into Snowflake.",
    )

    quality = PythonOperator(
        task_id="data_quality_checks",
        python_callable=task_data_quality,
        doc_md="Runs data quality assertions.",
    )

    notify = PythonOperator(
        task_id="notify_success",
        python_callable=task_notify_success,
        doc_md="Logs pipeline completion.",
    )

    # ── Task dependencies (DAG graph) ──────────────────────────────────────────
    start >> extract >> process >> upload_s3 >> load_sf >> quality >> notify >> end

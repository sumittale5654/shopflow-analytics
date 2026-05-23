"""
ShopFlow — PySpark Processing Layer
Reads raw Parquet files, cleans and transforms the data,
and writes processed Parquet files ready for the data warehouse.

Run locally:  python -m processing.spark_jobs
On Databricks: Upload this file and run each section as a notebook cell.
"""

import os
from datetime import datetime

from loguru import logger
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType, IntegerType, StringType, TimestampType, BooleanType
)
import glob
from ingestion.config import DATA_DIR, RAW_DIR, PROCESSED_DIR, MARTS_DIR


# ── Spark session factory ─────────────────────────────────────────────────────
def get_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("ShopFlow-Analytics")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.driver.memory", "2g")
        # Uncomment on Databricks to enable Delta Lake:
        # .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        # .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    )


# ── Helpers ───────────────────────────────────────────────────────────────────
def read_latest_parquet(spark: SparkSession, table: str) -> DataFrame:
    base_path = f"{RAW_DIR}/{table}/"

    files = glob.glob(base_path + "**/*.parquet", recursive=True)

    logger.info(f"  Found {len(files)} parquet files")

    if len(files) == 0:
        raise Exception(f"No parquet files found for table: {table}")

    return spark.read.parquet(*files)


def write_parquet(df: DataFrame, path: str, partition_cols: list[str] | None = None) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    writer = df.write.mode("overwrite")
    if partition_cols:
        writer = writer.partitionBy(*partition_cols)
    writer.parquet(path)
    logger.success(f"  Written → {path} ({df.count():,} rows)")


# ── Step 1: Clean customers ───────────────────────────────────────────────────
def clean_customers(spark: SparkSession) -> DataFrame:
    logger.info("Cleaning customers...")
    df = read_latest_parquet(spark, "customers")

    cleaned = (
        df
        .dropDuplicates(["customer_id"])
        .filter(F.col("email").isNotNull())
        .filter(F.col("first_name").isNotNull())
        # Standardise tier values
        .withColumn(
            "customer_tier",
            F.when(F.col("customer_tier").isin("standard", "silver", "gold"), F.col("customer_tier"))
             .otherwise("standard")
        )
        # Derive full name
        .withColumn("full_name", F.concat_ws(" ", F.col("first_name"), F.col("last_name")))
        # Cast types
        .withColumn("customer_id", F.col("customer_id").cast(IntegerType()))
        .withColumn("signup_date", F.col("signup_date").cast("date"))
        .withColumn("created_at", F.col("created_at").cast(TimestampType()))
        # Add processing metadata
        .withColumn("_processed_at", F.current_timestamp())
    )

    out_path = f"{PROCESSED_DIR}/customers"
    write_parquet(cleaned, out_path)
    return cleaned


# ── Step 2: Clean products ────────────────────────────────────────────────────
def clean_products(spark: SparkSession) -> DataFrame:
    logger.info("Cleaning products...")
    df = read_latest_parquet(spark, "products")

    cleaned = (
        df
        .dropDuplicates(["product_id"])
        .filter(F.col("product_name").isNotNull())
        .filter(F.col("unit_price") > 0)
        .filter(F.col("cost_price") > 0)
        # Derive profit margin %
        .withColumn(
            "margin_pct",
            F.round(
                (F.col("unit_price") - F.col("cost_price")) / F.col("unit_price") * 100,
                2
            )
        )
        .withColumn("product_id", F.col("product_id").cast(IntegerType()))
        .withColumn("unit_price", F.col("unit_price").cast(DoubleType()))
        .withColumn("cost_price", F.col("cost_price").cast(DoubleType()))
        .withColumn("is_active", F.col("is_active").cast(BooleanType()))
        .withColumn("_processed_at", F.current_timestamp())
    )

    out_path = f"{PROCESSED_DIR}/products"
    write_parquet(cleaned, out_path)
    return cleaned


# ── Step 3: Clean orders ──────────────────────────────────────────────────────
def clean_orders(spark: SparkSession) -> DataFrame:
    logger.info("Cleaning orders...")
    df = read_latest_parquet(spark, "orders")

    cleaned = (
        df
        .dropDuplicates(["order_id"])
        .filter(F.col("order_id").isNotNull())
        .filter(F.col("customer_id").isNotNull())
        .filter(F.col("total_amount") >= 0)
        # Standardise status
        .withColumn(
            "status",
            F.when(
                F.col("status").isin("pending","processing","shipped","delivered","cancelled"),
                F.col("status")
            ).otherwise("unknown")
        )
        # Extract date parts for partitioning / easy filtering
        .withColumn("order_date", F.col("order_date").cast(TimestampType()))
        .withColumn("order_year",  F.year("order_date"))
        .withColumn("order_month", F.month("order_date"))
        .withColumn("order_day",   F.dayofmonth("order_date"))
        .withColumn("order_dow",   F.dayofweek("order_date"))  # 1=Sun, 7=Sat
        .withColumn("_processed_at", F.current_timestamp())
    )

    out_path = f"{PROCESSED_DIR}/orders"
    write_parquet(cleaned, out_path, partition_cols=["order_year", "order_month"])
    return cleaned


# ── Step 4: Clean order items ─────────────────────────────────────────────────
def clean_order_items(spark: SparkSession) -> DataFrame:
    logger.info("Cleaning order items...")
    df = read_latest_parquet(spark, "order_items")

    cleaned = (
        df
        .dropDuplicates(["item_id"])
        .filter(F.col("quantity") > 0)
        .filter(F.col("line_total") >= 0)
        .withColumn("item_id",    F.col("item_id").cast(IntegerType()))
        .withColumn("order_id",   F.col("order_id").cast(IntegerType()))
        .withColumn("product_id", F.col("product_id").cast(IntegerType()))
        .withColumn("quantity",   F.col("quantity").cast(IntegerType()))
        .withColumn("unit_price", F.col("unit_price").cast(DoubleType()))
        .withColumn("line_total", F.col("line_total").cast(DoubleType()))
        .withColumn("_processed_at", F.current_timestamp())
    )

    out_path = f"{PROCESSED_DIR}/order_items"
    write_parquet(cleaned, out_path)
    return cleaned


# ── Step 5: Build data mart — daily sales ─────────────────────────────────────
def build_daily_sales_mart(spark: SparkSession) -> DataFrame:
    logger.info("Building daily sales mart...")
    orders   = spark.read.parquet(f"{PROCESSED_DIR}/orders")
    items    = spark.read.parquet(f"{PROCESSED_DIR}/order_items")
    products = spark.read.parquet(f"{PROCESSED_DIR}/products")

    # Join items → orders → products
    enriched = (
        items.alias("i")
        .join(orders.alias("o"),   "order_id")
        .join(products.alias("p"), "product_id")
    )

    daily = (
        enriched
        .filter(F.col("o.status") != "cancelled")
        .groupBy(
            F.to_date("o.order_date").alias("sale_date"),
            "p.category"
        )
        .agg(
            F.countDistinct("o.order_id").alias("num_orders"),
            F.sum("i.quantity").alias("units_sold"),
            F.round(F.sum("i.line_total"), 2).alias("gross_revenue"),
            F.round(F.sum(F.col("i.quantity") * F.col("p.cost_price")), 2).alias("total_cost"),
            F.round(F.avg("o.discount_pct"), 2).alias("avg_discount_pct"),
        )
        .withColumn("gross_profit", F.round(F.col("gross_revenue") - F.col("total_cost"), 2))
        .withColumn("margin_pct",   F.round(F.col("gross_profit") / F.col("gross_revenue") * 100, 2))
        .withColumn("_processed_at", F.current_timestamp())
        .orderBy("sale_date", "category")
    )

    out_path = f"{MARTS_DIR}/daily_sales"
    write_parquet(daily, out_path)
    return daily


# ── Step 6: Build data mart — product performance ─────────────────────────────
def build_product_performance_mart(spark: SparkSession) -> DataFrame:
    logger.info("Building product performance mart...")
    orders   = spark.read.parquet(f"{PROCESSED_DIR}/orders")
    items    = spark.read.parquet(f"{PROCESSED_DIR}/order_items")
    products = spark.read.parquet(f"{PROCESSED_DIR}/products")

    perf = (
        items.alias("i")
        .join(orders.alias("o"), "order_id")
        .join(products.alias("p"), "product_id")
        .filter(F.col("o.status") != "cancelled")
        .groupBy("p.product_id", "p.product_name", "p.category", "p.subcategory", "p.brand")
        .agg(
            F.countDistinct("o.order_id").alias("num_orders"),
            F.sum("i.quantity").alias("total_units_sold"),
            F.round(F.sum("i.line_total"), 2).alias("total_revenue"),
            F.round(F.avg("i.unit_price"), 2).alias("avg_selling_price"),
            F.round(F.avg("i.discount_pct"), 2).alias("avg_discount_pct"),
            F.countDistinct("o.customer_id").alias("unique_customers"),
        )
        .withColumn("revenue_rank",
            F.rank().over(
                __import__("pyspark.sql.window", fromlist=["Window"])
                .Window.orderBy(F.desc("total_revenue"))
            )
        )
        .withColumn("_processed_at", F.current_timestamp())
    )

    out_path = f"{MARTS_DIR}/product_performance"
    write_parquet(perf, out_path)
    return perf


# ── Step 7: Build data mart — customer lifetime value ─────────────────────────
def build_customer_ltv_mart(spark: SparkSession) -> DataFrame:
    logger.info("Building customer LTV mart...")
    orders    = spark.read.parquet(f"{PROCESSED_DIR}/orders")
    customers = spark.read.parquet(f"{PROCESSED_DIR}/customers")

    ltv = (
        orders.alias("o")
        .filter(F.col("status") != "cancelled")
        .groupBy("o.customer_id")
        .agg(
            F.count("order_id").alias("total_orders"),
            F.round(F.sum("total_amount"), 2).alias("lifetime_value"),
            F.round(F.avg("total_amount"), 2).alias("avg_order_value"),
            F.min("order_date").alias("first_order_date"),
            F.max("order_date").alias("last_order_date"),
            F.countDistinct(F.to_date("order_date")).alias("active_days"),
        )
        .join(customers.alias("c"), "customer_id")
        .select(
            "customer_id", "c.full_name", "c.email", "c.customer_tier",
            "c.city", "c.country", "c.signup_date",
            "total_orders", "lifetime_value", "avg_order_value",
            "first_order_date", "last_order_date", "active_days"
        )
        # Segment customers by LTV
        .withColumn(
            "ltv_segment",
            F.when(F.col("lifetime_value") >= 5000, "VIP")
             .when(F.col("lifetime_value") >= 1000, "High")
             .when(F.col("lifetime_value") >= 200,  "Medium")
             .otherwise("Low")
        )
        .withColumn("_processed_at", F.current_timestamp())
    )

    out_path = f"{MARTS_DIR}/customer_ltv"
    write_parquet(ltv, out_path)
    return ltv


# ── Main runner ───────────────────────────────────────────────────────────────
def run():
    for path in [PROCESSED_DIR, MARTS_DIR]:
        os.makedirs(path, exist_ok=True)

    spark = get_spark()
    spark.sparkContext.setLogLevel("WARN")

    logger.info("=== ShopFlow PySpark Processing ===")

    clean_customers(spark)
    clean_products(spark)
    clean_orders(spark)
    clean_order_items(spark)

    build_daily_sales_mart(spark)
    build_product_performance_mart(spark)
    build_customer_ltv_mart(spark)

    spark.stop()
    logger.success("=== Processing complete ===")


if __name__ == "__main__":
    run()

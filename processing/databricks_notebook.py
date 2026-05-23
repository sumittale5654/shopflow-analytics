# Databricks notebook source
# ShopFlow Analytics — Databricks Notebook
# Copy each cell block below into a new cell in Databricks Community Edition.
# Run cells top to bottom.

# ─────────────────────────────────────────────────────────────
# CELL 1 — Install dependencies
# ─────────────────────────────────────────────────────────────
# %pip install loguru pyarrow

# ─────────────────────────────────────────────────────────────
# CELL 2 — Config (update S3 paths or use DBFS)
# ─────────────────────────────────────────────────────────────
"""
Configuration for the Databricks environment.
On Databricks Community Edition, use DBFS paths (/dbfs/shopflow/...).
On a real Databricks workspace, use S3/ADLS paths.
"""

# If using DBFS (Community Edition):
BASE_PATH = "/dbfs/shopflow"

# If using S3 (real workspace):
# BASE_PATH = "s3a://shopflow-analytics-lake"

RAW_PATH       = f"{BASE_PATH}/raw"
PROCESSED_PATH = f"{BASE_PATH}/processed"
MARTS_PATH     = f"{BASE_PATH}/marts"

print("Config loaded. Paths:")
print(f"  RAW:       {RAW_PATH}")
print(f"  PROCESSED: {PROCESSED_PATH}")
print(f"  MARTS:     {MARTS_PATH}")

# ─────────────────────────────────────────────────────────────
# CELL 3 — Upload raw Parquet files to DBFS (one-time setup)
# ─────────────────────────────────────────────────────────────
"""
Run this cell ONLY the first time to upload your local raw Parquet files to DBFS.
After running the pipeline locally, your files are in ./data/raw/
"""
# dbutils.fs.cp("file:/local/path/to/data/raw", "dbfs:/shopflow/raw", recurse=True)

# ─────────────────────────────────────────────────────────────
# CELL 4 — Read raw data
# ─────────────────────────────────────────────────────────────
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, DoubleType, TimestampType

customers_raw  = spark.read.parquet(f"{RAW_PATH}/customers/**/*.parquet")
products_raw   = spark.read.parquet(f"{RAW_PATH}/products/**/*.parquet")
orders_raw     = spark.read.parquet(f"{RAW_PATH}/orders/**/*.parquet")
items_raw      = spark.read.parquet(f"{RAW_PATH}/order_items/**/*.parquet")

print("Row counts (raw):")
print(f"  customers  : {customers_raw.count():,}")
print(f"  products   : {products_raw.count():,}")
print(f"  orders     : {orders_raw.count():,}")
print(f"  order_items: {items_raw.count():,}")

# ─────────────────────────────────────────────────────────────
# CELL 5 — Clean & enrich customers
# ─────────────────────────────────────────────────────────────
customers_clean = (
    customers_raw
    .dropDuplicates(["customer_id"])
    .filter(F.col("email").isNotNull())
    .withColumn("full_name", F.concat_ws(" ", "first_name", "last_name"))
    .withColumn("customer_tier",
        F.when(F.col("customer_tier").isin("standard","silver","gold"), F.col("customer_tier"))
         .otherwise("standard"))
    .withColumn("_processed_at", F.current_timestamp())
)

customers_clean.write.mode("overwrite").parquet(f"{PROCESSED_PATH}/customers")
print(f"Cleaned customers: {customers_clean.count():,}")
customers_clean.show(5)

# ─────────────────────────────────────────────────────────────
# CELL 6 — Clean products
# ─────────────────────────────────────────────────────────────
products_clean = (
    products_raw
    .dropDuplicates(["product_id"])
    .filter(F.col("unit_price") > 0)
    .withColumn("margin_pct",
        F.round((F.col("unit_price") - F.col("cost_price")) / F.col("unit_price") * 100, 2))
    .withColumn("_processed_at", F.current_timestamp())
)

products_clean.write.mode("overwrite").parquet(f"{PROCESSED_PATH}/products")
display(products_clean)

# ─────────────────────────────────────────────────────────────
# CELL 7 — Clean orders + extract date parts
# ─────────────────────────────────────────────────────────────
orders_clean = (
    orders_raw
    .dropDuplicates(["order_id"])
    .filter(F.col("total_amount") >= 0)
    .withColumn("order_date",  F.col("order_date").cast(TimestampType()))
    .withColumn("order_year",  F.year("order_date"))
    .withColumn("order_month", F.month("order_date"))
    .withColumn("order_dow",   F.dayofweek("order_date"))
    .withColumn("_processed_at", F.current_timestamp())
)

orders_clean.write.mode("overwrite").partitionBy("order_year", "order_month").parquet(
    f"{PROCESSED_PATH}/orders"
)
print(f"Cleaned orders: {orders_clean.count():,}")

# ─────────────────────────────────────────────────────────────
# CELL 8 — Build DAILY SALES mart
# ─────────────────────────────────────────────────────────────
daily_sales = (
    items_raw.alias("i")
    .join(orders_clean.alias("o"), "order_id")
    .join(products_clean.alias("p"), "product_id")
    .filter(F.col("o.status") != "cancelled")
    .groupBy(F.to_date("o.order_date").alias("sale_date"), "p.category")
    .agg(
        F.countDistinct("o.order_id").alias("num_orders"),
        F.sum("i.quantity").alias("units_sold"),
        F.round(F.sum("i.line_total"), 2).alias("gross_revenue"),
        F.round(F.sum(F.col("i.quantity") * F.col("p.cost_price")), 2).alias("total_cost"),
        F.round(F.avg("o.discount_pct"), 2).alias("avg_discount_pct"),
    )
    .withColumn("gross_profit", F.round(F.col("gross_revenue") - F.col("total_cost"), 2))
    .withColumn("margin_pct",   F.round(F.col("gross_profit") / F.col("gross_revenue") * 100, 2))
    .orderBy("sale_date")
)

daily_sales.write.mode("overwrite").parquet(f"{MARTS_PATH}/daily_sales")
display(daily_sales)

# ─────────────────────────────────────────────────────────────
# CELL 9 — Build CUSTOMER LTV mart
# ─────────────────────────────────────────────────────────────
customer_ltv = (
    orders_clean.filter(F.col("status") != "cancelled")
    .groupBy("customer_id")
    .agg(
        F.count("order_id").alias("total_orders"),
        F.round(F.sum("total_amount"), 2).alias("lifetime_value"),
        F.round(F.avg("total_amount"), 2).alias("avg_order_value"),
        F.min("order_date").alias("first_order_date"),
        F.max("order_date").alias("last_order_date"),
    )
    .join(customers_clean, "customer_id")
    .withColumn("ltv_segment",
        F.when(F.col("lifetime_value") >= 5000, "VIP")
         .when(F.col("lifetime_value") >= 1000, "High")
         .when(F.col("lifetime_value") >= 200,  "Medium")
         .otherwise("Low"))
    .withColumn("_processed_at", F.current_timestamp())
)

customer_ltv.write.mode("overwrite").parquet(f"{MARTS_PATH}/customer_ltv")
display(customer_ltv.orderBy(F.desc("lifetime_value")))

# ─────────────────────────────────────────────────────────────
# CELL 10 — Register as Delta tables (optional, Databricks only)
# ─────────────────────────────────────────────────────────────
"""
Uncomment to register marts as Delta Lake tables for SQL querying.
Requires Delta Lake runtime (available on all Databricks clusters).
"""

# spark.sql("CREATE DATABASE IF NOT EXISTS shopflow_marts")
#
# daily_sales.write.format("delta").mode("overwrite").saveAsTable("shopflow_marts.daily_sales")
# customer_ltv.write.format("delta").mode("overwrite").saveAsTable("shopflow_marts.customer_ltv")
#
# %sql
# SELECT * FROM shopflow_marts.daily_sales ORDER BY sale_date DESC LIMIT 20

# ─────────────────────────────────────────────────────────────
# CELL 11 — Quick visualisation (Databricks display)
# ─────────────────────────────────────────────────────────────
monthly_summary = (
    daily_sales
    .withColumn("month", F.date_trunc("month", F.col("sale_date")))
    .groupBy("month")
    .agg(
        F.round(F.sum("gross_revenue"), 2).alias("monthly_revenue"),
        F.sum("num_orders").alias("monthly_orders"),
    )
    .orderBy("month")
)
display(monthly_summary)

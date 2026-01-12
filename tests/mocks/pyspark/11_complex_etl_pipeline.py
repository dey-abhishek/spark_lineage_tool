#!/usr/bin/env python3
"""
Comprehensive PySpark mock - Complex ETL pipeline
Tests: Multi-source reads, transformations, multiple outputs
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, lit, concat, to_date, sum, avg, count, row_number
from pyspark.sql import Window
import sys
import os

# Configuration from env
RUN_DATE = os.getenv("RUN_DATE", "2024-01-01")
ENV = os.getenv("ENVIRONMENT", "prod")
INPUT_DB = f"{ENV}_raw"
OUTPUT_DB = f"{ENV}_analytics"

spark = SparkSession.builder \
    .appName(f"ComplexETL_{RUN_DATE}") \
    .enableHiveSupport() \
    .getOrCreate()

# Read from multiple sources
df_transactions = spark.read.parquet(f"/data/raw/transactions/{RUN_DATE}")
df_users = spark.table(f"{INPUT_DB}.users")
df_products = spark.read.format("delta").load("/data/raw/products")
df_categories = spark.read.json("/data/raw/categories/*.json")

# Complex transformations
df_enriched = df_transactions \
    .join(df_users, "user_id", "left") \
    .join(df_products, "product_id", "left") \
    .join(df_categories, df_products.category_id == df_categories.id, "left")

# Add derived columns
df_enriched = df_enriched.withColumn(
    "customer_tier",
    when(col("total_purchases") > 1000, "Gold")
    .when(col("total_purchases") > 500, "Silver")
    .otherwise("Bronze")
)

# Window functions
window_spec = Window.partitionBy("user_id").orderBy(col("transaction_date").desc())
df_enriched = df_enriched.withColumn("rank", row_number().over(window_spec))

# Aggregations
df_summary = df_enriched.groupBy("user_id", "customer_tier").agg(
    sum("amount").alias("total_amount"),
    avg("amount").alias("avg_amount"),
    count("*").alias("transaction_count")
)

# Multiple outputs with different formats
df_enriched.write.mode("overwrite") \
    .partitionBy("date") \
    .parquet(f"/data/processed/enriched_transactions/{RUN_DATE}")

df_summary.write.mode("overwrite") \
    .format("delta") \
    .save(f"/data/processed/user_summaries/{RUN_DATE}")

df_enriched.write.mode("append") \
    .saveAsTable(f"{OUTPUT_DB}.daily_transactions")

# Export to CSV for external systems
df_summary.coalesce(1).write.mode("overwrite") \
    .option("header", "true") \
    .csv(f"/data/exports/summaries/{RUN_DATE}")

spark.stop()


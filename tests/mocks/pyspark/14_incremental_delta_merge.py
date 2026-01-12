#!/usr/bin/env python3
"""
Incremental load with merge
Tests: Delta Lake merge, incremental processing, SCD Type 2
"""

from pyspark.sql import SparkSession
from delta.tables import DeltaTable
from pyspark.sql.functions import col, current_timestamp, lit

spark = SparkSession.builder \
    .appName("IncrementalLoad") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .getOrCreate()

# Read incremental data
incremental_date = "${RUN_DATE}"
df_incremental = spark.read.parquet(f"/data/raw/incremental/{incremental_date}")

# Read existing Delta table
delta_path = "/data/processed/customers"
df_existing = DeltaTable.forPath(spark, delta_path)

# Perform SCD Type 2 merge
df_existing.alias("target").merge(
    df_incremental.alias("source"),
    "target.customer_id = source.customer_id"
).whenMatchedUpdate(
    condition = "target.is_current = true AND (target.email != source.email OR target.address != source.address)",
    set = {
        "is_current": "false",
        "end_date": "source.effective_date"
    }
).whenMatchedInsert(
    condition = "target.is_current = true AND (target.email != source.email OR target.address != source.address)",
    values = {
        "customer_id": "source.customer_id",
        "email": "source.email",
        "address": "source.address",
        "start_date": "source.effective_date",
        "end_date": "null",
        "is_current": "true"
    }
).whenNotMatchedInsert(
    values = {
        "customer_id": "source.customer_id",
        "email": "source.email",
        "start_date": "source.effective_date",
        "is_current": "true"
    }
).execute()

# Write checkpoint
spark.createDataFrame([(incremental_date,)], ["processed_date"]) \
    .write.mode("append") \
    .parquet("/data/checkpoints/incremental_load")

spark.stop()


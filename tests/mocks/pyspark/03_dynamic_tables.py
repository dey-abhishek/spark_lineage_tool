#!/usr/bin/env python3
"""PySpark job with dynamic table names from config."""

from pyspark.sql import SparkSession
import os

spark = SparkSession.builder.appName("DynamicTables").getOrCreate()

# Dynamic paths from environment
input_db = os.getenv("INPUT_DATABASE", "prod")
output_db = os.getenv("OUTPUT_DATABASE", "analytics")
run_date = os.getenv("RUN_DATE", "2024-01-01")

# Dynamic table reads
df = spark.table(f"{input_db}.transactions")

# Filter by date
df_filtered = df.filter(df.date == run_date)

# Save to dynamic table
df_filtered.write.mode("overwrite").saveAsTable(f"{output_db}.daily_transactions")

spark.stop()


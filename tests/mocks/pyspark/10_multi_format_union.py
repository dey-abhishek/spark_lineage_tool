#!/usr/bin/env python3
"""PySpark with multi-format reads and union."""

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("MultiFormat").getOrCreate()

# Read from various formats
df_parquet = spark.read.parquet("/data/raw/logs/2024-01/*.parquet")
df_json = spark.read.json("/data/raw/logs/2024-02/*.json")
df_csv = spark.read.option("header", "true").csv("/data/raw/logs/2024-03/*.csv")

# Union all
df_combined = df_parquet.union(df_json).union(df_csv)

# Multiple output formats
df_combined.write.mode("overwrite").parquet("/data/processed/logs_unified")
df_combined.write.mode("overwrite").format("avro").save("/data/processed/logs_avro")
df_combined.write.mode("overwrite").json("/data/processed/logs_json")


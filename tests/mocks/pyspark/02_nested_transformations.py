#!/usr/bin/env python3
"""PySpark job with nested transformations."""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, avg

spark = SparkSession.builder.appName("NestedTransform").getOrCreate()

# Multiple reads
df_transactions = spark.read.parquet("/data/raw/transactions")
df_users = spark.read.parquet("/data/raw/users")
df_products = spark.read.csv("/data/raw/products", header=True)

# Complex joins
df_enriched = df_transactions.join(df_users, "user_id").join(df_products, "product_id")

# Aggregations
df_summary = df_enriched.groupBy("user_id", "category").agg(
    sum("amount").alias("total_amount"),
    avg("price").alias("avg_price")
)

# Multiple writes
df_enriched.write.mode("overwrite").parquet("/data/processed/enriched_transactions")
df_summary.write.mode("append").parquet("/data/processed/user_summaries")

spark.stop()


#!/usr/bin/env python3
"""PySpark with partitioning and bucketing."""

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Partitioning").getOrCreate()

df = spark.read.csv("/data/raw/sales", header=True)

# Write with partitioning
df.write.mode("overwrite") \\
    .partitionBy("year", "month") \\
    .parquet("/data/processed/sales_partitioned")

# Write with bucketing to Hive table
df.write.mode("overwrite") \\
    .bucketBy(10, "user_id") \\
    .sortBy("timestamp") \\
    .saveAsTable("analytics.sales_bucketed")


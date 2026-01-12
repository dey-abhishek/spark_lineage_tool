#!/usr/bin/env python3
"""Simple PySpark job - Read parquet, write parquet."""

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("SimpleJob").getOrCreate()

# Read from HDFS
df = spark.read.parquet("/data/raw/users")

# Simple transformation
df_filtered = df.filter(df.age > 18)

# Write to HDFS
df_filtered.write.mode("overwrite").parquet("/data/processed/adults")

spark.stop()


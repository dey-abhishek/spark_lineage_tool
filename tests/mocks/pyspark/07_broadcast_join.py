#!/usr/bin/env python3
"""PySpark with joins and broadcast."""

from pyspark.sql import SparkSession
from pyspark.sql.functions import broadcast

spark = SparkSession.builder.appName("BroadcastJoin").getOrCreate()

large_df = spark.read.orc("/data/raw/transactions_large")
small_df = spark.read.json("/data/raw/categories_small")

# Broadcast join
joined = large_df.join(broadcast(small_df), "category_id")

joined.write.mode("overwrite").orc("/data/processed/categorized_transactions")


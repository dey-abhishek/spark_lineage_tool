#!/usr/bin/env python3
"""
Streaming PySpark job with checkpointing
Tests: Structured Streaming, watermarks, stateful operations
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, window, from_json
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType

spark = SparkSession.builder \
    .appName("StreamingJob") \
    .getOrCreate()

# Define schema
schema = StructType([
    StructField("user_id", StringType()),
    StructField("event_type", StringType()),
    StructField("amount", DoubleType()),
    StructField("timestamp", TimestampType())
])

# Read stream
df_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "events") \
    .load()

# Parse JSON
df_parsed = df_stream.select(
    from_json(col("value").cast("string"), schema).alias("data")
).select("data.*")

# Windowed aggregation
df_windowed = df_parsed \
    .withWatermark("timestamp", "10 minutes") \
    .groupBy(
        window("timestamp", "5 minutes"),
        "user_id"
    ).sum("amount")

# Write stream with checkpointing
query = df_windowed.writeStream \
    .outputMode("append") \
    .format("parquet") \
    .option("path", "/data/streaming/output") \
    .option("checkpointLocation", "/data/streaming/checkpoints") \
    .start()

query.awaitTermination()


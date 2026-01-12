#!/usr/bin/env python3
"""PySpark with temp views and window functions."""

from pyspark.sql import SparkSession
from pyspark.sql import Window
from pyspark.sql.functions import row_number, lag

spark = SparkSession.builder.appName("TempViews").getOrCreate()

# Create temp views
spark.read.parquet("/data/raw/events").createOrReplaceTempView("events")
spark.read.table("prod.users").createOrReplaceTempView("users")

# Query temp views
df = spark.sql("SELECT * FROM events e JOIN users u ON e.user_id = u.id")

# Window functions
window_spec = Window.partitionBy("user_id").orderBy("timestamp")
df_windowed = df.withColumn("row_num", row_number().over(window_spec))

df_windowed.write.mode("overwrite").parquet("/data/processed/user_sessions")


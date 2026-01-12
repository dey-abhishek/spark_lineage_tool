#!/usr/bin/env python3
"""
PySpark with SFTP Data Source
Uses spark-sftp library to read data from SFTP servers
"""

from pyspark.sql import SparkSession
from datetime import datetime

# Configuration
SFTP_HOST = "sftp.vendor.com"
SFTP_PORT = "22"
SFTP_USER = "data_reader"
SFTP_KEY_PATH = "/keys/sftp_key"
REMOTE_BASE = "/exports/daily"
RUN_DATE = datetime.now().strftime("%Y-%m-%d")

# Create Spark session with SFTP support
spark = SparkSession.builder \
    .appName("SFTP Data Ingestion") \
    .config("spark.jars.packages", "com.springml:spark-sftp_2.12:1.1.3") \
    .getOrCreate()

# Read CSV from SFTP
sales_df = spark.read \
    .format("com.springml.spark.sftp") \
    .option("host", SFTP_HOST) \
    .option("port", SFTP_PORT) \
    .option("username", SFTP_USER) \
    .option("privateKeyFile", SFTP_KEY_PATH) \
    .option("fileType", "csv") \
    .option("inferSchema", "true") \
    .option("header", "true") \
    .load(f"{REMOTE_BASE}/sales/sales_{RUN_DATE}.csv")

# Read JSON from SFTP with wildcard
transactions_df = spark.read \
    .format("com.springml.spark.sftp") \
    .option("host", SFTP_HOST) \
    .option("username", SFTP_USER) \
    .option("privateKeyFile", SFTP_KEY_PATH) \
    .option("fileType", "json") \
    .load(f"{REMOTE_BASE}/transactions/transactions_*.json")

# Process data
processed_df = sales_df.join(
    transactions_df,
    sales_df.order_id == transactions_df.transaction_id,
    "left"
)

# Write to HDFS
processed_df.write \
    .mode("overwrite") \
    .parquet(f"/data/processed/sales_transactions/{RUN_DATE}")

# Write to Hive
processed_df.write \
    .mode("overwrite") \
    .saveAsTable("analytics.daily_sales_transactions")

spark.stop()


#!/usr/bin/env python3
"""
PySpark with Multiple SFTP Sources
Complex pipeline reading from multiple SFTP servers
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, current_timestamp
import os

# Environment configuration
ENV = os.getenv("ENV", "prod")
RUN_DATE = os.getenv("RUN_DATE", "2024-01-15")

# SFTP configurations for different vendors
VENDOR_CONFIGS = {
    "vendor1": {
        "host": "vendor1.sftp.com",
        "user": "data_ingest",
        "key": "/keys/vendor1_key",
        "path": f"/exports/customers/customer_master_{RUN_DATE}.csv"
    },
    "vendor2": {
        "host": "vendor2.sftp.com",
        "user": "data_ingest",
        "key": "/keys/vendor2_key",
        "path": f"/feeds/orders/orders_{RUN_DATE}.json"
    },
    "vendor3": {
        "host": "vendor3.sftp.com",
        "user": "data_sync",
        "password": os.getenv("VENDOR3_PASSWORD"),
        "path": f"/data/inventory/inventory_*.parquet"
    }
}

spark = SparkSession.builder \
    .appName(f"Multi-SFTP Ingestion - {ENV}") \
    .config("spark.jars.packages", "com.springml:spark-sftp_2.12:1.1.3") \
    .getOrCreate()

# Read customers from Vendor 1 (CSV with SSH key)
customers_df = spark.read \
    .format("com.springml.spark.sftp") \
    .option("host", VENDOR_CONFIGS["vendor1"]["host"]) \
    .option("username", VENDOR_CONFIGS["vendor1"]["user"]) \
    .option("privateKeyFile", VENDOR_CONFIGS["vendor1"]["key"]) \
    .option("fileType", "csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load(VENDOR_CONFIGS["vendor1"]["path"])

customers_df = customers_df.withColumn("source", lit("vendor1")) \
    .withColumn("ingestion_time", current_timestamp())

# Read orders from Vendor 2 (JSON with SSH key)
orders_df = spark.read \
    .format("com.springml.spark.sftp") \
    .option("host", VENDOR_CONFIGS["vendor2"]["host"]) \
    .option("username", VENDOR_CONFIGS["vendor2"]["user"]) \
    .option("privateKeyFile", VENDOR_CONFIGS["vendor2"]["key"]) \
    .option("fileType", "json") \
    .load(VENDOR_CONFIGS["vendor2"]["path"])

orders_df = orders_df.withColumn("source", lit("vendor2")) \
    .withColumn("ingestion_time", current_timestamp())

# Read inventory from Vendor 3 (Parquet with password)
inventory_df = spark.read \
    .format("com.springml.spark.sftp") \
    .option("host", VENDOR_CONFIGS["vendor3"]["host"]) \
    .option("username", VENDOR_CONFIGS["vendor3"]["user"]) \
    .option("password", VENDOR_CONFIGS["vendor3"]["password"]) \
    .option("fileType", "parquet") \
    .load(VENDOR_CONFIGS["vendor3"]["path"])

inventory_df = inventory_df.withColumn("source", lit("vendor3")) \
    .withColumn("ingestion_time", current_timestamp())

# Write to HDFS landing zone
customers_df.write \
    .mode("overwrite") \
    .partitionBy("source") \
    .parquet(f"/data/landing/{ENV}/customers/{RUN_DATE}")

orders_df.write \
    .mode("overwrite") \
    .partitionBy("source") \
    .parquet(f"/data/landing/{ENV}/orders/{RUN_DATE}")

inventory_df.write \
    .mode("overwrite") \
    .partitionBy("source") \
    .parquet(f"/data/landing/{ENV}/inventory/{RUN_DATE}")

# Write to Hive staging tables
customers_df.write \
    .mode("overwrite") \
    .saveAsTable(f"staging_{ENV}.external_customers")

orders_df.write \
    .mode("overwrite") \
    .saveAsTable(f"staging_{ENV}.external_orders")

inventory_df.write \
    .mode("overwrite") \
    .saveAsTable(f"staging_{ENV}.external_inventory")

spark.stop()


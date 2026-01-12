#!/usr/bin/env python3
"""
PySpark with springml spark-sftp library - Write Operations
Based on: https://github.com/springml/spark-sftp
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp

spark = SparkSession.builder \
    .appName("SpringML SFTP Write Example") \
    .config("spark.jars.packages", "com.springml:spark-sftp_2.11:1.1.5") \
    .getOrCreate()

# Read data from Hive
source_df = spark.table("analytics.customer_summary")

# Add timestamp
output_df = source_df.withColumn("export_time", current_timestamp())

# Write CSV to SFTP with password
output_df.write \
    .format("com.springml.spark.sftp") \
    .option("host", "partner.sftp.com") \
    .option("username", "data_export") \
    .option("password", "export_pass") \
    .option("fileType", "csv") \
    .option("header", "true") \
    .option("delimiter", ",") \
    .mode("overwrite") \
    .save("/outgoing/customer_export.csv")

# Write JSON to SFTP with PEM
summary_df = output_df.groupBy("region").count()

summary_df.write \
    .format("com.springml.spark.sftp") \
    .option("host", "backup.company.com") \
    .option("username", "backup_user") \
    .option("pem", "/keys/backup_key.pem") \
    .option("fileType", "json") \
    .mode("overwrite") \
    .save("/backups/summary.json")

# Write Parquet to SFTP
output_df.select("customer_id", "total_amount", "order_count").write \
    .format("com.springml.spark.sftp") \
    .option("host", "archive.example.com") \
    .option("username", "archiver") \
    .option("password", "archive123") \
    .option("fileType", "parquet") \
    .mode("overwrite") \
    .save("/archive/customers.parquet")

spark.stop()


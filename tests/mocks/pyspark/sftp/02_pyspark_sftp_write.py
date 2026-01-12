#!/usr/bin/env python3
"""
PySpark with SFTP Write Operations
Demonstrates writing DataFrames to SFTP servers
"""

from pyspark.sql import SparkSession
from datetime import datetime

# Configuration
SFTP_HOST = "partner.sftp.com"
SFTP_PORT = "22"
SFTP_USER = "data_sender"
SFTP_PASSWORD_ENV = "SFTP_PASSWORD"
REMOTE_OUTBOX = "/incoming/reports"
RUN_DATE = datetime.now().strftime("%Y-%m-%d")

# Create Spark session
spark = SparkSession.builder \
    .appName("SFTP Data Export") \
    .config("spark.jars.packages", "com.springml:spark-sftp_2.12:1.1.3") \
    .getOrCreate()

# Read processed data from Hive
daily_report_df = spark.table("analytics.daily_summary")

# Filter for current date
from pyspark.sql.functions import col, lit
report_df = daily_report_df.filter(col("report_date") == lit(RUN_DATE))

# Write CSV to SFTP
report_df.write \
    .format("com.springml.spark.sftp") \
    .option("host", SFTP_HOST) \
    .option("port", SFTP_PORT) \
    .option("username", SFTP_USER) \
    .option("password", SFTP_PASSWORD_ENV) \
    .option("fileType", "csv") \
    .option("header", "true") \
    .mode("overwrite") \
    .save(f"{REMOTE_OUTBOX}/daily_report_{RUN_DATE}.csv")

# Write JSON summary to SFTP
summary_df = report_df.groupBy("category").agg({"amount": "sum", "count": "count"})

summary_df.write \
    .format("com.springml.spark.sftp") \
    .option("host", SFTP_HOST) \
    .option("username", SFTP_USER) \
    .option("password", SFTP_PASSWORD_ENV) \
    .option("fileType", "json") \
    .mode("overwrite") \
    .save(f"{REMOTE_OUTBOX}/summary_{RUN_DATE}.json")

spark.stop()


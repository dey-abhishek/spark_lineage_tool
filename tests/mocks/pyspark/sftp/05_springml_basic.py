#!/usr/bin/env python3
"""
PySpark with springml spark-sftp library - Basic Usage
Based on: https://github.com/springml/spark-sftp
"""

from pyspark.sql import SparkSession

# Create Spark session with spark-sftp package
spark = SparkSession.builder \
    .appName("SpringML SFTP Basic Example") \
    .config("spark.jars.packages", "com.springml:spark-sftp_2.11:1.1.5") \
    .getOrCreate()

# Example 1: Read CSV from SFTP with password authentication
df_csv = spark.read \
    .format("com.springml.spark.sftp") \
    .option("host", "sftp.example.com") \
    .option("username", "sftp_user") \
    .option("password", "sftp_password") \
    .option("fileType", "csv") \
    .option("inferSchema", "true") \
    .option("delimiter", ",") \
    .load("/data/input/sample.csv")

df_csv.show()

# Example 2: Read CSV with PEM file authentication
df_pem = spark.read \
    .format("com.springml.spark.sftp") \
    .option("host", "sftp.example.com") \
    .option("username", "sftp_user") \
    .option("pem", "/path/to/private_key.pem") \
    .option("fileType", "csv") \
    .option("header", "true") \
    .load("/data/input/customers.csv")

# Example 3: Read JSON from SFTP
df_json = spark.read \
    .format("com.springml.spark.sftp") \
    .option("host", "sftp.example.com") \
    .option("username", "data_user") \
    .option("password", "pass123") \
    .option("fileType", "json") \
    .load("/data/input/transactions.json")

# Example 4: Read Parquet from SFTP
df_parquet = spark.read \
    .format("com.springml.spark.sftp") \
    .option("host", "data.vendor.com") \
    .option("username", "spark_reader") \
    .option("pem", "/keys/id_rsa") \
    .option("fileType", "parquet") \
    .load("/exports/daily/sales.parquet")

# Write processed data to HDFS
df_csv.write.mode("overwrite").parquet("/data/processed/customers")

# Write to Hive
df_json.write.mode("overwrite").saveAsTable("raw.transactions")

spark.stop()


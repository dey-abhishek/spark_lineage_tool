#!/usr/bin/env python3
"""
PySpark with springml spark-sftp library - Complex Pipeline
Multiple SFTP sources with different authentication methods
Based on: https://github.com/springml/spark-sftp
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, current_date
from datetime import datetime

# Environment config
RUN_DATE = datetime.now().strftime("%Y-%m-%d")
ENV = "production"

spark = SparkSession.builder \
    .appName(f"SFTP Multi-Source Pipeline - {ENV}") \
    .config("spark.jars.packages", "com.springml:spark-sftp_2.11:1.1.5") \
    .getOrCreate()

# Source 1: Customer master data (CSV with PEM authentication)
customers_df = spark.read \
    .format("com.springml.spark.sftp") \
    .option("host", "vendor1.data.com") \
    .option("username", "customer_sync") \
    .option("pem", "/keys/vendor1.pem") \
    .option("pemPassphrase", "key_password") \
    .option("fileType", "csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .option("delimiter", "|") \
    .load(f"/exports/customers/customer_master_{RUN_DATE}.csv")

# Source 2: Transaction data (JSON with password)
transactions_df = spark.read \
    .format("com.springml.spark.sftp") \
    .option("host", "vendor2.data.com") \
    .option("username", "transaction_reader") \
    .option("password", "trans_pass_2024") \
    .option("fileType", "json") \
    .load(f"/feeds/transactions/trans_{RUN_DATE}.json")

# Source 3: Product catalog (Parquet with PEM)
products_df = spark.read \
    .format("com.springml.spark.sftp") \
    .option("host", "vendor3.data.com") \
    .option("username", "catalog_sync") \
    .option("pem", "/keys/vendor3_rsa") \
    .option("fileType", "parquet") \
    .load("/catalog/products/latest/*.parquet")

# Source 4: Inventory (Avro with password)
inventory_df = spark.read \
    .format("com.springml.spark.sftp") \
    .option("host", "inventory.vendor.com") \
    .option("username", "inv_reader") \
    .option("password", "inv_secret") \
    .option("fileType", "avro") \
    .load(f"/inventory/stock_{RUN_DATE}.avro")

# Add metadata
customers_df = customers_df.withColumn("source", lit("vendor1")) \
    .withColumn("load_date", current_date())

transactions_df = transactions_df.withColumn("source", lit("vendor2")) \
    .withColumn("load_date", current_date())

products_df = products_df.withColumn("source", lit("vendor3")) \
    .withColumn("load_date", current_date())

inventory_df = inventory_df.withColumn("source", lit("inventory_vendor")) \
    .withColumn("load_date", current_date())

# Write to HDFS landing zones
customers_df.write \
    .mode("overwrite") \
    .partitionBy("load_date") \
    .parquet(f"/data/landing/{ENV}/customers")

transactions_df.write \
    .mode("overwrite") \
    .partitionBy("load_date") \
    .parquet(f"/data/landing/{ENV}/transactions")

products_df.write \
    .mode("overwrite") \
    .partitionBy("load_date") \
    .parquet(f"/data/landing/{ENV}/products")

inventory_df.write \
    .mode("overwrite") \
    .partitionBy("load_date") \
    .parquet(f"/data/landing/{ENV}/inventory")

# Write to Hive staging tables
customers_df.write.mode("overwrite").saveAsTable(f"staging_{ENV}.sftp_customers")
transactions_df.write.mode("overwrite").saveAsTable(f"staging_{ENV}.sftp_transactions")
products_df.write.mode("overwrite").saveAsTable(f"staging_{ENV}.sftp_products")
inventory_df.write.mode("overwrite").saveAsTable(f"staging_{ENV}.sftp_inventory")

# Join and aggregate
enriched_df = transactions_df \
    .join(customers_df, "customer_id", "left") \
    .join(products_df, "product_id", "left") \
    .join(inventory_df, "product_id", "left")

# Write enriched data back to SFTP for partner
enriched_df.select(
    "transaction_id",
    "customer_id", 
    "product_id",
    "quantity",
    "amount"
).write \
    .format("com.springml.spark.sftp") \
    .option("host", "partner.exchange.com") \
    .option("username", "data_exchange") \
    .option("pem", "/keys/partner_exchange.pem") \
    .option("fileType", "csv") \
    .option("header", "true") \
    .option("delimiter", ",") \
    .mode("overwrite") \
    .save(f"/incoming/enriched_transactions_{RUN_DATE}.csv")

# Write summary to another SFTP location
summary_df = enriched_df.groupBy("customer_id", "product_id") \
    .agg({"amount": "sum", "quantity": "sum"})

summary_df.write \
    .format("com.springml.spark.sftp") \
    .option("host", "reporting.example.com") \
    .option("username", "report_writer") \
    .option("password", "report_pass") \
    .option("fileType", "json") \
    .mode("overwrite") \
    .save(f"/reports/daily_summary_{RUN_DATE}.json")

spark.stop()


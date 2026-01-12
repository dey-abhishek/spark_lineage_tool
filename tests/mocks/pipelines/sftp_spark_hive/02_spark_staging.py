#!/usr/bin/env python3
"""
Pipeline Stage 2: Spark Staging Layer
Reads raw data from landing zone and writes to Hive staging tables
"""

import sys
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, lit

def main(run_date, env):
    """Process landing zone data into staging tables."""
    
    spark = SparkSession.builder \
        .appName(f"Staging Pipeline - {env} - {run_date}") \
        .enableHiveSupport() \
        .getOrCreate()
    
    landing_zone = f"/data/landing/{env}/{run_date}"
    
    print(f"Processing landing zone: {landing_zone}")
    
    # Process Vendor 1: Customers
    print("Processing customer data...")
    customer_master_df = spark.read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .csv(f"{landing_zone}/vendor1/customer_master.csv")
    
    customer_trans_df = spark.read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .csv(f"{landing_zone}/vendor1/customer_transactions.csv")
    
    # Add metadata columns
    customer_master_df = customer_master_df \
        .withColumn("load_date", lit(run_date)) \
        .withColumn("load_timestamp", current_timestamp()) \
        .withColumn("source", lit("vendor1"))
    
    customer_trans_df = customer_trans_df \
        .withColumn("load_date", lit(run_date)) \
        .withColumn("load_timestamp", current_timestamp()) \
        .withColumn("source", lit("vendor1"))
    
    # Write to staging tables
    customer_master_df.write \
        .mode("overwrite") \
        .partitionBy("load_date") \
        .saveAsTable(f"staging_{env}.customer_master")
    
    customer_trans_df.write \
        .mode("overwrite") \
        .partitionBy("load_date") \
        .saveAsTable(f"staging_{env}.customer_transactions")
    
    # Process Vendor 2: Products
    print("Processing product data...")
    products_df = spark.read \
        .json(f"{landing_zone}/vendor2/product_catalog_*.json")
    
    products_df = products_df \
        .withColumn("load_date", lit(run_date)) \
        .withColumn("load_timestamp", current_timestamp()) \
        .withColumn("source", lit("vendor2"))
    
    products_df.write \
        .mode("overwrite") \
        .partitionBy("load_date") \
        .saveAsTable(f"staging_{env}.products")
    
    # Process Vendor 3: Inventory
    print("Processing inventory data...")
    inventory_df = spark.read \
        .parquet(f"{landing_zone}/vendor3/inventory.parquet")
    
    inventory_df = inventory_df \
        .withColumn("load_date", lit(run_date)) \
        .withColumn("load_timestamp", current_timestamp()) \
        .withColumn("source", lit("vendor3"))
    
    inventory_df.write \
        .mode("overwrite") \
        .partitionBy("load_date") \
        .saveAsTable(f"staging_{env}.inventory")
    
    print("Staging layer processing completed!")
    
    # Write summary stats
    stats_df = spark.createDataFrame([
        ("customer_master", customer_master_df.count()),
        ("customer_transactions", customer_trans_df.count()),
        ("products", products_df.count()),
        ("inventory", inventory_df.count())
    ], ["table_name", "row_count"])
    
    stats_df.write \
        .mode("overwrite") \
        .saveAsTable(f"staging_{env}.load_stats")
    
    spark.stop()

if __name__ == "__main__":
    run_date = sys.argv[1] if len(sys.argv) > 1 else datetime.now().strftime("%Y-%m-%d")
    env = sys.argv[2] if len(sys.argv) > 2 else "prod"
    main(run_date, env)


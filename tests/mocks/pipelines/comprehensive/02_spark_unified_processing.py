#!/usr/bin/env python3
"""
Comprehensive Pipeline Stage 2: Unified Spark Processing
Processes all sources: SFTP, S3, Kafka, RDBMS via NiFi
"""

import sys
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, current_timestamp, lit, 
    to_date, sum as _sum, avg, count, row_number
)
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, TimestampType
from pyspark.sql.window import Window

def main(run_date, env):
    """Unified processing of all data sources."""
    
    spark = SparkSession.builder \
        .appName(f"Comprehensive Pipeline - {env} - {run_date}") \
        .enableHiveSupport() \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic") \
        .config("spark.dynamicAllocation.enabled", "true") \
        .getOrCreate()
    
    raw_zone = f"/data/raw"
    
    print(f"═══════════════════════════════════════════════════════════════")
    print(f"  Processing Comprehensive Pipeline for {run_date}")
    print(f"═══════════════════════════════════════════════════════════════")
    
    # Process 1: SFTP Transactions (Vendor 1)
    print("\n[1/6] Processing SFTP transactions...")
    transactions_df = spark.read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .csv(f"{raw_zone}/transactions/{env}/{run_date}/*.csv")
    
    transactions_df = transactions_df \
        .withColumn("load_date", lit(run_date)) \
        .withColumn("load_timestamp", current_timestamp()) \
        .withColumn("source", lit("sftp_vendor1"))
    
    transactions_df.write \
        .mode("overwrite") \
        .partitionBy("load_date") \
        .saveAsTable(f"unified_{env}.transactions")
    
    # Process 2: SFTP Customers (Vendor 2)
    print("[2/6] Processing SFTP customers...")
    customers_schema = StructType([
        StructField("customer_id", StringType(), True),
        StructField("name", StringType(), True),
        StructField("email", StringType(), True),
        StructField("segment", StringType(), True),
        StructField("registration_date", StringType(), True)
    ])
    
    customers_df = spark.read \
        .schema(customers_schema) \
        .json(f"{raw_zone}/customers/{env}/{run_date}/*.json")
    
    customers_df = customers_df \
        .withColumn("load_date", lit(run_date)) \
        .withColumn("load_timestamp", current_timestamp()) \
        .withColumn("source", lit("sftp_vendor2"))
    
    customers_df.write \
        .mode("overwrite") \
        .partitionBy("load_date") \
        .saveAsTable(f"unified_{env}.customers")
    
    # Process 3: S3 Partner Data
    print("[3/6] Processing S3 partner data...")
    partners_df = spark.read \
        .parquet(f"{raw_zone}/partners/{env}/{run_date}/*.parquet")
    
    partners_df = partners_df \
        .withColumn("load_date", lit(run_date)) \
        .withColumn("load_timestamp", current_timestamp()) \
        .withColumn("source", lit("s3_partners"))
    
    partners_df.write \
        .mode("overwrite") \
        .partitionBy("load_date") \
        .saveAsTable(f"unified_{env}.partner_data")
    
    # Process 4: Kafka Realtime Streams
    print("[4/6] Processing Kafka realtime streams...")
    realtime_schema = StructType([
        StructField("event_id", StringType(), True),
        StructField("customer_id", StringType(), True),
        StructField("event_type", StringType(), True),
        StructField("event_timestamp", TimestampType(), True),
        StructField("event_data", StringType(), True)
    ])
    
    realtime_df = spark.read \
        .schema(realtime_schema) \
        .json(f"{raw_zone}/realtime/{env}/{run_date}/*.json")
    
    realtime_df = realtime_df \
        .withColumn("load_date", lit(run_date)) \
        .withColumn("load_timestamp", current_timestamp()) \
        .withColumn("source", lit("kafka_stream"))
    
    realtime_df.write \
        .mode("overwrite") \
        .partitionBy("load_date", "event_type") \
        .saveAsTable(f"unified_{env}.realtime_events")
    
    # Process 5: Create Unified View
    print("[5/6] Creating unified customer view...")
    
    # Join all sources
    unified_view = transactions_df \
        .join(customers_df, "customer_id", "left") \
        .join(
            realtime_df.groupBy("customer_id").agg(
                count("event_id").alias("event_count")
            ),
            "customer_id",
            "left"
        )
    
    # Calculate customer metrics
    window_spec = Window.partitionBy("customer_id").orderBy(col("load_timestamp").desc())
    
    customer_360 = unified_view \
        .withColumn("row_num", row_number().over(window_spec)) \
        .filter(col("row_num") == 1) \
        .select(
            "customer_id",
            col("name").alias("customer_name"),
            "email",
            "segment",
            "event_count",
            "load_date"
        )
    
    customer_360.write \
        .mode("overwrite") \
        .partitionBy("load_date") \
        .saveAsTable(f"unified_{env}.customer_360")
    
    # Process 6: Create Summary Statistics
    print("[6/6] Creating summary statistics...")
    
    summary_data = [
        ("transactions", transactions_df.count(), "sftp_vendor1"),
        ("customers", customers_df.count(), "sftp_vendor2"),
        ("partner_data", partners_df.count(), "s3_partners"),
        ("realtime_events", realtime_df.count(), "kafka_stream"),
        ("customer_360", customer_360.count(), "unified_view")
    ]
    
    summary_df = spark.createDataFrame(
        summary_data, 
        ["table_name", "row_count", "source_system"]
    )
    summary_df = summary_df.withColumn("run_date", lit(run_date))
    
    summary_df.write \
        .mode("overwrite") \
        .saveAsTable(f"unified_{env}.load_summary")
    
    # Show summary
    print("\n═══════════════════════════════════════════════════════════════")
    print("  Processing Summary:")
    print("═══════════════════════════════════════════════════════════════")
    summary_df.show(truncate=False)
    
    spark.stop()

if __name__ == "__main__":
    run_date = sys.argv[1] if len(sys.argv) > 1 else datetime.now().strftime("%Y-%m-%d")
    env = sys.argv[2] if len(sys.argv) > 2 else "prod"
    main(run_date, env)


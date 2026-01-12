#!/usr/bin/env python3
"""
Pipeline Stage 2: Spark Processing for NiFi-ingested data
Processes data from NiFi landing zone
"""

import sys
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, current_timestamp, lit
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType

def main(run_date, env):
    """Process NiFi-ingested data."""
    
    spark = SparkSession.builder \
        .appName(f"NiFi Pipeline Processing - {env} - {run_date}") \
        .enableHiveSupport() \
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic") \
        .getOrCreate()
    
    landing_zone = f"/data/landing"
    
    print(f"Processing NiFi data for date: {run_date}")
    
    # Process Sales Data from S3 (via NiFi)
    print("Processing sales data...")
    sales_schema = StructType([
        StructField("sale_id", StringType(), True),
        StructField("customer_id", StringType(), True),
        StructField("product_id", StringType(), True),
        StructField("quantity", IntegerType(), True),
        StructField("amount", DoubleType(), True),
        StructField("sale_date", StringType(), True)
    ])
    
    sales_df = spark.read \
        .schema(sales_schema) \
        .json(f"{landing_zone}/sales/{run_date}/*.json")
    
    sales_df = sales_df \
        .withColumn("load_date", lit(run_date)) \
        .withColumn("load_timestamp", current_timestamp()) \
        .withColumn("source", lit("nifi_s3"))
    
    sales_df.write \
        .mode("overwrite") \
        .partitionBy("load_date") \
        .saveAsTable(f"staging_{env}.sales_from_nifi")
    
    # Process Application Logs (via NiFi)
    print("Processing application logs...")
    logs_df = spark.read \
        .text(f"{landing_zone}/logs/{run_date}/*.log")
    
    logs_df = logs_df \
        .withColumn("load_date", lit(run_date)) \
        .withColumn("load_timestamp", current_timestamp()) \
        .withColumn("source", lit("nifi_hdfs"))
    
    logs_df.write \
        .mode("overwrite") \
        .partitionBy("load_date") \
        .saveAsTable(f"staging_{env}.application_logs")
    
    # Process Kafka Events (via NiFi)
    print("Processing Kafka events...")
    events_schema = StructType([
        StructField("event_id", StringType(), True),
        StructField("user_id", StringType(), True),
        StructField("event_type", StringType(), True),
        StructField("event_data", StringType(), True),
        StructField("timestamp", StringType(), True)
    ])
    
    events_df = spark.read \
        .schema(events_schema) \
        .json(f"{landing_zone}/events/{run_date}/*.json")
    
    events_df = events_df \
        .withColumn("load_date", lit(run_date)) \
        .withColumn("load_timestamp", current_timestamp()) \
        .withColumn("source", lit("nifi_kafka"))
    
    events_df.write \
        .mode("overwrite") \
        .partitionBy("load_date", "event_type") \
        .saveAsTable(f"staging_{env}.user_events")
    
    # Create summary statistics
    print("Creating summary statistics...")
    summary_data = [
        ("sales_from_nifi", sales_df.count()),
        ("application_logs", logs_df.count()),
        ("user_events", events_df.count())
    ]
    
    summary_df = spark.createDataFrame(summary_data, ["table_name", "row_count"])
    summary_df = summary_df.withColumn("run_date", lit(run_date))
    
    summary_df.write \
        .mode("overwrite") \
        .saveAsTable(f"staging_{env}.nifi_load_summary")
    
    print("NiFi data processing completed!")
    
    spark.stop()

if __name__ == "__main__":
    run_date = sys.argv[1] if len(sys.argv) > 1 else datetime.now().strftime("%Y-%m-%d")
    env = sys.argv[2] if len(sys.argv) > 2 else "prod"
    main(run_date, env)


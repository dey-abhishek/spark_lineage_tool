"""
Stage 2: Data Cleansing Pipeline
Reads from Bronze layer, cleanses and deduplicates, writes to Silver layer
Input: /data/bronze/customers_raw
Output: /data/silver/customers_clean
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim, upper, when, row_number
from pyspark.sql.window import Window
from datetime import datetime

# Initialize Spark
spark = SparkSession.builder \
    .appName("Stage2-CustomerCleansing") \
    .enableHiveSupport() \
    .getOrCreate()

# Configuration
bronze_path = "/data/bronze/customers_raw"
silver_path = "/data/silver/customers_clean"
run_date = datetime.now().strftime("%Y-%m-%d")

print(f"[Stage 2] Starting data cleansing pipeline...")
print(f"[Stage 2] Input: {bronze_path}")
print(f"[Stage 2] Output: {silver_path}")

# Read from Bronze layer
bronze_df = spark.read \
    .parquet(bronze_path) \
    .filter(col("ingestion_date") == run_date)

print(f"[Stage 2] Loaded {bronze_df.count()} records from Bronze")

# Data cleansing transformations
cleansed_df = bronze_df \
    .withColumn("customer_name", trim(upper(col("customer_name")))) \
    .withColumn("email", trim(col("email"))) \
    .withColumn("phone", trim(col("phone"))) \
    .withColumn("country", upper(col("country"))) \
    .filter(col("customer_id").isNotNull()) \
    .filter(col("email").isNotNull())

# Remove duplicates - keep most recent record per customer_id
window_spec = Window.partitionBy("customer_id").orderBy(col("last_modified_date").desc())
deduped_df = cleansed_df \
    .withColumn("row_num", row_number().over(window_spec)) \
    .filter(col("row_num") == 1) \
    .drop("row_num")

# Data quality flags
enriched_df = deduped_df \
    .withColumn("has_phone", when(col("phone").isNotNull(), True).otherwise(False)) \
    .withColumn("has_address", when(col("address").isNotNull(), True).otherwise(False)) \
    .withColumn("data_quality_score", 
                when(col("has_phone") & col("has_address"), "HIGH")
                .when(col("has_phone") | col("has_address"), "MEDIUM")
                .otherwise("LOW"))

# Write to Silver layer
enriched_df.write \
    .mode("overwrite") \
    .partitionBy("country", "ingestion_date") \
    .parquet(silver_path)

record_count = enriched_df.count()
print(f"[Stage 2] Successfully cleansed {record_count} records to Silver layer")
print(f"[Stage 2] Output path: {silver_path}")

# Update tracking
tracking_df = spark.createDataFrame([
    (run_date, "stage2_cleansing", silver_path, record_count, "SUCCESS")
], ["run_date", "stage", "output_path", "record_count", "status"])

tracking_df.write \
    .mode("append") \
    .saveAsTable("etl_metadata.pipeline_tracking")

spark.stop()


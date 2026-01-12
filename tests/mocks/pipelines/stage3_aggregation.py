"""
Stage 3: Data Aggregation Pipeline
Joins Silver customer data with transaction data, creates aggregations for Gold layer
Input: /data/silver/customers_clean, /data/silver/transactions
Output: hive://analytics_gold.customer_summary
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, count, avg, max, min, countDistinct
from datetime import datetime

# Initialize Spark
spark = SparkSession.builder \
    .appName("Stage3-CustomerAggregation") \
    .enableHiveSupport() \
    .getOrCreate()

# Configuration
silver_customers_path = "/data/silver/customers_clean"
silver_transactions_path = "/data/silver/transactions"
gold_table = "analytics_gold.customer_summary"
run_date = datetime.now().strftime("%Y-%m-%d")

print(f"[Stage 3] Starting data aggregation pipeline...")
print(f"[Stage 3] Input 1: {silver_customers_path}")
print(f"[Stage 3] Input 2: {silver_transactions_path}")
print(f"[Stage 3] Output: {gold_table}")

# Read from Silver layer
customers_df = spark.read \
    .parquet(silver_customers_path) \
    .filter(col("ingestion_date") == run_date)

transactions_df = spark.read \
    .parquet(silver_transactions_path) \
    .filter(col("transaction_date") >= "2024-01-01")

print(f"[Stage 3] Loaded {customers_df.count()} customers and {transactions_df.count()} transactions")

# Calculate customer transaction metrics
transaction_metrics = transactions_df \
    .groupBy("customer_id") \
    .agg(
        count("transaction_id").alias("total_transactions"),
        sum("transaction_amount").alias("total_spend"),
        avg("transaction_amount").alias("avg_transaction_value"),
        max("transaction_date").alias("last_transaction_date"),
        min("transaction_date").alias("first_transaction_date"),
        countDistinct("product_category").alias("distinct_categories")
    )

# Join with customer data
customer_summary = customers_df \
    .join(transaction_metrics, "customer_id", "left") \
    .select(
        col("customer_id"),
        col("customer_name"),
        col("email"),
        col("country"),
        col("data_quality_score"),
        col("total_transactions").cast("bigint"),
        col("total_spend").cast("decimal(18,2)"),
        col("avg_transaction_value").cast("decimal(18,2)"),
        col("last_transaction_date"),
        col("first_transaction_date"),
        col("distinct_categories").cast("int")
    )

# Add customer segmentation
from pyspark.sql.functions import when
customer_summary_with_segment = customer_summary \
    .withColumn("customer_segment",
                when(col("total_spend") > 10000, "PLATINUM")
                .when(col("total_spend") > 5000, "GOLD")
                .when(col("total_spend") > 1000, "SILVER")
                .otherwise("BRONZE"))

# Write to Gold layer (Hive table)
customer_summary_with_segment.write \
    .mode("overwrite") \
    .partitionBy("country") \
    .format("hive") \
    .saveAsTable(gold_table)

record_count = customer_summary_with_segment.count()
print(f"[Stage 3] Successfully aggregated {record_count} customer summaries to Gold layer")
print(f"[Stage 3] Output table: {gold_table}")

# Update tracking
tracking_df = spark.createDataFrame([
    (run_date, "stage3_aggregation", gold_table, record_count, "SUCCESS")
], ["run_date", "stage", "output_path", "record_count", "status"])

tracking_df.write \
    .mode("append") \
    .saveAsTable("etl_metadata.pipeline_tracking")

spark.stop()


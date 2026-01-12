"""
Stage 4: Business Reporting Views
Creates final business views and reports from Gold layer
Input: hive://analytics_gold.customer_summary
Output: hive://analytics_reports.customer_insights, hive://analytics_reports.segment_stats
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, sum, avg, lit
from datetime import datetime

# Initialize Spark
spark = SparkSession.builder \
    .appName("Stage4-BusinessReporting") \
    .enableHiveSupport() \
    .getOrCreate()

# Configuration
gold_table = "analytics_gold.customer_summary"
insights_table = "analytics_reports.customer_insights"
segment_stats_table = "analytics_reports.segment_stats"
run_date = datetime.now().strftime("%Y-%m-%d")

print(f"[Stage 4] Starting business reporting pipeline...")
print(f"[Stage 4] Input: {gold_table}")
print(f"[Stage 4] Output 1: {insights_table}")
print(f"[Stage 4] Output 2: {segment_stats_table}")

# Read from Gold layer
customer_summary_df = spark.table(gold_table)

print(f"[Stage 4] Loaded {customer_summary_df.count()} records from Gold layer")

# Create Customer Insights View
customer_insights = customer_summary_df \
    .select(
        col("customer_id"),
        col("customer_name"),
        col("email"),
        col("country"),
        col("customer_segment"),
        col("total_spend"),
        col("total_transactions"),
        col("avg_transaction_value"),
        col("data_quality_score")
    ) \
    .withColumn("report_date", lit(run_date)) \
    .withColumn("is_active", 
                col("total_transactions").isNotNull() & (col("total_transactions") > 0))

# Write Customer Insights
customer_insights.write \
    .mode("overwrite") \
    .partitionBy("country", "customer_segment") \
    .format("hive") \
    .saveAsTable(insights_table)

print(f"[Stage 4] Created {insights_table} with {customer_insights.count()} records")

# Create Segment Statistics
segment_stats = customer_summary_df \
    .groupBy("customer_segment", "country") \
    .agg(
        count("customer_id").alias("customer_count"),
        sum("total_spend").alias("segment_revenue"),
        avg("total_spend").alias("avg_customer_value"),
        avg("total_transactions").alias("avg_transactions_per_customer"),
        sum("total_transactions").alias("total_transactions")
    ) \
    .withColumn("report_date", lit(run_date))

# Write Segment Statistics
segment_stats.write \
    .mode("overwrite") \
    .format("hive") \
    .saveAsTable(segment_stats_table)

print(f"[Stage 4] Created {segment_stats_table} with {segment_stats.count()} records")

# Also create a summary report in HDFS for external consumption
summary_report_path = "/data/reports/customer_analytics_summary"
segment_stats.coalesce(1).write \
    .mode("overwrite") \
    .format("json") \
    .save(summary_report_path)

print(f"[Stage 4] Created JSON summary report at {summary_report_path}")

# Update tracking for both outputs
tracking_data = [
    (run_date, "stage4_reporting", insights_table, customer_insights.count(), "SUCCESS"),
    (run_date, "stage4_reporting", segment_stats_table, segment_stats.count(), "SUCCESS"),
    (run_date, "stage4_reporting", summary_report_path, segment_stats.count(), "SUCCESS")
]

tracking_df = spark.createDataFrame(tracking_data, 
                                   ["run_date", "stage", "output_path", "record_count", "status"])

tracking_df.write \
    .mode("append") \
    .saveAsTable("etl_metadata.pipeline_tracking")

print(f"[Stage 4] Business reporting pipeline completed successfully!")
spark.stop()


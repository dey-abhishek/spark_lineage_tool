#!/usr/bin/env python3
"""
Comprehensive Pipeline Stage 5: Final Export Layer
Exports results to multiple destinations (SFTP, S3, JDBC)
"""

import sys
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, lit

def main(run_date, env):
    """Export analytics results to multiple destinations."""
    
    spark = SparkSession.builder \
        .appName(f"Comprehensive Export - {env} - {run_date}") \
        .enableHiveSupport() \
        .config("spark.jars.packages", "com.springml:spark-sftp_2.11:1.1.5") \
        .getOrCreate()
    
    print(f"═══════════════════════════════════════════════════════════════")
    print(f"  Exporting Results - {run_date}")
    print(f"═══════════════════════════════════════════════════════════════")
    
    # Read analytics results
    print("\n[1/4] Loading analytics results...")
    customer_segments = spark.table(f"analytics_{env}.customer_segments")
    churn_prediction = spark.table(f"analytics_{env}.churn_prediction")
    customer_trends = spark.table(f"analytics_{env}.customer_trends")
    
    # Export 1: Customer Segments to SFTP (springml)
    print("[2/4] Exporting customer segments to SFTP...")
    
    customer_segments.write \
        .format("com.springml.spark.sftp") \
        .option("host", "export.partner.com") \
        .option("username", "export_user") \
        .option("pem", "/keys/export.pem") \
        .option("fileType", "csv") \
        .option("header", "true") \
        .mode("overwrite") \
        .save(f"/exports/segments/customer_segments_{run_date}.csv")
    
    # Export 2: Churn Alerts to SFTP
    print("[3/4] Exporting churn alerts to SFTP...")
    
    high_risk_customers = churn_prediction \
        .filter(col("churn_category") == "High Risk") \
        .select(
            "customer_id",
            "customer_name",
            "email",
            "churn_risk_score",
            "days_since_last_transaction",
            "total_revenue"
        )
    
    high_risk_customers.write \
        .format("com.springml.spark.sftp") \
        .option("host", "alerts.company.com") \
        .option("username", "alert_system") \
        .option("password", "alert_pass") \
        .option("fileType", "json") \
        .mode("overwrite") \
        .save(f"/alerts/churn/churn_alerts_{run_date}.json")
    
    # Export 3: Customer Trends to JDBC (for reporting DB)
    print("[4/4] Exporting customer trends to reporting database...")
    
    trends_export = customer_trends \
        .select(
            "customer_id",
            "transaction_date",
            "daily_revenue",
            "revenue_trend",
            "revenue_7day_avg"
        ) \
        .withColumn("export_timestamp", current_timestamp())
    
    trends_export.write \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://reporting-db:5432/analytics") \
        .option("dbtable", f"customer_trends_{env}") \
        .option("user", "analytics_user") \
        .option("password", "analytics_pass") \
        .option("driver", "org.postgresql.Driver") \
        .mode("overwrite") \
        .save()
    
    # Export 4: Archive to S3/HDFS
    print("Archiving complete dataset...")
    
    # Archive to HDFS
    customer_segments.write \
        .mode("overwrite") \
        .parquet(f"/data/archive/{env}/segments/{run_date}")
    
    churn_prediction.write \
        .mode("overwrite") \
        .parquet(f"/data/archive/{env}/churn/{run_date}")
    
    customer_trends.write \
        .mode("overwrite") \
        .partitionBy("transaction_date") \
        .parquet(f"/data/archive/{env}/trends/{run_date}")
    
    # Create export manifest
    print("Creating export manifest...")
    
    manifest_data = [
        ("customer_segments", customer_segments.count(), "sftp", "export.partner.com"),
        ("churn_alerts", high_risk_customers.count(), "sftp", "alerts.company.com"),
        ("customer_trends", customer_trends.count(), "jdbc", "reporting-db"),
        ("segments_archive", customer_segments.count(), "hdfs", f"/data/archive/{env}/segments/{run_date}"),
        ("churn_archive", churn_prediction.count(), "hdfs", f"/data/archive/{env}/churn/{run_date}"),
        ("trends_archive", customer_trends.count(), "hdfs", f"/data/archive/{env}/trends/{run_date}")
    ]
    
    manifest_df = spark.createDataFrame(
        manifest_data,
        ["export_name", "record_count", "destination_type", "destination_path"]
    )
    manifest_df = manifest_df \
        .withColumn("export_date", lit(run_date)) \
        .withColumn("export_timestamp", current_timestamp())
    
    manifest_df.write \
        .mode("overwrite") \
        .saveAsTable(f"export_{env}.manifest")
    
    # Display summary
    print("\n═══════════════════════════════════════════════════════════════")
    print("  Export Summary:")
    print("═══════════════════════════════════════════════════════════════")
    manifest_df.show(truncate=False)
    
    print("\nAll exports completed successfully!")
    
    spark.stop()

if __name__ == "__main__":
    run_date = sys.argv[1] if len(sys.argv) > 1 else datetime.now().strftime("%Y-%m-%d")
    env = sys.argv[2] if len(sys.argv) > 2 else "prod"
    main(run_date, env)


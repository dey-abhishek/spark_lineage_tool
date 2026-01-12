"""
PySpark job that receives inputs/outputs from shell script parameters
Demonstrates config-driven architecture where paths come from external config
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import argparse
import sys

def parse_arguments():
    """Parse command-line arguments passed from shell script"""
    parser = argparse.ArgumentParser(description='Customer Analytics ETL Job')
    
    # Run parameters
    parser.add_argument('--run-date', required=True, help='Run date (YYYY-MM-DD)')
    parser.add_argument('--env', required=True, help='Environment (dev/test/prod)')
    
    # Input paths (from config)
    parser.add_argument('--input-transactions', required=True, help='Transactions input path')
    parser.add_argument('--input-customers', required=True, help='Customers input path')
    parser.add_argument('--input-products', required=True, help='Products input path')
    parser.add_argument('--input-events', required=True, help='Events input path')
    
    # Hive table inputs
    parser.add_argument('--hive-orders-table', required=True, help='Hive orders table (db.table)')
    parser.add_argument('--hive-users-table', required=True, help='Hive users table (db.table)')
    
    # Output paths (from config)
    parser.add_argument('--output-enriched', required=True, help='Enriched output path')
    parser.add_argument('--output-summary', required=True, help='Summary output path')
    parser.add_argument('--output-fraud', required=True, help='Fraud alerts output path')
    
    # Staging
    parser.add_argument('--staging-path', required=True, help='Staging area path')
    
    return parser.parse_args()


def main():
    """Main ETL logic using parameters from config/shell"""
    args = parse_arguments()
    
    print(f"[INFO] Starting Customer Analytics ETL")
    print(f"[INFO] Run Date: {args.run_date}")
    print(f"[INFO] Environment: {args.env}")
    print(f"[INFO] Input Transactions: {args.input_transactions}")
    print(f"[INFO] Output Enriched: {args.output_enriched}")
    
    # Initialize Spark
    spark = SparkSession.builder \
        .appName(f"CustomerAnalyticsETL_{args.run_date}") \
        .config("spark.sql.adaptive.enabled", "true") \
        .enableHiveSupport() \
        .getOrCreate()
    
    # ========== READ FROM PATHS PASSED AS PARAMETERS ==========
    
    print(f"[INFO] Reading from {args.input_transactions}")
    df_transactions = spark.read.parquet(args.input_transactions)
    
    print(f"[INFO] Reading from {args.input_customers}")
    df_customers = spark.read \
        .format("avro") \
        .load(args.input_customers)
    
    print(f"[INFO] Reading from {args.input_products}")
    df_products = spark.read.orc(args.input_products)
    
    print(f"[INFO] Reading from {args.input_events}")
    df_events = spark.read \
        .option("multiLine", "true") \
        .json(args.input_events)
    
    # ========== READ FROM HIVE TABLES PASSED AS PARAMETERS ==========
    
    print(f"[INFO] Reading from Hive table: {args.hive_orders_table}")
    df_orders = spark.table(args.hive_orders_table)
    
    print(f"[INFO] Reading from Hive table: {args.hive_users_table}")
    df_users = spark.table(args.hive_users_table)
    
    # Also use SQL with parameter
    df_orders_sql = spark.sql(f"""
        SELECT * FROM {args.hive_orders_table}
        WHERE order_date = '{args.run_date}'
    """)
    
    # ========== TRANSFORMATIONS ==========
    
    print(f"[INFO] Performing transformations...")
    
    # Join data from multiple sources
    df_enriched = df_transactions \
        .join(df_customers, "customer_id", "left") \
        .join(df_products, "product_id", "left") \
        .join(df_orders, "order_id", "left") \
        .join(df_users, "user_id", "left") \
        .join(df_events, "event_id", "left")
    
    # Add metadata columns
    df_enriched = df_enriched \
        .withColumn("run_date", lit(args.run_date)) \
        .withColumn("processing_timestamp", current_timestamp()) \
        .withColumn("env", lit(args.env))
    
    # Calculate customer summary
    df_summary = df_enriched.groupBy("customer_id", "run_date").agg(
        count("transaction_id").alias("total_transactions"),
        sum("amount").alias("total_amount"),
        avg("amount").alias("avg_transaction_value"),
        max("transaction_date").alias("last_transaction_date")
    )
    
    # Fraud detection
    df_fraud = df_enriched.filter(
        (col("amount") > 10000) & 
        (col("customer_tenure_days") < 30)
    ).select(
        "transaction_id",
        "customer_id",
        "amount",
        "transaction_date",
        lit("HIGH_RISK").alias("alert_type")
    )
    
    # ========== WRITE TO STAGING (using parameter) ==========
    
    print(f"[INFO] Writing to staging: {args.staging_path}")
    
    staging_enriched = f"{args.staging_path}/enriched"
    df_enriched.write \
        .mode("overwrite") \
        .partitionBy("run_date", "region") \
        .parquet(staging_enriched)
    
    staging_summary = f"{args.staging_path}/summary"
    df_summary.write \
        .mode("overwrite") \
        .orc(staging_summary)
    
    # ========== WRITE TO FINAL OUTPUTS (using parameters) ==========
    
    print(f"[INFO] Writing enriched data to: {args.output_enriched}")
    df_enriched.write \
        .mode("overwrite") \
        .partitionBy("run_date", "region", "customer_tier") \
        .option("compression", "snappy") \
        .parquet(args.output_enriched)
    
    print(f"[INFO] Writing summary to: {args.output_summary}")
    df_summary.write \
        .mode("overwrite") \
        .partitionBy("run_date") \
        .option("compression", "zlib") \
        .orc(args.output_summary)
    
    print(f"[INFO] Writing fraud alerts to: {args.output_fraud}")
    df_fraud.write \
        .mode("append") \
        .json(args.output_fraud)
    
    # ========== WRITE TO HIVE TABLES (paths will be loaded by Hive) ==========
    
    # The shell script will use beeline to load from these paths
    # But we also demonstrate direct Hive writes
    
    print(f"[INFO] Writing to Hive analytics table...")
    df_enriched.write \
        .mode("overwrite") \
        .format("orc") \
        .saveAsTable(f"{args.env}_analytics.enriched_transactions_daily")
    
    df_summary.write \
        .mode("append") \
        .insertInto(f"{args.env}_reports.customer_360_view")
    
    # ========== LOG LINEAGE INFORMATION ==========
    
    print(f"\n[INFO] ========== ETL LINEAGE SUMMARY ==========")
    print(f"[INFO] INPUTS:")
    print(f"[INFO]   - Parquet: {args.input_transactions}")
    print(f"[INFO]   - Avro: {args.input_customers}")
    print(f"[INFO]   - ORC: {args.input_products}")
    print(f"[INFO]   - JSON: {args.input_events}")
    print(f"[INFO]   - Hive: {args.hive_orders_table}")
    print(f"[INFO]   - Hive: {args.hive_users_table}")
    print(f"[INFO] OUTPUTS:")
    print(f"[INFO]   - Parquet: {args.output_enriched}")
    print(f"[INFO]   - ORC: {args.output_summary}")
    print(f"[INFO]   - JSON: {args.output_fraud}")
    print(f"[INFO]   - Hive: {args.env}_analytics.enriched_transactions_daily")
    print(f"[INFO]   - Hive: {args.env}_reports.customer_360_view")
    print(f"[INFO] STAGING: {args.staging_path}")
    print(f"[INFO] ==========================================\n")
    
    spark.stop()
    
    print(f"[INFO] ETL completed successfully!")
    return 0


if __name__ == "__main__":
    sys.exit(main())


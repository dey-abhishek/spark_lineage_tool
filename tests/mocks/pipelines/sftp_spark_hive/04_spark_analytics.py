#!/usr/bin/env python3
"""
Pipeline Stage 4: Spark Analytics Layer
Creates analytical aggregations and exports results
"""

import sys
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as _sum, avg, count, max as _max, dense_rank
from pyspark.sql.window import Window

def main(run_date, env):
    """Create analytics and export results."""
    
    spark = SparkSession.builder \
        .appName(f"Analytics Pipeline - {env} - {run_date}") \
        .enableHiveSupport() \
        .getOrCreate()
    
    print(f"Creating analytics for date: {run_date}")
    
    # Read enriched data
    customers_df = spark.table(f"enriched_{env}.customers")
    products_df = spark.table(f"enriched_{env}.products_with_inventory")
    transactions_df = spark.table(f"enriched_{env}.customer_transaction_summary")
    
    # Analytics 1: Customer Segmentation
    print("Creating customer segmentation...")
    customer_segments = customers_df \
        .withColumn("segment", 
            col("total_spend") / col("total_transactions")) \
        .select(
            "customer_id",
            "customer_name",
            "total_spend",
            "total_transactions",
            "avg_transaction_amount",
            "segment"
        )
    
    # Define customer tiers
    window_spec = Window.orderBy(col("total_spend").desc())
    
    customer_tiers = customer_segments \
        .withColumn("tier_rank", dense_rank().over(window_spec)) \
        .withColumn("tier",
            col("tier_rank").cast("string"))  # Simplified tier logic
    
    customer_tiers.write \
        .mode("overwrite") \
        .saveAsTable(f"analytics_{env}.customer_tiers")
    
    # Analytics 2: Product Performance
    print("Creating product performance metrics...")
    product_metrics = products_df \
        .select(
            "product_id",
            "product_name",
            "category",
            "price",
            "quantity_available",
            "quantity_reserved"
        ) \
        .withColumn("stock_value", 
            col("price") * col("quantity_available"))
    
    product_metrics.write \
        .mode("overwrite") \
        .saveAsTable(f"analytics_{env}.product_metrics")
    
    # Analytics 3: Daily Transaction Trends
    print("Creating transaction trends...")
    daily_trends = transactions_df \
        .groupBy("transaction_date") \
        .agg(
            _sum("transaction_count").alias("total_transactions"),
            _sum("daily_spend").alias("total_revenue"),
            avg("avg_transaction").alias("avg_transaction_value"),
            count("customer_id").alias("active_customers")
        )
    
    daily_trends.write \
        .mode("overwrite") \
        .partitionBy("transaction_date") \
        .saveAsTable(f"analytics_{env}.daily_trends")
    
    # Analytics 4: Customer Lifetime Value
    print("Creating customer lifetime value...")
    clv = customers_df \
        .select(
            "customer_id",
            "customer_name",
            "total_spend",
            "total_transactions",
            "last_transaction_date"
        ) \
        .withColumn("clv_score", 
            col("total_spend") * col("total_transactions"))  # Simplified CLV
    
    clv.write \
        .mode("overwrite") \
        .saveAsTable(f"analytics_{env}.customer_lifetime_value")
    
    # Export top customers to HDFS for reporting
    print("Exporting top customers...")
    top_customers = clv.orderBy(col("clv_score").desc()).limit(1000)
    
    top_customers.write \
        .mode("overwrite") \
        .option("header", "true") \
        .csv(f"/data/exports/{env}/top_customers/{run_date}")
    
    # Export product summary to HDFS
    print("Exporting product summary...")
    product_summary = product_metrics \
        .groupBy("category") \
        .agg(
            count("product_id").alias("product_count"),
            _sum("stock_value").alias("total_stock_value"),
            avg("price").alias("avg_price")
        )
    
    product_summary.write \
        .mode("overwrite") \
        .option("header", "true") \
        .csv(f"/data/exports/{env}/product_summary/{run_date}")
    
    print("Analytics processing completed!")
    
    spark.stop()

if __name__ == "__main__":
    run_date = sys.argv[1] if len(sys.argv) > 1 else datetime.now().strftime("%Y-%m-%d")
    env = sys.argv[2] if len(sys.argv) > 2 else "prod"
    main(run_date, env)


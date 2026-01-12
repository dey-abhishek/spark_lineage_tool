#!/usr/bin/env python3
"""
Pipeline Stage 4: Spark ML and Advanced Analytics
Creates machine learning features and predictions
"""

import sys
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, avg, stddev, lag
from pyspark.sql.window import Window

def main(run_date, env):
    """Create ML features and predictions."""
    
    spark = SparkSession.builder \
        .appName(f"NiFi ML Pipeline - {env} - {run_date}") \
        .enableHiveSupport() \
        .config("spark.sql.adaptive.enabled", "true") \
        .getOrCreate()
    
    print(f"Creating ML features for date: {run_date}")
    
    # Read analytics data
    sales_analysis = spark.table(f"analytics_{env}.sales_analysis")
    user_behavior = spark.table(f"analytics_{env}.user_behavior")
    
    # Feature Engineering 1: Customer Value Score
    print("Creating customer value scores...")
    
    window_spec = Window.partitionBy("customer_id").orderBy("sale_date")
    
    customer_features = sales_analysis \
        .withColumn("prev_amount", lag("amount", 1).over(window_spec)) \
        .withColumn("amount_change", 
            when(col("prev_amount").isNotNull(), 
                 col("amount") - col("prev_amount"))
            .otherwise(0)) \
        .groupBy("customer_id") \
        .agg(
            avg("amount").alias("avg_purchase"),
            stddev("amount").alias("purchase_volatility"),
            avg("amount_change").alias("avg_amount_change")
        )
    
    customer_features.write \
        .mode("overwrite") \
        .saveAsTable(f"ml_{env}.customer_features")
    
    # Feature Engineering 2: User Engagement Score
    print("Creating user engagement scores...")
    
    engagement_features = user_behavior \
        .groupBy("user_id") \
        .agg(
            avg("event_count").alias("avg_events"),
            avg("purchase_count").alias("avg_purchases"),
            avg("total_spent").alias("avg_spend")
        ) \
        .withColumn("engagement_score",
            (col("avg_events") * 0.3 + 
             col("avg_purchases") * 0.4 + 
             col("avg_spend") * 0.3))
    
    engagement_features.write \
        .mode("overwrite") \
        .saveAsTable(f"ml_{env}.engagement_features")
    
    # Create predictions table (simplified for lineage purposes)
    print("Creating predictions...")
    
    predictions = customer_features.join(
        engagement_features,
        customer_features.customer_id == engagement_features.user_id,
        "inner"
    ).select(
        customer_features.customer_id,
        customer_features.avg_purchase,
        engagement_features.engagement_score
    ).withColumn("predicted_churn",
        when(col("engagement_score") < 0.5, "high_risk")
        .when(col("engagement_score") < 0.7, "medium_risk")
        .otherwise("low_risk"))
    
    predictions.write \
        .mode("overwrite") \
        .saveAsTable(f"ml_{env}.churn_predictions")
    
    # Export ML features and predictions
    print("Exporting ML results...")
    
    predictions.write \
        .mode("overwrite") \
        .option("header", "true") \
        .parquet(f"/data/ml_exports/{env}/predictions/{run_date}")
    
    engagement_features.write \
        .mode("overwrite") \
        .option("header", "true") \
        .parquet(f"/data/ml_exports/{env}/engagement/{run_date}")
    
    print("ML pipeline completed!")
    
    spark.stop()

if __name__ == "__main__":
    run_date = sys.argv[1] if len(sys.argv) > 1 else datetime.now().strftime("%Y-%m-%d")
    env = sys.argv[2] if len(sys.argv) > 2 else "prod"
    main(run_date, env)


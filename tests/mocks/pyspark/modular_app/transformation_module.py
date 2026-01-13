"""
transformation_module.py
Handles all data transformation and business logic
"""

from pyspark.sql import DataFrame, Window
from pyspark.sql.functions import (
    col, count, sum as _sum, avg, max as _max, min as _min,
    countDistinct, current_timestamp, lit, when, desc,
    from_json, to_json, struct, explode, array, concat_ws,
    datediff, current_date, row_number
)
from pyspark.sql.types import StructType, StructField, StringType, LongType
from typing import Optional


class TransformationModule:
    """Module responsible for data transformation and business logic"""
    
    def __init__(self, spark):
        self.spark = spark
    
    def enrich_transactions(self, transactions: DataFrame, customers: DataFrame, 
                           products: DataFrame) -> DataFrame:
        """Enrich transactions with customer and product details"""
        print("Enriching transactions with customer and product data")
        
        return (transactions
                .join(customers, "customer_id", "left")
                .join(products, "product_id", "left")
                .withColumn("enriched_timestamp", current_timestamp())
                .select(
                    "transaction_id", "customer_id", "name", "email",
                    "product_id", "product_name", "category", 
                    "amount", "timestamp", "region", "enriched_timestamp"
                ))
    
    def calculate_customer_metrics(self, transactions: DataFrame, run_date: str) -> DataFrame:
        """Calculate customer-level metrics and aggregations"""
        print(f"Calculating customer metrics for: {run_date}")
        
        window_spec = Window.partitionBy("customer_id").orderBy(desc("timestamp"))
        
        return (transactions
                .groupBy("customer_id", "region")
                .agg(
                    count("transaction_id").alias("transaction_count"),
                    _sum("amount").alias("total_spent"),
                    avg("amount").alias("avg_transaction_value"),
                    _max("timestamp").alias("last_transaction_date"),
                    countDistinct("category").alias("unique_categories"),
                    countDistinct("product_id").alias("unique_products")
                )
                .withColumn("run_date", lit(run_date))
                .withColumn("calculated_at", current_timestamp()))
    
    def detect_fraud(self, enriched_transactions: DataFrame) -> DataFrame:
        """Apply fraud detection rules"""
        print("Applying fraud detection rules")
        
        return (enriched_transactions
                .withColumn("is_high_value", when(col("amount") > 10000, True).otherwise(False))
                .withColumn("is_suspicious", 
                    when((col("amount") > 5000) & 
                         (col("timestamp").cast("date") == current_date()), 
                         True).otherwise(False))
                .filter((col("is_suspicious") == True) | (col("is_high_value") == True))
                .select("transaction_id", "customer_id", "amount", "timestamp", 
                       "is_suspicious", "is_high_value"))
    
    def generate_product_summary(self, transactions: DataFrame, run_date: str) -> DataFrame:
        """Generate daily product sales summary"""
        print(f"Generating product summary for: {run_date}")
        
        return (transactions
                .filter(col("status") == "completed")
                .groupBy("product_id", "product_name", "category")
                .agg(
                    count("transaction_id").alias("sales_count"),
                    _sum("amount").alias("total_revenue"),
                    avg("amount").alias("avg_price"),
                    countDistinct("customer_id").alias("unique_customers")
                )
                .withColumn("report_date", lit(run_date))
                .orderBy(desc("total_revenue")))
    
    def calculate_inventory_status(self, inventory: DataFrame, transactions: DataFrame) -> DataFrame:
        """Calculate inventory status with sales velocity"""
        print("Calculating inventory status")
        
        sales_velocity = (transactions
                         .groupBy("product_id")
                         .agg(count("transaction_id").alias("daily_sales")))
        
        return (inventory
                .join(sales_velocity, "product_id", "left")
                .withColumn("days_of_stock", 
                    when(col("daily_sales").isNull(), lit(999))
                    .otherwise(col("quantity") / col("daily_sales")))
                .withColumn("reorder_needed", 
                    when(col("days_of_stock") < 7, True).otherwise(False))
                .select("product_id", "quantity", "warehouse_id", 
                       "daily_sales", "days_of_stock", "reorder_needed"))
    
    def process_events(self, events: DataFrame) -> DataFrame:
        """Process real-time events from Kafka stream"""
        print("Processing real-time events")
        
        schema = StructType([
            StructField("event_id", StringType(), True),
            StructField("customer_id", StringType(), True),
            StructField("event_type", StringType(), True),
            StructField("timestamp", LongType(), True)
        ])
        
        return (events
                .select(from_json(col("value"), schema).alias("data"))
                .select("data.*")
                .withColumn("processed_at", current_timestamp()))
    
    def generate_customer_segments(self, customer_metrics: DataFrame) -> DataFrame:
        """Segment customers based on their behavior"""
        print("Generating customer segments")
        
        return (customer_metrics
                .withColumn("segment",
                    when(col("total_spent") > 10000, "VIP")
                    .when(col("total_spent") > 5000, "Premium")
                    .when(col("total_spent") > 1000, "Regular")
                    .otherwise("New"))
                .withColumn("risk_category",
                    when(datediff(current_date(), col("last_transaction_date")) > 90, "At Risk")
                    .when(datediff(current_date(), col("last_transaction_date")) > 30, "Moderate")
                    .otherwise("Active"))
                .select("customer_id", "region", "transaction_count", "total_spent",
                       "segment", "risk_category", "run_date"))
    
    def create_daily_snapshot(self, transactions: DataFrame, orders: DataFrame, run_date: str) -> DataFrame:
        """Create daily snapshot combining transactions and orders"""
        print(f"Creating daily snapshot for: {run_date}")
        
        return (transactions
                .join(orders, ["customer_id", "order_id"], "outer")
                .withColumn("snapshot_date", lit(run_date))
                .withColumn("snapshot_timestamp", current_timestamp())
                .select("customer_id", "order_id", "transaction_id", 
                       "amount", "total_amount", "snapshot_date", "snapshot_timestamp"))


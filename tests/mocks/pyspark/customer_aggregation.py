"""
Customer Aggregation Script
Receives schema-qualified table names as command line arguments from shell script
"""

import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import count, sum, avg

# Parse command line arguments
parser = argparse.ArgumentParser(description='Customer Aggregation')
parser.add_argument('--source-customers', required=True, help='Source customers table (schema.table)')
parser.add_argument('--source-orders', required=True, help='Source orders table (schema.table)')
parser.add_argument('--target-table', required=True, help='Target table (schema.table)')
args = parser.parse_args()

print(f"📊 Customer Aggregation")
print(f"  Source Customers: {args.source_customers}")
print(f"  Source Orders: {args.source_orders}")
print(f"  Target: {args.target_table}")

# Initialize Spark
spark = SparkSession.builder \
    .appName("CustomerAggregation") \
    .enableHiveSupport() \
    .getOrCreate()

# Read from schema-qualified tables (passed as parameters)
customers_df = spark.table(args.source_customers)
orders_df = spark.table(args.source_orders)

# Aggregate
customer_summary = customers_df.join(
    orders_df, "customer_id", "left"
).groupBy(
    "customer_id", "customer_name", "email"
).agg(
    count("order_id").alias("total_orders"),
    sum("order_amount").alias("total_spent"),
    avg("order_amount").alias("avg_order_value")
)

# Write to schema-qualified target table (passed as parameter)
customer_summary.write \
    .mode("overwrite") \
    .saveAsTable(args.target_table)

print(f"✅ Aggregation complete: {customer_summary.count()} customers written to {args.target_table}")
spark.stop()


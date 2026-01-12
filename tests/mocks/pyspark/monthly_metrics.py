"""
Monthly Metrics Script
Receives schema-qualified tables from shell script parameters
"""

import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, sum, date_format

parser = argparse.ArgumentParser(description='Monthly Metrics')
parser.add_argument('--input-summary', required=True, help='Customer summary table (schema.table)')
parser.add_argument('--input-products', required=True, help='Product performance table (schema.table)')
parser.add_argument('--output-table', required=True, help='Output table (schema.table)')
args = parser.parse_args()

print(f"📊 Monthly Metrics")
print(f"  Input Summary: {args.input_summary}")
print(f"  Input Products: {args.input_products}")
print(f"  Output: {args.output_table}")

spark = SparkSession.builder \
    .appName("MonthlyMetrics") \
    .enableHiveSupport() \
    .getOrCreate()

# Read from schema-qualified tables
summary_df = spark.table(args.input_summary)
products_df = spark.table(args.input_products)

# Calculate metrics
metrics_df = summary_df.join(
    products_df, summary_df.customer_id == products_df.product_id, "left"
).groupBy(
    date_format(col("order_date"), "yyyy-MM").alias("month")
).agg(
    count("customer_id").alias("unique_customers"),
    sum("total_spent").alias("monthly_revenue")
)

# Write to schema-qualified output table
metrics_df.write \
    .mode("overwrite") \
    .saveAsTable(args.output_table)

print(f"✅ Metrics complete: {metrics_df.count()} months written to {args.output_table}")
spark.stop()


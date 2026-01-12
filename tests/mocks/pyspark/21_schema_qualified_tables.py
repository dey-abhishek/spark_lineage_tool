"""
Test: Schema-Qualified Table Names (schema.table)
Tests the tool's ability to extract and track schema-qualified Hive table names
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, sum, avg

# Initialize Spark
spark = SparkSession.builder \
    .appName("SchemaQualifiedTables") \
    .enableHiveSupport() \
    .getOrCreate()

print("Testing schema-qualified table names...")

# Read from schema-qualified tables
customers_df = spark.table("prod_db.customers")
orders_df = spark.table("prod_db.orders")
products_df = spark.read.table("catalog_db.products")

# Join tables
customer_orders = customers_df.join(
    orders_df, 
    customers_df.customer_id == orders_df.customer_id
)

# Read using SQL with schema-qualified names
revenue_df = spark.sql("""
    SELECT 
        c.customer_id,
        c.customer_name,
        SUM(o.order_amount) as total_revenue
    FROM prod_db.customers c
    JOIN prod_db.orders o ON c.customer_id = o.customer_id
    GROUP BY c.customer_id, c.customer_name
""")

# Write to schema-qualified tables
customer_orders.write \
    .mode("overwrite") \
    .saveAsTable("analytics_db.customer_orders")

revenue_df.write \
    .mode("overwrite") \
    .format("hive") \
    .saveAsTable("analytics_db.customer_revenue")

# Insert into existing table
spark.sql("""
    INSERT OVERWRITE TABLE analytics_db.daily_summary
    SELECT 
        current_date() as report_date,
        COUNT(DISTINCT customer_id) as total_customers,
        SUM(order_amount) as total_revenue
    FROM prod_db.orders
""")

# Create external table with schema
spark.sql("""
    CREATE TABLE IF NOT EXISTS warehouse_db.external_customers
    USING parquet
    LOCATION '/data/warehouse/customers'
    AS SELECT * FROM prod_db.customers
""")

print("Schema-qualified table operations completed!")
spark.stop()


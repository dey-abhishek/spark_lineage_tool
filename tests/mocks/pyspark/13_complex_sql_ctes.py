#!/usr/bin/env python3
"""
PySpark with complex SQL and CTEs
Tests: Complex SQL parsing, CTEs, subqueries
"""

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("ComplexSQL").getOrCreate()

# Complex SQL with CTEs
result = spark.sql("""
WITH customer_metrics AS (
    SELECT 
        user_id,
        COUNT(*) as order_count,
        SUM(amount) as total_spent,
        AVG(amount) as avg_order_value
    FROM prod.orders
    WHERE order_date >= '2024-01-01'
    GROUP BY user_id
),
top_customers AS (
    SELECT 
        user_id,
        total_spent,
        RANK() OVER (ORDER BY total_spent DESC) as rank
    FROM customer_metrics
    WHERE order_count > 5
)
INSERT OVERWRITE TABLE analytics.top_customers
SELECT 
    tc.user_id,
    u.user_name,
    u.email,
    tc.total_spent,
    tc.rank,
    CURRENT_DATE() as report_date
FROM top_customers tc
JOIN prod.users u ON tc.user_id = u.user_id
WHERE tc.rank <= 100
""")

# Multi-table insert
spark.sql("""
FROM prod.transactions t
INSERT INTO TABLE analytics.daily_sales
SELECT date, SUM(amount) as total_sales
GROUP BY date
INSERT INTO TABLE analytics.daily_orders
SELECT date, COUNT(*) as order_count
GROUP BY date
""")

spark.stop()


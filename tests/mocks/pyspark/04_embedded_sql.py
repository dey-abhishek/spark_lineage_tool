#!/usr/bin/env python3
"""PySpark job with embedded SQL."""

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("EmbeddedSQL").getOrCreate()

# Read using SQL
df1 = spark.sql("""
    SELECT 
        user_id,
        transaction_date,
        SUM(amount) as total_amount
    FROM prod.transactions
    WHERE transaction_date >= '2024-01-01'
    GROUP BY user_id, transaction_date
""")

# Complex SQL with multiple tables
df2 = spark.sql("""
    INSERT OVERWRITE TABLE analytics.user_metrics
    SELECT 
        u.user_id,
        u.user_name,
        COUNT(t.transaction_id) as transaction_count,
        SUM(t.amount) as total_spent
    FROM prod.users u
    LEFT JOIN prod.transactions t ON u.user_id = t.user_id
    GROUP BY u.user_id, u.user_name
""")

# Save results
df1.write.mode("overwrite").insertInto("analytics.daily_summaries")

spark.stop()


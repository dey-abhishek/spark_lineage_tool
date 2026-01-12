#!/usr/bin/env python3
"""PySpark with caching and UDFs."""

from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

def categorize_age(age):
    if age < 18:
        return "minor"
    elif age < 65:
        return "adult"
    return "senior"

spark = SparkSession.builder.appName("CachingUDFs").getOrCreate()

categorize_udf = udf(categorize_age, StringType())

df = spark.read.parquet("/data/raw/users")
df.cache()  # Cache for reuse

df_categorized = df.withColumn("age_category", categorize_udf(df.age))

df_categorized.write.parquet("/data/processed/users_categorized")
df_categorized.write.csv("/data/processed/users_categorized_csv")


#!/usr/bin/env python3
"""
PySpark with UDFs and error handling
Tests: UDF registration, bad records handling, data quality
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.types import StringType, IntegerType
import re

spark = SparkSession.builder.appName("DataQuality").getOrCreate()

# Define UDFs
@udf(returnType=StringType())
def clean_phone(phone):
    if phone:
        return re.sub(r'[^0-9]', '', phone)
    return None

@udf(returnType=IntegerType())
def calculate_age_bucket(age):
    if age < 18:
        return 1
    elif age < 35:
        return 2
    elif age < 55:
        return 3
    return 4

# Read with bad records handling
df = spark.read \
    .option("badRecordsPath", "/data/errors/bad_records") \
    .option("mode", "PERMISSIVE") \
    .option("columnNameOfCorruptRecord", "_corrupt_record") \
    .json("/data/raw/user_data")

# Apply UDFs
df_cleaned = df \
    .withColumn("phone_cleaned", clean_phone(col("phone"))) \
    .withColumn("age_bucket", calculate_age_bucket(col("age")))

# Separate good and bad records
df_good = df_cleaned.filter(col("_corrupt_record").isNull())
df_bad = df_cleaned.filter(col("_corrupt_record").isNotNull())

# Write good records
df_good.drop("_corrupt_record").write.mode("overwrite") \
    .parquet("/data/processed/users_cleaned")

# Write bad records for investigation
df_bad.write.mode("overwrite") \
    .parquet("/data/errors/corrupt_records")

# Data quality metrics
df_metrics = spark.sql("""
    SELECT 
        'user_data' as table_name,
        COUNT(*) as total_records,
        SUM(CASE WHEN phone_cleaned IS NULL THEN 1 ELSE 0 END) as null_phones,
        CURRENT_TIMESTAMP() as check_time
    FROM df_good
""")

df_metrics.write.mode("append").saveAsTable("analytics.data_quality_metrics")

spark.stop()


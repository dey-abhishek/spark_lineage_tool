"""
Stage 1: Data Ingestion Pipeline
Ingests raw customer data from Oracle database into Bronze layer (HDFS)
Output: /data/bronze/customers_raw
"""

from pyspark.sql import SparkSession
from datetime import datetime

# Initialize Spark
spark = SparkSession.builder \
    .appName("Stage1-CustomerIngestion") \
    .enableHiveSupport() \
    .getOrCreate()

# Configuration
jdbc_url = "jdbc:oracle:thin:@prod-oracle-db:1521:ORCL"
source_table = "PROD_CRM.CUSTOMERS"
bronze_path = "/data/bronze/customers_raw"
run_date = datetime.now().strftime("%Y-%m-%d")

# JDBC connection properties
jdbc_properties = {
    "user": "etl_user",
    "password": "***",
    "driver": "oracle.jdbc.OracleDriver",
    "fetchsize": "10000"
}

print(f"[Stage 1] Starting customer data ingestion from Oracle...")
print(f"[Stage 1] Source: {source_table}")
print(f"[Stage 1] Target: {bronze_path}")

# Read from Oracle
customers_df = spark.read \
    .jdbc(
        url=jdbc_url,
        table=source_table,
        properties=jdbc_properties
    )

# Add ingestion metadata
from pyspark.sql.functions import lit, current_timestamp

customers_with_metadata = customers_df \
    .withColumn("ingestion_date", lit(run_date)) \
    .withColumn("ingestion_timestamp", current_timestamp()) \
    .withColumn("source_system", lit("ORACLE_CRM"))

# Write to Bronze layer (HDFS) - Parquet format
customers_with_metadata.write \
    .mode("overwrite") \
    .partitionBy("ingestion_date") \
    .parquet(bronze_path)

record_count = customers_with_metadata.count()
print(f"[Stage 1] Successfully ingested {record_count} records to Bronze layer")
print(f"[Stage 1] Output path: {bronze_path}")

# Also write metadata to tracking table
tracking_df = spark.createDataFrame([
    (run_date, "stage1_ingest", bronze_path, record_count, "SUCCESS")
], ["run_date", "stage", "output_path", "record_count", "status"])

tracking_df.write \
    .mode("append") \
    .saveAsTable("etl_metadata.pipeline_tracking")

spark.stop()


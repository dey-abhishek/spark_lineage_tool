#!/usr/bin/env python3
"""
PySpark with SFTP using paramiko library
Direct SFTP operations using Python libraries within Spark
"""

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
import paramiko
import os
from io import StringIO

# SFTP Configuration
SFTP_HOST = "data.vendor.com"
SFTP_PORT = 22
SFTP_USER = "spark_ingest"
SFTP_KEY_PATH = "/keys/spark_sftp_key"
REMOTE_DIR = "/feeds/realtime"
LOCAL_TEMP = "/tmp/sftp_staging"

spark = SparkSession.builder \
    .appName("SFTP with Paramiko Integration") \
    .getOrCreate()

def download_from_sftp(host, user, key_path, remote_path, local_path):
    """Download file from SFTP using paramiko"""
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    
    # Connect using SSH key
    private_key = paramiko.RSAKey.from_private_key_file(key_path)
    ssh.connect(host, port=SFTP_PORT, username=user, pkey=private_key)
    
    sftp = ssh.open_sftp()
    sftp.get(remote_path, local_path)
    sftp.close()
    ssh.close()

# Download multiple files from SFTP
import subprocess
from datetime import datetime

RUN_DATE = datetime.now().strftime("%Y-%m-%d")

# Download customer updates
customer_remote = f"{REMOTE_DIR}/customer_updates_{RUN_DATE}.csv"
customer_local = f"{LOCAL_TEMP}/customer_updates.csv"

# In real Spark, this would be done in a UDF or driver
# For lineage tracking, we care about the SFTP paths
print(f"Downloading from sftp://{SFTP_HOST}{customer_remote}")

# Read the downloaded file into Spark
customer_schema = StructType([
    StructField("customer_id", StringType(), True),
    StructField("name", StringType(), True),
    StructField("email", StringType(), True),
    StructField("status", StringType(), True)
])

customers_df = spark.read \
    .schema(customer_schema) \
    .option("header", "true") \
    .csv(customer_local)

# Process and write to HDFS
customers_df.write \
    .mode("overwrite") \
    .parquet(f"/data/processed/customer_updates/{RUN_DATE}")

# Write to Hive
customers_df.write \
    .mode("append") \
    .saveAsTable("raw.customer_updates")

# Upload results back to SFTP
results_df = spark.sql("""
    SELECT customer_id, status, count(*) as update_count
    FROM raw.customer_updates
    WHERE date = current_date()
    GROUP BY customer_id, status
""")

# Write to temp location
results_local = f"{LOCAL_TEMP}/update_summary_{RUN_DATE}.csv"
results_df.coalesce(1).write \
    .mode("overwrite") \
    .option("header", "true") \
    .csv(results_local)

# Upload to SFTP (this shows the intent for lineage tracking)
results_remote = f"{REMOTE_DIR}/processed/update_summary_{RUN_DATE}.csv"
print(f"Uploading to sftp://{SFTP_HOST}{results_remote}")

spark.stop()


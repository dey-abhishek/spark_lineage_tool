"""
PySpark job demonstrating all file formats and RDD operations
Covers: Parquet, JSON, Avro, ORC, Text, CSV, Binary formats
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark import RDD
import sys

def main(run_date, env):
    spark = SparkSession.builder \
        .appName("MultiFormatETL") \
        .config("spark.sql.avro.compression.codec", "snappy") \
        .enableHiveSupport() \
        .getOrCreate()
    
    sc = spark.sparkContext
    base_path = f"/data/{env}"
    
    # ========== READ: Multiple Formats ==========
    
    # 1. Read Parquet
    df_parquet = spark.read \
        .option("mergeSchema", "true") \
        .parquet(f"{base_path}/raw/transactions/{run_date}/*.parquet")
    
    # 2. Read JSON (multiline and single-line)
    df_json = spark.read \
        .option("multiLine", "true") \
        .option("mode", "PERMISSIVE") \
        .json(f"{base_path}/raw/events/{run_date}/*.json")
    
    # 3. Read Avro
    df_avro = spark.read \
        .format("avro") \
        .load(f"{base_path}/raw/users/{run_date}/*.avro")
    
    # 4. Read ORC
    df_orc = spark.read \
        .orc(f"{base_path}/raw/products/{run_date}/*.orc")
    
    # 5. Read CSV with schema
    csv_schema = StructType([
        StructField("id", StringType(), True),
        StructField("name", StringType(), True),
        StructField("value", DoubleType(), True)
    ])
    df_csv = spark.read \
        .option("header", "true") \
        .option("delimiter", "|") \
        .schema(csv_schema) \
        .csv(f"{base_path}/raw/metadata/{run_date}/*.csv")
    
    # 6. Read Text files (logs)
    df_text = spark.read \
        .text(f"{base_path}/raw/logs/{run_date}/*.log")
    
    # 7. Read Binary files
    df_binary = spark.read \
        .format("binaryFile") \
        .load(f"{base_path}/raw/images/{run_date}/*.jpg")
    
    # 8. Read from Hive tables
    df_hive_customers = spark.table(f"{env}_warehouse.customers")
    df_hive_orders = spark.sql(f"SELECT * FROM {env}_warehouse.orders WHERE order_date = '{run_date}'")
    
    # ========== RDD Operations ==========
    
    # 9. Convert DataFrame to RDD for custom processing
    rdd_transactions = df_parquet.rdd.map(lambda row: (row.transaction_id, row.amount))
    
    # 10. Read text file as RDD
    rdd_logs = sc.textFile(f"{base_path}/raw/app_logs/{run_date}/*.txt")
    
    # 11. Process RDD with transformations
    rdd_parsed = rdd_logs \
        .filter(lambda line: "ERROR" in line) \
        .map(lambda line: line.split("|")) \
        .filter(lambda parts: len(parts) >= 5)
    
    # 12. Convert RDD back to DataFrame
    df_errors = rdd_parsed.toDF(["timestamp", "level", "component", "message", "stack_trace"])
    
    # 13. Read sequence file (RDD)
    rdd_sequence = sc.sequenceFile(f"{base_path}/raw/sequence/{run_date}/part-*")
    
    # 14. Read object file (RDD)
    rdd_objects = sc.objectFile(f"{base_path}/raw/pickled/{run_date}/*.pickle")
    
    # ========== Complex Joins with Multiple Formats ==========
    
    df_enriched = df_parquet \
        .join(df_avro, "user_id", "left") \
        .join(df_json, "event_id", "left") \
        .join(df_hive_customers, "customer_id", "left") \
        .join(df_orc, "product_id", "left")
    
    # ========== WRITE: Multiple Formats ==========
    
    # 15. Write Parquet with partitioning
    df_enriched.write \
        .mode("overwrite") \
        .partitionBy("run_date", "region") \
        .parquet(f"{base_path}/processed/enriched_transactions/{run_date}")
    
    # 16. Write JSON
    df_json.select("event_id", "event_type", "payload") \
        .write \
        .mode("append") \
        .json(f"{base_path}/processed/events_json/{run_date}")
    
    # 17. Write Avro
    df_avro.write \
        .format("avro") \
        .mode("overwrite") \
        .save(f"{base_path}/processed/users_avro/{run_date}")
    
    # 18. Write ORC with compression
    df_orc.write \
        .option("compression", "zlib") \
        .mode("overwrite") \
        .orc(f"{base_path}/processed/products_orc/{run_date}")
    
    # 19. Write CSV with header
    df_csv.write \
        .option("header", "true") \
        .option("delimiter", ",") \
        .mode("overwrite") \
        .csv(f"{base_path}/processed/metadata_csv/{run_date}")
    
    # 20. Write Text file (single column)
    df_text.write \
        .mode("overwrite") \
        .text(f"{base_path}/processed/logs_text/{run_date}")
    
    # 21. Write to Hive table (managed)
    df_enriched.write \
        .mode("overwrite") \
        .saveAsTable(f"{env}_analytics.enriched_transactions")
    
    # 22. Insert into existing Hive table
    df_enriched.write \
        .mode("append") \
        .insertInto(f"{env}_analytics.daily_transactions")
    
    # 23. Create Hive table with specific format
    df_enriched.write \
        .mode("overwrite") \
        .format("orc") \
        .option("path", f"{base_path}/warehouse/transactions_orc") \
        .saveAsTable(f"{env}_warehouse.transactions_orc")
    
    # ========== RDD Output Operations ==========
    
    # 24. Save RDD as text file
    rdd_errors_formatted = rdd_parsed.map(lambda parts: ",".join(parts))
    rdd_errors_formatted.saveAsTextFile(f"{base_path}/processed/error_logs/{run_date}")
    
    # 25. Save RDD as sequence file
    rdd_key_value = rdd_transactions.map(lambda x: (x[0], str(x[1])))
    rdd_key_value.saveAsSequenceFile(f"{base_path}/processed/transactions_seq/{run_date}")
    
    # 26. Save RDD as object file
    rdd_objects_filtered = rdd_objects.filter(lambda obj: obj is not None)
    rdd_objects_filtered.saveAsPickleFile(f"{base_path}/processed/objects_pickle/{run_date}")
    
    # ========== Mixed Format Pipeline ==========
    
    # Read from Hive, process, write to multiple formats
    df_orders = spark.table(f"{env}_warehouse.orders")
    
    # Aggregate and write to different formats
    df_summary = df_orders.groupBy("customer_id", "order_date").agg(
        count("*").alias("order_count"),
        sum("order_total").alias("total_spent")
    )
    
    # Write summary in multiple formats for different consumers
    df_summary.write.mode("overwrite").parquet(f"{base_path}/reports/order_summary.parquet")
    df_summary.write.mode("overwrite").json(f"{base_path}/reports/order_summary.json")
    df_summary.write.mode("overwrite").format("avro").save(f"{base_path}/reports/order_summary.avro")
    
    # Write to Hive for SQL access
    df_summary.write.mode("overwrite").saveAsTable(f"{env}_reports.order_summary_daily")
    
    spark.stop()


if __name__ == "__main__":
    run_date = sys.argv[1] if len(sys.argv) > 1 else "2024-01-01"
    env = sys.argv[2] if len(sys.argv) > 2 else "prod"
    main(run_date, env)


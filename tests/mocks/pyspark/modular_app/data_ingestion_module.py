"""
data_ingestion_module.py
Handles all data ingestion operations for the ETL pipeline
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col
from typing import Optional


class DataIngestionModule:
    """Module responsible for reading data from various sources"""
    
    def __init__(self, spark: SparkSession, env: str = "prod"):
        self.spark = spark
        self.env = env
    
    def read_customers(self) -> DataFrame:
        """Read customer data from Hive"""
        table_name = f"{self.env}_raw.customers"
        print(f"Reading customers from: {table_name}")
        
        return (self.spark.table(table_name)
                .filter(col("status") == "active")
                .select("customer_id", "name", "email", "registration_date", "region"))
    
    def read_transactions(self, run_date: str, base_path: str = "/data/raw") -> DataFrame:
        """Read transaction data from HDFS Parquet"""
        path = f"{base_path}/transactions/date={run_date}/*.parquet"
        print(f"Reading transactions from: {path}")
        
        return (self.spark.read
                .option("mergeSchema", "true")
                .parquet(path)
                .select("transaction_id", "customer_id", "product_id", "amount", "timestamp", "status"))
    
    def read_products(self, jdbc_url: str = "jdbc:mysql://prod-mysql:3306/catalog") -> DataFrame:
        """Read product catalog from JDBC (MySQL)"""
        print(f"Reading products from: {jdbc_url}")
        
        return (self.spark.read
                .format("jdbc")
                .option("url", jdbc_url)
                .option("dbtable", "products")
                .option("user", "etl_user")
                .option("password", "${MYSQL_PASSWORD}")
                .option("driver", "com.mysql.jdbc.Driver")
                .load()
                .select("product_id", "product_name", "category", "price", "supplier_id"))
    
    def read_orders(self, run_date: str) -> DataFrame:
        """Read order data from Hive staging table"""
        table_name = f"{self.env}_staging.orders"
        print(f"Reading orders from: {table_name}")
        
        return (self.spark.table(table_name)
                .filter(col("order_date") == run_date)
                .select("order_id", "customer_id", "order_date", "total_amount", "status"))
    
    def read_events_stream(self, topic: str = "customer-events") -> DataFrame:
        """Read event stream from Kafka"""
        print(f"Reading events from Kafka topic: {topic}")
        
        return (self.spark.readStream
                .format("kafka")
                .option("kafka.bootstrap.servers", "kafka-broker:9092")
                .option("subscribe", topic)
                .option("startingOffsets", "latest")
                .load()
                .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)", "timestamp"))
    
    def read_reference_data(self, s3_bucket: str = "s3a://company-data") -> DataFrame:
        """Read reference data from S3"""
        path = f"{s3_bucket}/reference/regions/*.json"
        print(f"Reading reference data from: {path}")
        
        return (self.spark.read
                .option("multiLine", "true")
                .json(path))
    
    def read_inventory(self, warehouse_path: str = "/data/warehouse") -> DataFrame:
        """Read inventory data from Avro files"""
        path = f"{warehouse_path}/inventory/*.avro"
        print(f"Reading inventory from: {path}")
        
        return (self.spark.read
                .format("avro")
                .load(path)
                .select("product_id", "quantity", "warehouse_id", "last_updated"))
    
    def read_audit_logs(self, run_date: str, hdfs_path: str = "/data/audit") -> DataFrame:
        """Read audit logs from HDFS text files"""
        path = f"{hdfs_path}/logs/date={run_date}/*.log"
        print(f"Reading audit logs from: {path}")
        
        return (self.spark.read
                .text(path)
                .toDF("log_entry"))


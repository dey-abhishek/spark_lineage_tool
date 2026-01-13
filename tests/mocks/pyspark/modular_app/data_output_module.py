"""
data_output_module.py
Handles all data writing operations to various sinks
"""

from pyspark.sql import DataFrame
from typing import Optional


class DataOutputModule:
    """Module responsible for writing data to various sinks"""
    
    def __init__(self, spark, env: str = "prod"):
        self.spark = spark
        self.env = env
    
    def write_enriched_transactions(self, df: DataFrame, run_date: str) -> None:
        """Write enriched transactions to Hive Gold layer"""
        table_name = f"{self.env}_gold.enriched_transactions"
        print(f"Writing enriched transactions to: {table_name}")
        
        (df.write
         .mode("append")
         .partitionBy("run_date")
         .format("parquet")
         .saveAsTable(table_name))
    
    def write_customer_metrics(self, df: DataFrame, run_date: str, 
                               base_path: str = "/data/gold") -> None:
        """Write customer metrics to HDFS"""
        path = f"{base_path}/customer_metrics/date={run_date}"
        print(f"Writing customer metrics to: {path}")
        
        (df.write
         .mode("overwrite")
         .partitionBy("region")
         .parquet(path))
    
    def write_fraud_alerts(self, df: DataFrame, 
                          jdbc_url: str = "jdbc:postgresql://prod-pg:5432/alerts") -> None:
        """Write fraud alerts to PostgreSQL"""
        print(f"Writing fraud alerts to: {jdbc_url}")
        
        (df.write
         .mode("append")
         .format("jdbc")
         .option("url", jdbc_url)
         .option("dbtable", "fraud_alerts")
         .option("user", "etl_user")
         .option("password", "${PG_PASSWORD}")
         .option("driver", "org.postgresql.Driver")
         .save())
    
    def write_product_summary(self, df: DataFrame) -> None:
        """Write product summary to Hive reporting layer"""
        table_name = f"{self.env}_reports.product_daily_summary"
        print(f"Writing product summary to: {table_name}")
        
        (df.write
         .mode("overwrite")
         .format("orc")
         .option("compression", "snappy")
         .saveAsTable(table_name))
    
    def write_inventory_status(self, df: DataFrame, run_date: str) -> None:
        """Write inventory status to Hive warehouse table"""
        table_name = f"{self.env}_warehouse.inventory_status"
        print(f"Writing inventory status to: {table_name}")
        
        (df.write
         .mode("overwrite")
         .insertInto(table_name))
    
    def write_events_to_kafka(self, df: DataFrame, topic: str = "processed-events") -> None:
        """Write processed events to Kafka"""
        print(f"Writing processed events to Kafka topic: {topic}")
        
        (df.selectExpr("CAST(customer_id AS STRING) AS key", "to_json(struct(*)) AS value")
         .writeStream
         .format("kafka")
         .option("kafka.bootstrap.servers", "kafka-broker:9092")
         .option("topic", topic)
         .option("checkpointLocation", f"/tmp/checkpoints/{topic}")
         .trigger(processingTime="30 seconds")
         .start())
    
    def write_to_delta(self, df: DataFrame, table_name: str, 
                      base_path: str = "/data/delta") -> None:
        """Write data to Delta Lake"""
        path = f"{base_path}/{table_name}"
        print(f"Writing to Delta Lake: {path}")
        
        (df.write
         .format("delta")
         .mode("overwrite")
         .option("mergeSchema", "true")
         .save(path))
    
    def export_to_s3(self, df: DataFrame, report_name: str, run_date: str,
                    s3_bucket: str = "s3a://company-reports") -> None:
        """Export reports to S3 as CSV"""
        path = f"{s3_bucket}/daily/{report_name}/date={run_date}"
        print(f"Exporting report to S3: {path}")
        
        (df.write
         .mode("overwrite")
         .option("header", "true")
         .option("compression", "gzip")
         .csv(path))
    
    def write_customer_segments(self, df: DataFrame, run_date: str) -> None:
        """Write customer segments to Hive analytics table"""
        table_name = f"{self.env}_analytics.customer_segments"
        print(f"Writing customer segments to: {table_name}")
        
        (df.write
         .mode("append")
         .partitionBy("run_date")
         .format("parquet")
         .saveAsTable(table_name))
    
    def write_daily_snapshot(self, df: DataFrame, run_date: str,
                            base_path: str = "/data/snapshots") -> None:
        """Write daily snapshot to HDFS"""
        path = f"{base_path}/daily_snapshot/date={run_date}"
        print(f"Writing daily snapshot to: {path}")
        
        (df.write
         .mode("overwrite")
         .parquet(path))
    
    def write_to_redshift(self, df: DataFrame, table_name: str,
                         redshift_url: str = "jdbc:redshift://prod-rs:5439/warehouse") -> None:
        """Write data to AWS Redshift"""
        print(f"Writing to Redshift table: {table_name}")
        
        (df.write
         .format("jdbc")
         .option("url", redshift_url)
         .option("dbtable", table_name)
         .option("user", "etl_user")
         .option("password", "${REDSHIFT_PASSWORD}")
         .option("driver", "com.amazon.redshift.jdbc.Driver")
         .option("tempdir", "s3a://temp-bucket/redshift-temp")
         .mode("append")
         .save())


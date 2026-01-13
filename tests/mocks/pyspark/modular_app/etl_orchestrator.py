"""
etl_orchestrator.py
Main orchestrator for the modularized PySpark ETL pipeline
Coordinates all modules to execute the complete data flow
"""

import sys
from datetime import datetime
from pyspark.sql import SparkSession

from data_ingestion_module import DataIngestionModule
from transformation_module import TransformationModule
from data_output_module import DataOutputModule


class ETLOrchestrator:
    """Orchestrates the entire ETL pipeline"""
    
    def __init__(self, env: str, run_date: str, base_path: str = "/data"):
        self.env = env
        self.run_date = run_date
        self.base_path = base_path
        
        print(f"Initializing ETL Pipeline - env: {env}, run_date: {run_date}, base_path: {base_path}")
        
        # Initialize Spark session
        self.spark = (SparkSession.builder
                     .appName(f"ETL-Pipeline-{env}-{run_date}")
                     .config("spark.sql.warehouse.dir", f"{base_path}/warehouse")
                     .config("spark.sql.adaptive.enabled", "true")
                     .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
                     .config("spark.sql.hive.convertMetastoreParquet", "true")
                     .enableHiveSupport()
                     .getOrCreate())
        
        # Initialize modules
        self.ingestion = DataIngestionModule(self.spark, env)
        self.transformation = TransformationModule(self.spark)
        self.output = DataOutputModule(self.spark, env)
    
    def run_batch_pipeline(self) -> None:
        """Execute the batch ETL pipeline"""
        try:
            print("\n=== Starting Ingestion Phase ===")
            self._ingestion_phase()
            
            print("\n=== Starting Transformation Phase ===")
            self._transformation_phase()
            
            print("\n=== Starting Output Phase ===")
            self._output_phase()
            
            print("\n=== ETL Pipeline Completed Successfully ===")
            
        except Exception as e:
            print(f"ETL Pipeline failed: {str(e)}")
            raise
    
    def _ingestion_phase(self) -> None:
        """Ingestion phase - read all source data"""
        # Read from Hive tables
        self.customers = self.ingestion.read_customers()
        self.orders = self.ingestion.read_orders(self.run_date)
        
        # Read from HDFS
        self.transactions = self.ingestion.read_transactions(
            self.run_date, 
            f"{self.base_path}/raw"
        )
        
        # Read from JDBC
        self.products = self.ingestion.read_products()
        
        # Read from S3
        self.reference_data = self.ingestion.read_reference_data(
            f"s3a://company-data-{self.env}"
        )
        
        # Read from Avro files
        self.inventory = self.ingestion.read_inventory(f"{self.base_path}/warehouse")
        
        # Read audit logs
        self.audit_logs = self.ingestion.read_audit_logs(
            self.run_date, 
            f"{self.base_path}/audit"
        )
        
        # Cache frequently used datasets
        self.customers.cache()
        self.products.cache()
        
        print(f"Ingested {self.customers.count()} customers")
        print(f"Ingested {self.transactions.count()} transactions")
        print(f"Ingested {self.products.count()} products")
        print(f"Ingested {self.orders.count()} orders")
    
    def _transformation_phase(self) -> None:
        """Transformation phase - apply business logic"""
        # Enrich transactions with customer and product data
        self.enriched_transactions = self.transformation.enrich_transactions(
            self.transactions, 
            self.customers, 
            self.products
        )
        
        # Calculate customer-level metrics
        self.customer_metrics = self.transformation.calculate_customer_metrics(
            self.enriched_transactions, 
            self.run_date
        )
        
        # Detect fraudulent transactions
        self.fraud_alerts = self.transformation.detect_fraud(self.enriched_transactions)
        
        # Generate product summary
        self.product_summary = self.transformation.generate_product_summary(
            self.enriched_transactions, 
            self.run_date
        )
        
        # Calculate inventory status
        self.inventory_status = self.transformation.calculate_inventory_status(
            self.inventory, 
            self.transactions
        )
        
        # Generate customer segments
        self.customer_segments = self.transformation.generate_customer_segments(
            self.customer_metrics
        )
        
        # Create daily snapshot
        self.daily_snapshot = self.transformation.create_daily_snapshot(
            self.transactions, 
            self.orders, 
            self.run_date
        )
        
        print(f"Enriched {self.enriched_transactions.count()} transactions")
        print(f"Calculated metrics for {self.customer_metrics.count()} customers")
        print(f"Detected {self.fraud_alerts.count()} fraud alerts")
        print(f"Generated summary for {self.product_summary.count()} products")
    
    def _output_phase(self) -> None:
        """Output phase - write results to various sinks"""
        # Write to Hive tables
        self.output.write_enriched_transactions(self.enriched_transactions, self.run_date)
        self.output.write_product_summary(self.product_summary)
        self.output.write_inventory_status(self.inventory_status, self.run_date)
        self.output.write_customer_segments(self.customer_segments, self.run_date)
        
        # Write to HDFS
        self.output.write_customer_metrics(
            self.customer_metrics, 
            self.run_date, 
            f"{self.base_path}/gold"
        )
        self.output.write_daily_snapshot(
            self.daily_snapshot, 
            self.run_date, 
            f"{self.base_path}/snapshots"
        )
        
        # Write to JDBC (PostgreSQL)
        self.output.write_fraud_alerts(self.fraud_alerts)
        
        # Write to Delta Lake
        self.output.write_to_delta(self.customer_metrics, "customer_metrics", f"{self.base_path}/delta")
        
        # Export to S3
        self.output.export_to_s3(
            self.product_summary, 
            "product_summary", 
            self.run_date, 
            f"s3a://company-reports-{self.env}"
        )
        
        # Write to Redshift
        if self.env == "prod":
            self.output.write_to_redshift(self.customer_segments, "customer_segments")
    
    def run_streaming_pipeline(self) -> None:
        """Execute the streaming pipeline"""
        print("\n=== Starting Streaming Phase ===")
        
        try:
            # Read from Kafka
            events_stream = self.ingestion.read_events_stream("customer-events")
            
            # Process events
            processed_events = self.transformation.process_events(events_stream)
            
            # Write to Kafka
            self.output.write_events_to_kafka(processed_events, "processed-customer-events")
            
            # Keep streaming job running
            self.spark.streams.awaitAnyTermination()
            
        except Exception as e:
            print(f"Streaming Pipeline failed: {str(e)}")
            raise
    
    def cleanup(self) -> None:
        """Cleanup resources"""
        print("Cleaning up resources...")
        self.spark.stop()


def main():
    """Main entry point"""
    # Parse command-line arguments
    env = sys.argv[1] if len(sys.argv) > 1 else "prod"
    run_date = sys.argv[2] if len(sys.argv) > 2 else datetime.now().strftime("%Y-%m-%d")
    base_path = sys.argv[3] if len(sys.argv) > 3 else "/data"
    
    # Initialize orchestrator
    orchestrator = ETLOrchestrator(env, run_date, base_path)
    
    try:
        # Run batch pipeline
        orchestrator.run_batch_pipeline()
        
        # Run streaming pipeline (only in prod)
        if env == "prod":
            orchestrator.run_streaming_pipeline()
        
    except Exception as e:
        print(f"Pipeline execution failed: {str(e)}")
        sys.exit(1)
    finally:
        orchestrator.cleanup()


if __name__ == "__main__":
    main()


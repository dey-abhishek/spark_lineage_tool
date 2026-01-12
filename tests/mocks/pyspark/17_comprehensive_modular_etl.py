"""
Real-world PySpark job demonstrating modularized architecture
This file imports from multiple custom modules that would need to be tracked
"""
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, lit, when, coalesce, concat, concat_ws, 
    to_date, date_format, datediff, months_between,
    sum, count, avg, max, min, first, last,
    row_number, rank, dense_rank, lag, lead,
    explode, posexplode, array, struct, map_from_arrays,
    udf, pandas_udf, PandasUDFType
)
from pyspark.sql.types import *
from pyspark.sql.window import Window
from typing import List, Dict, Optional
import sys
import os

# Custom module imports - these show modular code organization
from etl.core.base_job import BaseSparkJob
from etl.readers.jdbc_reader import OracleReader, PostgresReader, MySQLReader
from etl.readers.file_reader import HDFSReader, S3Reader, SFTPFileReader
from etl.readers.api_reader import RestAPIReader, GraphQLReader
from etl.readers.stream_reader import KafkaStreamReader, KinesisStreamReader
from etl.writers.jdbc_writer import PostgresWriter, MySQLWriter
from etl.writers.file_writer import ParquetWriter, DeltaWriter, IcebergWriter
from etl.writers.stream_writer import KafkaStreamWriter
from etl.transformers.data_quality import (
    NullChecker, DuplicateChecker, SchemaValidator,
    DataProfiler, OutlierDetector
)
from etl.transformers.business_logic import (
    CustomerSegmentation, ProductRecommendation,
    FraudDetection, ChurnPrediction
)
from etl.transformers.feature_engineering import (
    FeatureExtractor, FeatureScaler, FeatureEncoder,
    TimeSeriesFeatureBuilder, TextFeatureExtractor
)
from etl.utils.config_manager import ConfigManager, EnvironmentConfig
from etl.utils.secret_manager import VaultSecretManager, AWSSecretManager
from etl.utils.logger import StructuredLogger, MetricsCollector
from etl.utils.schema_registry import AvroSchemaRegistry, ProtobufSchemaRegistry
from etl.utils.data_catalog import GlueCatalog, HiveCatalog, UnityCatalog
from common.constants import (
    HDFS_BASE_PATH, S3_BUCKET, DATABASE_CONFIGS,
    KAFKA_BROKERS, SCHEMA_REGISTRY_URL
)
from common.exceptions import (
    DataQualityException, SchemaValidationException,
    JDBCConnectionException, StreamingException
)
from common.helpers import (
    generate_surrogate_key, parse_complex_json,
    decrypt_column, mask_pii, validate_email
)


class ComprehensiveModularETL(BaseSparkJob):
    """
    Demonstrates a fully modularized ETL job with imports from multiple packages
    """
    
    def __init__(self, config_path: str):
        super().__init__("ComprehensiveModularETL")
        self.config_manager = ConfigManager(config_path)
        self.env_config = self.config_manager.load_environment_config()
        self.secret_manager = VaultSecretManager(self.env_config.vault_url)
        self.logger = StructuredLogger(self.__class__.__name__)
        self.metrics = MetricsCollector()
        self.schema_registry = AvroSchemaRegistry(SCHEMA_REGISTRY_URL)
        
    def initialize_readers(self, spark: SparkSession) -> Dict[str, any]:
        """Initialize all data readers"""
        return {
            'oracle': OracleReader(spark, self.secret_manager, self.env_config.oracle_config),
            'postgres': PostgresReader(spark, self.secret_manager, self.env_config.postgres_config),
            'mysql': MySQLReader(spark, self.secret_manager, self.env_config.mysql_config),
            'hdfs': HDFSReader(spark, HDFS_BASE_PATH),
            's3': S3Reader(spark, S3_BUCKET),
            'sftp': SFTPFileReader(self.secret_manager, self.env_config.sftp_config),
            'rest_api': RestAPIReader(self.secret_manager),
            'kafka': KafkaStreamReader(spark, KAFKA_BROKERS, self.schema_registry)
        }
    
    def initialize_writers(self, spark: SparkSession) -> Dict[str, any]:
        """Initialize all data writers"""
        return {
            'postgres': PostgresWriter(spark, self.secret_manager, self.env_config.postgres_target),
            'mysql': MySQLWriter(spark, self.secret_manager, self.env_config.mysql_target),
            'parquet': ParquetWriter(spark, HDFS_BASE_PATH),
            'delta': DeltaWriter(spark, f"{HDFS_BASE_PATH}/delta"),
            'iceberg': IcebergWriter(spark, self.env_config.iceberg_catalog),
            'kafka': KafkaStreamWriter(spark, KAFKA_BROKERS)
        }
    
    def read_source_data(self, readers: Dict) -> Dict[str, DataFrame]:
        """Read data from all sources"""
        self.logger.info("Reading source data from multiple systems")
        
        # Read from Oracle
        df_customers = readers['oracle'].read_table(
            schema="CUSTOMER_DB",
            table="CUSTOMERS",
            partitions=20,
            partition_column="CUSTOMER_ID"
        )
        self.metrics.record_dataset_read("oracle.customers", df_customers.count())
        
        # Read from PostgreSQL with custom query
        df_transactions = readers['postgres'].read_query("""
            SELECT 
                t.transaction_id,
                t.customer_id,
                t.amount,
                t.transaction_date,
                t.merchant_id,
                m.merchant_name,
                m.merchant_category_code
            FROM transactions.fact_transactions t
            INNER JOIN merchants.dim_merchants m ON t.merchant_id = m.merchant_id
            WHERE t.transaction_date >= CURRENT_DATE - INTERVAL '90 days'
        """, partitions=50)
        self.metrics.record_dataset_read("postgres.transactions", df_transactions.count())
        
        # Read from MySQL
        df_products = readers['mysql'].read_table(
            database="ecommerce",
            table="products",
            filter_condition="is_active = true"
        )
        
        # Read from HDFS
        df_behavioral_data = readers['hdfs'].read_parquet(
            path=f"{HDFS_BASE_PATH}/raw/behavioral_data/{self.env_config.run_date}"
        )
        
        # Read from S3
        df_external_enrichment = readers['s3'].read_json(
            path=f"s3a://{S3_BUCKET}/enrichment/{self.env_config.run_date}/*.json"
        )
        
        # Read from SFTP (files are first downloaded to temp location)
        sftp_files = readers['sftp'].download_files(
            remote_path=f"/exports/{self.env_config.run_date}/",
            local_path=f"/tmp/sftp/{self.env_config.run_date}/",
            pattern="*.csv"
        )
        df_vendor_data = self.spark.read.csv(
            f"/tmp/sftp/{self.env_config.run_date}/*.csv",
            header=True,
            inferSchema=True
        )
        
        # Read from REST API
        df_api_data = readers['rest_api'].fetch_paginated(
            endpoint="https://api.partner.com/v2/customer_insights",
            params={"date": self.env_config.run_date},
            auth_token=self.secret_manager.get_secret("api/partner/token")
        )
        
        # Read from Kafka (streaming)
        df_realtime_events = readers['kafka'].read_stream(
            topic="customer_events",
            starting_offsets="latest",
            schema=self.schema_registry.get_schema("customer_event_v3")
        )
        
        return {
            'customers': df_customers,
            'transactions': df_transactions,
            'products': df_products,
            'behavioral': df_behavioral_data,
            'enrichment': df_external_enrichment,
            'vendor': df_vendor_data,
            'api': df_api_data,
            'realtime': df_realtime_events
        }
    
    def apply_data_quality_checks(self, dfs: Dict[str, DataFrame]) -> Dict[str, DataFrame]:
        """Apply comprehensive data quality checks"""
        self.logger.info("Applying data quality checks")
        
        # Initialize DQ transformers
        null_checker = NullChecker(critical_columns={
            'customers': ['customer_id', 'email'],
            'transactions': ['transaction_id', 'customer_id', 'amount']
        })
        
        duplicate_checker = DuplicateChecker()
        schema_validator = SchemaValidator(self.schema_registry)
        profiler = DataProfiler()
        outlier_detector = OutlierDetector(method='iqr', threshold=3.0)
        
        validated_dfs = {}
        
        for name, df in dfs.items():
            self.logger.info(f"Validating dataset: {name}")
            
            # Check for nulls
            df_checked = null_checker.check_and_flag(df, dataset_name=name)
            
            # Check for duplicates
            df_checked = duplicate_checker.deduplicate(
                df_checked,
                key_columns=['customer_id'] if name == 'customers' else None
            )
            
            # Validate schema
            try:
                df_checked = schema_validator.validate(df_checked, expected_schema_name=name)
            except SchemaValidationException as e:
                self.logger.error(f"Schema validation failed for {name}: {str(e)}")
                raise
            
            # Profile data
            profile = profiler.profile_dataset(df_checked)
            self.metrics.record_data_profile(name, profile)
            
            # Detect outliers (for numeric columns)
            if name in ['transactions']:
                df_checked = outlier_detector.detect_and_flag(df_checked, columns=['amount'])
            
            validated_dfs[name] = df_checked
        
        return validated_dfs
    
    def apply_transformations(self, dfs: Dict[str, DataFrame]) -> DataFrame:
        """Apply business logic and feature engineering"""
        self.logger.info("Applying transformations")
        
        # Initialize transformers
        segmentation = CustomerSegmentation(rules_path="/config/segmentation_rules.json")
        recommendation = ProductRecommendation(model_path="/models/recommendation_v2")
        fraud_detection = FraudDetection(model_path="/models/fraud_detector_v5")
        churn_prediction = ChurnPrediction(model_path="/models/churn_predictor_v3")
        
        # Feature engineering
        feature_extractor = FeatureExtractor()
        feature_scaler = FeatureScaler(method='standard')
        feature_encoder = FeatureEncoder(method='one_hot')
        ts_feature_builder = TimeSeriesFeatureBuilder(window_sizes=[7, 30, 90])
        
        # Join all datasets
        df_base = dfs['transactions'] \
            .join(dfs['customers'], 'customer_id', 'left') \
            .join(dfs['products'], 'product_id', 'left') \
            .join(dfs['behavioral'], 'customer_id', 'left') \
            .join(dfs['enrichment'], 'customer_id', 'left') \
            .join(dfs['vendor'], 'customer_id', 'left') \
            .join(dfs['api'], 'customer_id', 'left')
        
        # Apply segmentation
        df_segmented = segmentation.apply(df_base)
        
        # Generate product recommendations
        df_with_recs = recommendation.generate_recommendations(
            df_segmented,
            top_n=5,
            customer_col='customer_id',
            product_col='product_id'
        )
        
        # Run fraud detection
        df_fraud_scored = fraud_detection.score(
            df_with_recs,
            features=['amount', 'merchant_category_code', 'transaction_hour', 'distance_from_home']
        )
        
        # Predict churn
        df_churn_scored = churn_prediction.predict(
            df_fraud_scored,
            features=ts_feature_builder.build_features(df_fraud_scored, date_col='transaction_date')
        )
        
        # Extract and engineer features
        df_features = feature_extractor.extract_all(df_churn_scored)
        df_scaled = feature_scaler.fit_transform(df_features, numeric_cols=['amount', 'transaction_count'])
        df_final = feature_encoder.fit_transform(df_scaled, categorical_cols=['merchant_category', 'customer_segment'])
        
        return df_final
    
    def write_outputs(self, df: DataFrame, writers: Dict):
        """Write to multiple destinations"""
        self.logger.info("Writing outputs to multiple destinations")
        
        # Write to Delta Lake (primary storage)
        writers['delta'].write(
            df,
            path="/delta/customer_360",
            mode="overwrite",
            partition_by=['run_date', 'customer_segment']
        )
        
        # Write to Iceberg (for time travel and ACID)
        writers['iceberg'].write(
            df,
            table="analytics.customer_360_daily",
            mode="append"
        )
        
        # Write to Parquet (for compatibility)
        writers['parquet'].write(
            df,
            path=f"{HDFS_BASE_PATH}/warehouse/customer_360/{self.env_config.run_date}",
            mode="overwrite",
            partition_by=['customer_segment']
        )
        
        # Write aggregated metrics to PostgreSQL
        df_metrics = df.groupBy('customer_segment', 'fraud_risk_level').agg(
            count('customer_id').alias('customer_count'),
            sum('amount').alias('total_transaction_value'),
            avg('churn_probability').alias('avg_churn_probability')
        )
        
        writers['postgres'].write(
            df_metrics,
            schema="analytics",
            table="customer_metrics_daily",
            mode="append"
        )
        
        # Write high-risk transactions to MySQL for alerting
        df_high_risk = df.filter(col('fraud_score') > 0.8)
        writers['mysql'].write(
            df_high_risk,
            database="alerting",
            table="high_risk_transactions",
            mode="append"
        )
        
        # Write to Kafka for downstream consumers
        writers['kafka'].write_stream(
            df.selectExpr("to_json(struct(*)) AS value"),
            topic="customer_360_updates",
            checkpoint_location=f"{HDFS_BASE_PATH}/checkpoints/customer_360_kafka"
        )
    
    def run(self):
        """Execute the full ETL pipeline"""
        try:
            self.logger.info("Starting Comprehensive Modular ETL")
            self.metrics.start_job()
            
            # Initialize
            readers = self.initialize_readers(self.spark)
            writers = self.initialize_writers(self.spark)
            
            # Read
            source_dfs = self.read_source_data(readers)
            
            # Validate
            validated_dfs = self.apply_data_quality_checks(source_dfs)
            
            # Transform
            final_df = self.apply_transformations(validated_dfs)
            
            # Write
            self.write_outputs(final_df, writers)
            
            self.metrics.end_job(success=True)
            self.logger.info("Comprehensive Modular ETL completed successfully")
            
        except Exception as e:
            self.metrics.end_job(success=False, error=str(e))
            self.logger.error(f"ETL job failed: {str(e)}")
            raise


if __name__ == "__main__":
    config_path = sys.argv[1] if len(sys.argv) > 1 else "/config/etl_config.yaml"
    
    job = ComprehensiveModularETL(config_path)
    job.run()


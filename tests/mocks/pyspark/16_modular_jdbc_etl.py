"""
Modularized PySpark ETL job with JDBC sources and SFTP integration
Demonstrates real-world enterprise patterns with external dependencies
"""
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import *
from pyspark.sql.types import *
import os
import sys

# Import custom modules (demonstrates modularized code)
from etl.transformers import DataQualityTransformer, BusinessLogicTransformer
from etl.readers import JDBCReader, SFTPReader, KafkaReader
from etl.writers import JDBCWriter, HDFSWriter, HiveWriter
from etl.utils.config_loader import ConfigLoader
from etl.utils.logging_helper import setup_logger
from etl.utils.secret_manager import SecretManager

class EnterpriseETLJob:
    """
    Enterprise-grade ETL job with multiple data sources
    """
    
    def __init__(self, spark: SparkSession, config_path: str):
        self.spark = spark
        self.config = ConfigLoader.load(config_path)
        self.logger = setup_logger(self.__class__.__name__)
        self.secret_manager = SecretManager(self.config.get('vault_url'))
        
    def read_from_oracle(self) -> DataFrame:
        """Read customer data from Oracle database"""
        oracle_config = self.config.get('oracle')
        password = self.secret_manager.get_secret('oracle/prod/password')
        
        jdbc_url = f"jdbc:oracle:thin:@{oracle_config['host']}:{oracle_config['port']}:{oracle_config['sid']}"
        
        df = self.spark.read \
            .format("jdbc") \
            .option("url", jdbc_url) \
            .option("dbtable", "(SELECT * FROM CUSTOMER_DB.CUSTOMERS WHERE LAST_UPDATED >= TO_DATE(?, 'YYYY-MM-DD'))") \
            .option("user", oracle_config['username']) \
            .option("password", password) \
            .option("driver", "oracle.jdbc.OracleDriver") \
            .option("fetchsize", "10000") \
            .option("numPartitions", "20") \
            .option("partitionColumn", "CUSTOMER_ID") \
            .option("lowerBound", "1") \
            .option("upperBound", "10000000") \
            .load()
        
        self.logger.info(f"Read {df.count()} records from Oracle")
        return df
    
    def read_from_postgres(self) -> DataFrame:
        """Read transaction data from PostgreSQL"""
        pg_config = self.config.get('postgres')
        password = self.secret_manager.get_secret('postgres/prod/password')
        
        jdbc_url = f"jdbc:postgresql://{pg_config['host']}:{pg_config['port']}/{pg_config['database']}"
        
        # Use query pushdown for better performance
        query = """
            (SELECT 
                t.transaction_id,
                t.customer_id,
                t.amount,
                t.currency,
                t.transaction_date,
                t.merchant_id,
                m.merchant_name,
                m.merchant_category
            FROM transactions.fact_transactions t
            JOIN transactions.dim_merchants m ON t.merchant_id = m.merchant_id
            WHERE t.transaction_date >= CURRENT_DATE - INTERVAL '90 days'
            ) as transactions
        """
        
        df = self.spark.read \
            .format("jdbc") \
            .option("url", jdbc_url) \
            .option("dbtable", query) \
            .option("user", pg_config['username']) \
            .option("password", password) \
            .option("driver", "org.postgresql.Driver") \
            .option("fetchsize", "50000") \
            .option("numPartitions", "50") \
            .load()
        
        self.logger.info(f"Read {df.count()} transactions from PostgreSQL")
        return df
    
    def read_from_mysql(self) -> DataFrame:
        """Read product catalog from MySQL"""
        mysql_config = self.config.get('mysql')
        password = self.secret_manager.get_secret('mysql/prod/password')
        
        jdbc_url = f"jdbc:mysql://{mysql_config['host']}:{mysql_config['port']}/{mysql_config['database']}?useSSL=true"
        
        df = self.spark.read \
            .jdbc(
                url=jdbc_url,
                table="catalog.products",
                column="product_id",
                lowerBound=1,
                upperBound=1000000,
                numPartitions=10,
                properties={
                    "user": mysql_config['username'],
                    "password": password,
                    "driver": "com.mysql.cj.jdbc.Driver"
                }
            )
        
        return df
    
    def read_from_mssql(self) -> DataFrame:
        """Read sales data from Microsoft SQL Server"""
        mssql_config = self.config.get('mssql')
        password = self.secret_manager.get_secret('mssql/prod/password')
        
        jdbc_url = f"jdbc:sqlserver://{mssql_config['host']}:{mssql_config['port']};databaseName={mssql_config['database']}"
        
        # Use predicates for parallel reading
        predicates = [
            "sale_date >= '2024-01-01' AND sale_date < '2024-04-01'",
            "sale_date >= '2024-04-01' AND sale_date < '2024-07-01'",
            "sale_date >= '2024-07-01' AND sale_date < '2024-10-01'",
            "sale_date >= '2024-10-01' AND sale_date < '2025-01-01'"
        ]
        
        df = self.spark.read \
            .jdbc(
                url=jdbc_url,
                table="dbo.sales_fact",
                predicates=predicates,
                properties={
                    "user": mssql_config['username'],
                    "password": password,
                    "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"
                }
            )
        
        return df
    
    def read_from_sftp(self) -> DataFrame:
        """Read files from SFTP server"""
        sftp_config = self.config.get('sftp')
        sftp_key = self.secret_manager.get_secret('sftp/prod/private_key')
        
        # Custom SFTP reader that downloads files to temporary location
        sftp_reader = SFTPReader(
            host=sftp_config['host'],
            port=sftp_config['port'],
            username=sftp_config['username'],
            private_key=sftp_key,
            remote_path=f"/data/exports/{self.config.get('run_date')}/",
            local_staging_path=f"/tmp/sftp_staging/{self.config.get('run_date')}/"
        )
        
        # Download files from SFTP to local staging
        local_files = sftp_reader.download_all(pattern="*.csv")
        
        # Read from staging location
        df = self.spark.read \
            .option("header", "true") \
            .option("inferSchema", "true") \
            .option("dateFormat", "yyyy-MM-dd") \
            .csv(f"/tmp/sftp_staging/{self.config.get('run_date')}/*.csv")
        
        self.logger.info(f"Read {df.count()} records from SFTP")
        return df
    
    def read_from_kafka_streaming(self) -> DataFrame:
        """Read streaming data from Kafka (for JDBC lookup enrichment)"""
        kafka_config = self.config.get('kafka')
        
        df = self.spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", kafka_config['brokers']) \
            .option("subscribe", kafka_config['topic']) \
            .option("startingOffsets", "latest") \
            .option("kafka.security.protocol", "SASL_SSL") \
            .option("kafka.sasl.mechanism", "PLAIN") \
            .option("kafka.sasl.jaas.config", self.secret_manager.get_secret('kafka/jaas_config')) \
            .load()
        
        # Parse Kafka value
        schema = StructType([
            StructField("event_id", StringType(), True),
            StructField("customer_id", IntegerType(), True),
            StructField("event_type", StringType(), True),
            StructField("timestamp", LongType(), True)
        ])
        
        df_parsed = df.select(
            from_json(col("value").cast("string"), schema).alias("data")
        ).select("data.*")
        
        return df_parsed
    
    def enrich_with_jdbc_lookup(self, stream_df: DataFrame) -> DataFrame:
        """Enrich streaming data with JDBC lookups"""
        # Cache reference data from JDBC
        customer_lookup = self.read_from_oracle().select("customer_id", "customer_tier", "region")
        customer_lookup.createOrReplaceTempView("customer_lookup")
        
        # Join streaming data with cached lookup
        enriched_df = stream_df.join(
            broadcast(customer_lookup),
            on="customer_id",
            how="left"
        )
        
        return enriched_df
    
    def write_to_jdbc(self, df: DataFrame, table: str):
        """Write results back to PostgreSQL"""
        pg_config = self.config.get('postgres_target')
        password = self.secret_manager.get_secret('postgres/target/password')
        
        jdbc_url = f"jdbc:postgresql://{pg_config['host']}:{pg_config['port']}/{pg_config['database']}"
        
        df.write \
            .format("jdbc") \
            .option("url", jdbc_url) \
            .option("dbtable", f"analytics.{table}") \
            .option("user", pg_config['username']) \
            .option("password", password) \
            .option("driver", "org.postgresql.Driver") \
            .mode("overwrite") \
            .save()
        
        self.logger.info(f"Written {df.count()} records to PostgreSQL table: {table}")
    
    def run(self):
        """Execute the ETL pipeline"""
        self.logger.info("Starting Enterprise ETL Job...")
        
        # Read from multiple RDBMS sources
        df_customers = self.read_from_oracle()
        df_transactions = self.read_from_postgres()
        df_products = self.read_from_mysql()
        df_sales = self.read_from_mssql()
        df_external_data = self.read_from_sftp()
        
        # Apply transformations using custom modules
        transformer = DataQualityTransformer()
        df_customers_clean = transformer.validate_and_clean(df_customers)
        df_transactions_clean = transformer.validate_and_clean(df_transactions)
        
        # Join all sources
        df_enriched = df_transactions_clean \
            .join(df_customers_clean, "customer_id", "left") \
            .join(df_products, "product_id", "left") \
            .join(df_sales, ["customer_id", "product_id"], "left") \
            .join(df_external_data, "customer_id", "left")
        
        # Apply business logic
        business_transformer = BusinessLogicTransformer()
        df_final = business_transformer.apply_rules(df_enriched)
        
        # Write to multiple targets
        hdfs_writer = HDFSWriter(self.config.get('hdfs'))
        hdfs_writer.write_parquet(
            df_final,
            f"/data/warehouse/customer_360/{self.config.get('run_date')}"
        )
        
        hive_writer = HiveWriter(self.spark)
        hive_writer.write_to_table(
            df_final,
            "analytics.customer_360_daily",
            partition_by=["run_date"]
        )
        
        # Write aggregated results back to PostgreSQL
        df_summary = df_final.groupBy("region", "customer_tier").agg(
            count("*").alias("customer_count"),
            sum("total_transaction_amount").alias("total_revenue")
        )
        self.write_to_jdbc(df_summary, "customer_summary_by_region")
        
        self.logger.info("Enterprise ETL Job completed successfully!")


if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("EnterpriseETLJob") \
        .config("spark.jars.packages", 
                "org.postgresql:postgresql:42.5.1,"
                "mysql:mysql-connector-java:8.0.30,"
                "com.microsoft.sqlserver:mssql-jdbc:11.2.1.jre8,"
                "com.oracle.database.jdbc:ojdbc8:21.7.0.0") \
        .enableHiveSupport() \
        .getOrCreate()
    
    config_path = sys.argv[1] if len(sys.argv) > 1 else "/config/etl_config.yaml"
    
    job = EnterpriseETLJob(spark, config_path)
    job.run()
    
    spark.stop()


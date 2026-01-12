"""
PySpark job demonstrating advanced variable resolution:
- Function parameters with defaults
- F-strings with variable substitution
- Command-line arguments with defaults
"""
from pyspark.sql import SparkSession
import sys

def main(env="prod", run_date="2024-01-15"):
    """Main function with default parameters."""
    
    # Create Spark session
    spark = SparkSession.builder \
        .appName("AdvancedResolutionDemo") \
        .enableHiveSupport() \
        .getOrCreate()
    
    # F-string using function parameter
    base_path = f"/data/{env}"
    warehouse_path = f"{base_path}/warehouse"
    
    # Another f-string with function parameter
    input_path = f"{base_path}/raw/transactions/{run_date}"
    output_path = f"{base_path}/processed/transactions/{run_date}"
    
    # Command-line argument with default
    batch_id = sys.argv[1] if len(sys.argv) > 1 else "batch_001"
    checkpoint_path = f"{base_path}/checkpoints/{batch_id}"
    
    # Read data
    df_raw = spark.read.parquet(input_path)
    
    # Process data
    df_processed = df_raw.select("*")
    
    # Write data
    df_processed.write \
        .mode("overwrite") \
        .parquet(output_path)
    
    # Write to Hive table (with schema from f-string)
    schema_name = f"{env}_analytics"
    df_processed.write \
        .mode("overwrite") \
        .saveAsTable(f"{schema_name}.transaction_summary")
    
    spark.stop()

if __name__ == "__main__":
    # Extract env and run_date from command line args with defaults
    env_arg = sys.argv[2] if len(sys.argv) > 2 else "prod"
    date_arg = sys.argv[3] if len(sys.argv) > 3 else "2024-01-15"
    
    main(env=env_arg, run_date=date_arg)


#!/bin/bash
# Order Ingestion Script
# Called by master_etl_pipeline.sh with parameters from etl_env.sh

set -e

SOURCE_TABLE=""
TARGET_TABLE=""
BRONZE_PATH=""

while [[ $# -gt 0 ]]; do
    case $1 in
        --source-table)
            SOURCE_TABLE="$2"
            shift 2
            ;;
        --target-table)
            TARGET_TABLE="$2"
            shift 2
            ;;
        --bronze-path)
            BRONZE_PATH="$2"
            shift 2
            ;;
        *)
            echo "Unknown option: $1"
            exit 1
            ;;
    esac
done

echo "📥 Ingesting orders from ${SOURCE_TABLE} to ${TARGET_TABLE}"

# Create Bronze directory
hdfs dfs -mkdir -p "${BRONZE_PATH}"

# Copy from source using hdfs
hdfs dfs -cp "/source/${SOURCE_TABLE}/*" "${BRONZE_PATH}/"

# Load into Hive with schema-qualified name
hive -e "
    CREATE EXTERNAL TABLE IF NOT EXISTS ${TARGET_TABLE} (
        order_id BIGINT,
        customer_id BIGINT,
        order_date DATE,
        order_amount DECIMAL(10,2)
    )
    STORED AS PARQUET
    LOCATION '${BRONZE_PATH}';
    
    MSCK REPAIR TABLE ${TARGET_TABLE};
"

echo "✅ Order ingestion completed: ${SOURCE_TABLE} → ${TARGET_TABLE}"


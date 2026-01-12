#!/bin/bash
# Customer Ingestion Script
# Called by master_etl_pipeline.sh with schema-qualified table names

set -e

# Default values (can be overridden by command line args)
SOURCE_TABLE=""
TARGET_TABLE=""
BRONZE_PATH=""

# Parse command line arguments
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

# Validate inputs
if [ -z "$SOURCE_TABLE" ] || [ -z "$TARGET_TABLE" ]; then
    echo "❌ Error: Missing required arguments"
    echo "Usage: $0 --source-table <db.table> --target-table <db.table> --bronze-path <path>"
    exit 1
fi

echo "📥 Ingesting customers from ${SOURCE_TABLE} to ${TARGET_TABLE}"

# Step 1: Export from source to Bronze layer
echo "  → Exporting to Bronze: ${BRONZE_PATH}"
hdfs dfs -mkdir -p "${BRONZE_PATH}"

# Use Sqoop to extract from source (schema.table)
sqoop import \
    --connect jdbc:oracle:thin:@prod-db:1521:ORCL \
    --username etl_user \
    --table "${SOURCE_TABLE}" \
    --target-dir "${BRONZE_PATH}/raw" \
    --as-parquetfile \
    --delete-target-dir

# Step 2: Load to Hive staging table (schema.table)
echo "  → Loading to Hive: ${TARGET_TABLE}"
beeline -u "jdbc:hive2://hive-server:10000/default" -e "
    CREATE TABLE IF NOT EXISTS ${TARGET_TABLE} (
        customer_id BIGINT,
        customer_name STRING,
        email STRING,
        signup_date DATE
    )
    STORED AS PARQUET;

    LOAD DATA INPATH '${BRONZE_PATH}/raw' 
    OVERWRITE INTO TABLE ${TARGET_TABLE};
"

echo "✅ Customer ingestion completed: ${SOURCE_TABLE} → ${TARGET_TABLE}"


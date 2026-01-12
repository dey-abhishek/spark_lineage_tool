#!/bin/bash
# Master ETL Pipeline Script
# Sources config and calls multiple sub-scripts with schema-qualified table names

set -e

# Get script directory
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CONFIG_DIR="${SCRIPT_DIR}/../configs"

# Source environment configuration
echo "Loading environment configuration..."
source "${CONFIG_DIR}/etl_env.sh"

# Verify required variables
if [ -z "$SOURCE_DB" ] || [ -z "$TARGET_DB" ]; then
    echo "❌ Error: Required environment variables not set"
    exit 1
fi

echo "========================================="
echo "Starting ETL Pipeline"
echo "Source DB: ${SOURCE_DB}"
echo "Target DB: ${TARGET_DB}"
echo "========================================="

# Step 1: Run customer ingestion
echo "Step 1: Customer Ingestion..."
bash "${SCRIPT_DIR}/ingest_customers.sh" \
    --source-table "${SOURCE_CUSTOMERS_TABLE}" \
    --target-table "${STAGING_CUSTOMERS}" \
    --bronze-path "${BRONZE_PATH}/customers"

# Step 2: Run order ingestion
echo "Step 2: Order Ingestion..."
bash "${SCRIPT_DIR}/ingest_orders.sh" \
    --source-table "${SOURCE_ORDERS_TABLE}" \
    --target-table "${STAGING_ORDERS}" \
    --bronze-path "${BRONZE_PATH}/orders"

# Step 3: Run aggregation with schema-qualified tables
echo "Step 3: Customer Aggregation..."
spark-submit \
    --master ${SPARK_MASTER} \
    --deploy-mode ${SPARK_DEPLOY_MODE} \
    --conf spark.sql.warehouse.dir=/user/hive/warehouse \
    "${SCRIPT_DIR}/../pyspark/customer_aggregation.py" \
    --source-customers "${STAGING_CUSTOMERS}" \
    --source-orders "${STAGING_ORDERS}" \
    --target-table "${TARGET_CUSTOMER_SUMMARY}"

# Step 4: Run product analysis
echo "Step 4: Product Performance Analysis..."
beeline -u "jdbc:hive2://${HIVE_SERVER}/${HIVE_DB}" \
    --hivevar source_products="${SOURCE_PRODUCTS_TABLE}" \
    --hivevar source_orders="${SOURCE_ORDERS_TABLE}" \
    --hivevar target_table="${TARGET_PRODUCT_PERFORMANCE}" \
    -f "${SCRIPT_DIR}/../hive/product_analysis.hql"

# Step 5: Generate monthly metrics
echo "Step 5: Monthly Metrics..."
spark-submit \
    --master ${SPARK_MASTER} \
    "${SCRIPT_DIR}/../pyspark/monthly_metrics.py" \
    --input-summary "${TARGET_CUSTOMER_SUMMARY}" \
    --input-products "${TARGET_PRODUCT_PERFORMANCE}" \
    --output-table "${TARGET_MONTHLY_METRICS}"

echo "========================================="
echo "✅ ETL Pipeline Completed Successfully"
echo "========================================="


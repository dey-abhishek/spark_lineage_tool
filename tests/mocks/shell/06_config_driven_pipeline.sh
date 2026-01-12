#!/bin/bash
##############################################################################
# Config-Driven ETL Orchestration Script
# Reads configuration from YAML and launches Spark/Hive jobs with parameters
##############################################################################

set -euo pipefail

# Script directory and config location
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CONFIG_FILE="${1:-${SCRIPT_DIR}/../configs/etl_pipeline_config.yaml}"
RUN_DATE="${2:-$(date +%Y-%m-%d)}"
ENV="${3:-prod}"

# Logging
LOG_DIR="/var/log/etl"
LOG_FILE="${LOG_DIR}/etl_pipeline_${RUN_DATE}.log"
mkdir -p "${LOG_DIR}"
exec 1> >(tee -a "${LOG_FILE}")
exec 2>&1

echo "[$(date)] Starting Config-Driven ETL Pipeline"
echo "[$(date)] Config: ${CONFIG_FILE}"
echo "[$(date)] Run Date: ${RUN_DATE}"
echo "[$(date)] Environment: ${ENV}"

##############################################################################
# Function: Parse YAML config (simplified - using yq or python)
##############################################################################

get_config_value() {
    local key="$1"
    # Using yq (YAML query tool) or fall back to grep/sed
    if command -v yq &> /dev/null; then
        yq eval ".${key}" "${CONFIG_FILE}" | envsubst
    else
        # Simple grep-based extraction (not robust for complex YAML)
        grep -A 1 "^  ${key}:" "${CONFIG_FILE}" | tail -1 | sed 's/.*: //' | envsubst
    fi
}

##############################################################################
# Parse Input/Output Paths from Config
##############################################################################

echo "[$(date)] Parsing configuration..."

# Input paths
INPUT_TRANSACTIONS=$(yq eval '.inputs.transactions.path' "${CONFIG_FILE}" | envsubst)
INPUT_CUSTOMERS=$(yq eval '.inputs.customers.path' "${CONFIG_FILE}" | envsubst)
INPUT_PRODUCTS=$(yq eval '.inputs.products.path' "${CONFIG_FILE}" | envsubst)
INPUT_EVENTS=$(yq eval '.inputs.events.path' "${CONFIG_FILE}" | envsubst)

# Hive inputs
HIVE_ORDERS_DB=$(yq eval '.inputs.hive_orders.database' "${CONFIG_FILE}" | envsubst)
HIVE_ORDERS_TABLE=$(yq eval '.inputs.hive_orders.table' "${CONFIG_FILE}" | envsubst)
HIVE_USERS_DB=$(yq eval '.inputs.hive_users.database' "${CONFIG_FILE}" | envsubst)
HIVE_USERS_TABLE=$(yq eval '.inputs.hive_users.table' "${CONFIG_FILE}" | envsubst)

# Output paths
OUTPUT_ENRICHED=$(yq eval '.outputs.enriched_transactions.path' "${CONFIG_FILE}" | envsubst)
OUTPUT_SUMMARY=$(yq eval '.outputs.customer_summary.path' "${CONFIG_FILE}" | envsubst)
OUTPUT_FRAUD=$(yq eval '.outputs.fraud_alerts.path' "${CONFIG_FILE}" | envsubst)

# Hive outputs
HIVE_ANALYTICS_DB=$(yq eval '.outputs.hive_analytics.database' "${CONFIG_FILE}" | envsubst)
HIVE_ANALYTICS_TABLE=$(yq eval '.outputs.hive_analytics.table' "${CONFIG_FILE}" | envsubst)
HIVE_REPORTS_DB=$(yq eval '.outputs.hive_reports.database' "${CONFIG_FILE}" | envsubst)
HIVE_REPORTS_TABLE=$(yq eval '.outputs.hive_reports.table' "${CONFIG_FILE}" | envsubst)

# Staging
STAGING_PATH=$(yq eval '.staging.base_path' "${CONFIG_FILE}" | envsubst)

# Spark config
EXECUTOR_MEMORY=$(yq eval '.spark_config.executor_memory' "${CONFIG_FILE}")
EXECUTOR_CORES=$(yq eval '.spark_config.executor_cores' "${CONFIG_FILE}")
NUM_EXECUTORS=$(yq eval '.spark_config.num_executors' "${CONFIG_FILE}")
DRIVER_MEMORY=$(yq eval '.spark_config.driver_memory' "${CONFIG_FILE}")

echo "[$(date)] Configuration parsed successfully"
echo "  Inputs:"
echo "    Transactions: ${INPUT_TRANSACTIONS}"
echo "    Customers: ${INPUT_CUSTOMERS}"
echo "    Products: ${INPUT_PRODUCTS}"
echo "    Hive Orders: ${HIVE_ORDERS_DB}.${HIVE_ORDERS_TABLE}"
echo "  Outputs:"
echo "    Enriched: ${OUTPUT_ENRICHED}"
echo "    Summary: ${OUTPUT_SUMMARY}"
echo "    Hive Analytics: ${HIVE_ANALYTICS_DB}.${HIVE_ANALYTICS_TABLE}"

##############################################################################
# Stage 1: Data Validation (check if inputs exist)
##############################################################################

echo "[$(date)] Stage 1: Validating input data..."

hdfs dfs -test -d "${INPUT_TRANSACTIONS}" || { echo "ERROR: ${INPUT_TRANSACTIONS} not found"; exit 1; }
hdfs dfs -test -d "${INPUT_CUSTOMERS}" || { echo "ERROR: ${INPUT_CUSTOMERS} not found"; exit 1; }
hdfs dfs -test -d "${INPUT_PRODUCTS}" || { echo "ERROR: ${INPUT_PRODUCTS} not found"; exit 1; }

echo "[$(date)] Input validation passed"

##############################################################################
# Stage 2: Launch Spark Job with Config Parameters
##############################################################################

echo "[$(date)] Stage 2: Launching Spark ETL job..."

spark-submit \
  --master yarn \
  --deploy-mode cluster \
  --name "CustomerAnalyticsETL_${RUN_DATE}" \
  --conf spark.executor.memory="${EXECUTOR_MEMORY}" \
  --conf spark.executor.cores="${EXECUTOR_CORES}" \
  --conf spark.executor.instances="${NUM_EXECUTORS}" \
  --conf spark.driver.memory="${DRIVER_MEMORY}" \
  --conf spark.sql.shuffle.partitions=200 \
  --conf spark.dynamicAllocation.enabled=true \
  --conf spark.sql.adaptive.enabled=true \
  --jars /opt/jars/postgresql-42.5.1.jar,/opt/jars/mysql-connector-8.0.30.jar \
  --py-files /opt/etl/dist/etl_framework.zip \
  /opt/etl/jobs/customer_analytics_etl.py \
  --run-date "${RUN_DATE}" \
  --env "${ENV}" \
  --input-transactions "${INPUT_TRANSACTIONS}" \
  --input-customers "${INPUT_CUSTOMERS}" \
  --input-products "${INPUT_PRODUCTS}" \
  --input-events "${INPUT_EVENTS}" \
  --hive-orders-table "${HIVE_ORDERS_DB}.${HIVE_ORDERS_TABLE}" \
  --hive-users-table "${HIVE_USERS_DB}.${HIVE_USERS_TABLE}" \
  --output-enriched "${OUTPUT_ENRICHED}" \
  --output-summary "${OUTPUT_SUMMARY}" \
  --output-fraud "${OUTPUT_FRAUD}" \
  --staging-path "${STAGING_PATH}"

SPARK_EXIT_CODE=$?

if [ ${SPARK_EXIT_CODE} -ne 0 ]; then
    echo "[$(date)] ERROR: Spark job failed with exit code ${SPARK_EXIT_CODE}"
    exit ${SPARK_EXIT_CODE}
fi

echo "[$(date)] Spark ETL job completed successfully"

##############################################################################
# Stage 3: Load Results to Hive using Beeline
##############################################################################

echo "[$(date)] Stage 3: Loading results to Hive..."

beeline -u "jdbc:hive2://hiveserver2:10000/default" \
  -n etl_user \
  --hiveconf run_date="${RUN_DATE}" \
  --hiveconf env="${ENV}" \
  --hiveconf output_enriched="${OUTPUT_ENRICHED}" \
  --hiveconf output_summary="${OUTPUT_SUMMARY}" \
  --hiveconf analytics_db="${HIVE_ANALYTICS_DB}" \
  --hiveconf analytics_table="${HIVE_ANALYTICS_TABLE}" \
  --hiveconf reports_db="${HIVE_REPORTS_DB}" \
  --hiveconf reports_table="${HIVE_REPORTS_TABLE}" \
  -e "
    -- Load enriched transactions to Hive
    INSERT OVERWRITE TABLE \${hiveconf:analytics_db}.\${hiveconf:analytics_table}
    PARTITION(run_date='\${hiveconf:run_date}')
    SELECT 
        transaction_id,
        customer_id,
        product_id,
        amount,
        transaction_date,
        region,
        customer_tier
    FROM parquet.\`\${hiveconf:output_enriched}\`
    WHERE run_date = '\${hiveconf:run_date}';
    
    -- Load summary to reports
    INSERT INTO TABLE \${hiveconf:reports_db}.\${hiveconf:reports_table}
    SELECT 
        customer_id,
        total_transactions,
        total_amount,
        avg_transaction_value,
        '\${hiveconf:run_date}' as report_date
    FROM orc.\`\${hiveconf:output_summary}\`;
"

echo "[$(date)] Hive load completed"

##############################################################################
# Stage 4: Run Hive Aggregations (using config parameters)
##############################################################################

echo "[$(date)] Stage 4: Running Hive aggregations..."

hive \
  --hiveconf run_date="${RUN_DATE}" \
  --hiveconf env="${ENV}" \
  --hiveconf analytics_db="${HIVE_ANALYTICS_DB}" \
  --hiveconf reports_db="${HIVE_REPORTS_DB}" \
  -f /opt/etl/hive/customer_aggregations.hql

echo "[$(date)] Hive aggregations completed"

##############################################################################
# Stage 5: Export Results to RDBMS (using config)
##############################################################################

echo "[$(date)] Stage 5: Exporting to PostgreSQL..."

# Read database config
DB_HOST=$(yq eval '.database.postgres.host' "${CONFIG_FILE}" || echo "postgres-prod.company.com")
DB_PORT=$(yq eval '.database.postgres.port' "${CONFIG_FILE}" || echo "5432")
DB_NAME=$(yq eval '.database.postgres.database' "${CONFIG_FILE}" || echo "analytics")

# Export summary to PostgreSQL
hive \
  --hiveconf run_date="${RUN_DATE}" \
  --hiveconf reports_db="${HIVE_REPORTS_DB}" \
  -e "
    SELECT 
        customer_id,
        total_transactions,
        total_amount,
        report_date
    FROM \${hiveconf:reports_db}.customer_360_view
    WHERE report_date = '\${hiveconf:run_date}'
" | psql -h "${DB_HOST}" -p "${DB_PORT}" -U etl_user -d "${DB_NAME}" -c "
    COPY reporting.customer_summary_staging FROM STDIN WITH CSV HEADER;
"

echo "[$(date)] PostgreSQL export completed"

##############################################################################
# Stage 6: Cleanup Staging (if configured)
##############################################################################

CLEANUP_STAGING=$(yq eval '.staging.cleanup_after_success' "${CONFIG_FILE}")

if [ "${CLEANUP_STAGING}" = "true" ]; then
    echo "[$(date)] Stage 6: Cleaning up staging area..."
    hdfs dfs -rm -r -skipTrash "${STAGING_PATH}" || true
    echo "[$(date)] Staging cleanup completed"
fi

##############################################################################
# Stage 7: Archive and Retention Management
##############################################################################

echo "[$(date)] Stage 7: Managing data retention..."

RETENTION_DAYS=$(yq eval '.staging.retention_days' "${CONFIG_FILE}")
CUTOFF_DATE=$(date -d "${RUN_DATE} - ${RETENTION_DAYS} days" +%Y-%m-%d)

# Archive old partitions
hdfs dfs -mv \
  "/data/processed/enriched_transactions/${CUTOFF_DATE}" \
  "/archive/enriched_transactions/${CUTOFF_DATE}" || true

echo "[$(date)] Retention management completed"

##############################################################################
# Final Status
##############################################################################

echo "[$(date)] ETL Pipeline completed successfully!"
echo ""
echo "Summary:"
echo "  Config: ${CONFIG_FILE}"
echo "  Run Date: ${RUN_DATE}"
echo "  Inputs Processed:"
echo "    - ${INPUT_TRANSACTIONS}"
echo "    - ${INPUT_CUSTOMERS}"
echo "    - ${INPUT_PRODUCTS}"
echo "    - ${HIVE_ORDERS_DB}.${HIVE_ORDERS_TABLE}"
echo "  Outputs Generated:"
echo "    - ${OUTPUT_ENRICHED}"
echo "    - ${OUTPUT_SUMMARY}"
echo "    - ${HIVE_ANALYTICS_DB}.${HIVE_ANALYTICS_TABLE}"
echo "    - ${HIVE_REPORTS_DB}.${HIVE_REPORTS_TABLE}"

exit 0


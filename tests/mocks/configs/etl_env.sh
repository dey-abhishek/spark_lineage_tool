# Environment Configuration for ETL Pipeline
# This file is sourced by shell scripts to provide table names and paths

# Database and schema names
export SOURCE_DB="prod_db"
export TARGET_DB="analytics_db"
export WAREHOUSE_DB="warehouse_db"
export REPORTING_DB="reporting_db"

# Source tables (schema.table format)
export SOURCE_CUSTOMERS_TABLE="${SOURCE_DB}.customers"
export SOURCE_ORDERS_TABLE="${SOURCE_DB}.orders"
export SOURCE_PRODUCTS_TABLE="catalog_db.products"

# Target tables (schema.table format)
export TARGET_CUSTOMER_SUMMARY="${TARGET_DB}.customer_summary"
export TARGET_PRODUCT_PERFORMANCE="${TARGET_DB}.product_performance"
export TARGET_ORDER_SUMMARY="${WAREHOUSE_DB}.order_summary"
export TARGET_MONTHLY_METRICS="${REPORTING_DB}.monthly_metrics"

# Intermediate tables
export STAGING_CUSTOMERS="${TARGET_DB}.staging_customers"
export STAGING_ORDERS="${TARGET_DB}.staging_orders"

# HDFS paths
export BRONZE_PATH="/data/bronze"
export SILVER_PATH="/data/silver"
export GOLD_PATH="/data/gold"

# Hive connection
export HIVE_SERVER="hive-server:10000"
export HIVE_DB="default"

# Spark configuration
export SPARK_MASTER="yarn"
export SPARK_DEPLOY_MODE="cluster"

echo "✅ Environment loaded: SOURCE_DB=${SOURCE_DB}, TARGET_DB=${TARGET_DB}"


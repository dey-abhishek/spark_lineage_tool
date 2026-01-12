#!/bin/bash
##############################################################################
# Complex Shell ETL Pipeline with SFTP, RDBMS exports, and multi-stage processing
# Demonstrates real-world data ingestion patterns
##############################################################################

set -euo pipefail

# Configuration
RUN_DATE=${1:-$(date +%Y-%m-%d)}
ENV=${2:-prod}
BASE_PATH="/data/${ENV}"
SFTP_HOST="sftp.vendor.com"
SFTP_USER="data_ingest"
SFTP_REMOTE_PATH="/exports/daily"
LOCAL_STAGING="/tmp/sftp_staging/${RUN_DATE}"
HDFS_STAGING="hdfs://namenode:8020/staging/external_data/${RUN_DATE}"
POSTGRES_HOST="postgres-${ENV}.company.com"
MYSQL_HOST="mysql-${ENV}.company.com"
ORACLE_HOST="oracle-${ENV}.company.com"

# Load credentials from vault
source /opt/scripts/vault_utils.sh
SFTP_KEY=$(get_secret "sftp/${ENV}/private_key")
POSTGRES_PASSWORD=$(get_secret "postgres/${ENV}/password")
MYSQL_PASSWORD=$(get_secret "mysql/${ENV}/password")
ORACLE_PASSWORD=$(get_secret "oracle/${ENV}/password")

# Logging
LOG_FILE="/var/log/etl/sftp_etl_${RUN_DATE}.log"
exec 1> >(tee -a "${LOG_FILE}")
exec 2>&1

echo "[$(date)] Starting SFTP ETL Pipeline for ${RUN_DATE}"

##############################################################################
# Stage 1: Download files from SFTP
##############################################################################

echo "[$(date)] Stage 1: Downloading files from SFTP..."

# Create staging directories
mkdir -p "${LOCAL_STAGING}"/{customers,transactions,products,logs}

# Download customer files via SFTP using key authentication
sftp -i "${SFTP_KEY}" -o StrictHostKeyChecking=no "${SFTP_USER}@${SFTP_HOST}" <<EOF
cd ${SFTP_REMOTE_PATH}/${RUN_DATE}/customers
lcd ${LOCAL_STAGING}/customers
mget *.csv
cd ../transactions
lcd ${LOCAL_STAGING}/transactions
mget *.parquet
cd ../products
lcd ${LOCAL_STAGING}/products
mget *.json
cd ../logs
lcd ${LOCAL_STAGING}/logs
mget *.gz
bye
EOF

# Alternative: Use rsync over SSH for incremental sync
# rsync -avz -e "ssh -i ${SFTP_KEY}" \
#   "${SFTP_USER}@${SFTP_HOST}:${SFTP_REMOTE_PATH}/${RUN_DATE}/" \
#   "${LOCAL_STAGING}/"

# Alternative: Use SCP for specific files
scp -i "${SFTP_KEY}" -r \
  "${SFTP_USER}@${SFTP_HOST}:${SFTP_REMOTE_PATH}/${RUN_DATE}/manifest.txt" \
  "${LOCAL_STAGING}/"

echo "[$(date)] Downloaded $(find ${LOCAL_STAGING} -type f | wc -l) files from SFTP"

##############################################################################
# Stage 2: Export data from RDBMS sources
##############################################################################

echo "[$(date)] Stage 2: Exporting data from RDBMS sources..."

# Export from PostgreSQL using COPY command
psql -h "${POSTGRES_HOST}" -U etl_user -d analytics <<EOSQL
\o ${LOCAL_STAGING}/postgres_export.csv
COPY (
  SELECT 
    customer_id,
    customer_name,
    email,
    registration_date,
    customer_tier
  FROM customers.master
  WHERE updated_date >= '${RUN_DATE}'::date
) TO STDOUT WITH CSV HEADER;
EOSQL

echo "[$(date)] Exported $(wc -l < ${LOCAL_STAGING}/postgres_export.csv) rows from PostgreSQL"

# Export from MySQL using mysqldump
mysqldump -h "${MYSQL_HOST}" \
  -u etl_user \
  -p"${MYSQL_PASSWORD}" \
  --single-transaction \
  --quick \
  --where="order_date >= '${RUN_DATE}'" \
  ecommerce orders \
  > "${LOCAL_STAGING}/mysql_orders_dump.sql"

# Alternative: MySQL into outfile
mysql -h "${MYSQL_HOST}" -u etl_user -p"${MYSQL_PASSWORD}" -e "
  SELECT 
    order_id,
    customer_id,
    product_id,
    order_amount,
    order_date
  INTO OUTFILE '${LOCAL_STAGING}/mysql_orders.csv'
  FIELDS TERMINATED BY ',' 
  ENCLOSED BY '\"'
  LINES TERMINATED BY '\n'
  FROM ecommerce.orders
  WHERE order_date >= '${RUN_DATE}';
"

echo "[$(date)] Exported MySQL orders"

# Export from Oracle using SQL*Plus
sqlplus -S etl_user/"${ORACLE_PASSWORD}"@"${ORACLE_HOST}":1521/ORCL <<EOSQL
SET ECHO OFF
SET FEEDBACK OFF
SET PAGESIZE 0
SET COLSEP ','
SET LINESIZE 32767
SET TRIMSPOOL ON
SPOOL ${LOCAL_STAGING}/oracle_transactions.csv
SELECT 
  transaction_id,
  account_id,
  transaction_amount,
  transaction_date,
  merchant_id
FROM transactions.fact_transactions
WHERE TRUNC(transaction_date) = TO_DATE('${RUN_DATE}', 'YYYY-MM-DD');
SPOOL OFF
EXIT;
EOSQL

echo "[$(date)] Exported $(wc -l < ${LOCAL_STAGING}/oracle_transactions.csv) rows from Oracle"

# Alternative: Oracle Data Pump export
expdp etl_user/"${ORACLE_PASSWORD}"@"${ORACLE_HOST}":1521/ORCL \
  directory=DATA_PUMP_DIR \
  dumpfile=transactions_${RUN_DATE}.dmp \
  logfile=export_${RUN_DATE}.log \
  tables=transactions.fact_transactions \
  query="transactions.fact_transactions:\"WHERE TRUNC(transaction_date) = TO_DATE('${RUN_DATE}', 'YYYY-MM-DD')\""

##############################################################################
# Stage 3: Upload to HDFS staging
##############################################################################

echo "[$(date)] Stage 3: Uploading files to HDFS staging..."

# Create HDFS directories
hdfs dfs -mkdir -p "${HDFS_STAGING}"/{customers,transactions,products,postgres,mysql,oracle}

# Upload SFTP files to HDFS
hdfs dfs -put -f "${LOCAL_STAGING}/customers/*.csv" "${HDFS_STAGING}/customers/"
hdfs dfs -put -f "${LOCAL_STAGING}/transactions/*.parquet" "${HDFS_STAGING}/transactions/"
hdfs dfs -put -f "${LOCAL_STAGING}/products/*.json" "${HDFS_STAGING}/products/"

# Upload RDBMS exports to HDFS
hdfs dfs -put -f "${LOCAL_STAGING}/postgres_export.csv" "${HDFS_STAGING}/postgres/"
hdfs dfs -put -f "${LOCAL_STAGING}/mysql_orders.csv" "${HDFS_STAGING}/mysql/"
hdfs dfs -put -f "${LOCAL_STAGING}/oracle_transactions.csv" "${HDFS_STAGING}/oracle/"

# Verify upload
HDFS_FILE_COUNT=$(hdfs dfs -ls -R "${HDFS_STAGING}" | grep "^-" | wc -l)
echo "[$(date)] Uploaded ${HDFS_FILE_COUNT} files to HDFS"

##############################################################################
# Stage 4: Process with Spark
##############################################################################

echo "[$(date)] Stage 4: Processing data with Spark..."

# Submit Spark job to process all sources
spark-submit \
  --master yarn \
  --deploy-mode cluster \
  --num-executors 50 \
  --executor-cores 4 \
  --executor-memory 8G \
  --driver-memory 4G \
  --conf spark.sql.shuffle.partitions=200 \
  --conf spark.dynamicAllocation.enabled=true \
  --conf spark.sql.adaptive.enabled=true \
  --jars /opt/jars/postgresql-42.5.1.jar,/opt/jars/mysql-connector-8.0.30.jar,/opt/jars/ojdbc8-21.7.0.0.jar \
  --py-files /opt/etl/dist/etl_framework.zip \
  /opt/etl/jobs/unified_data_processor.py \
  --run-date "${RUN_DATE}" \
  --env "${ENV}" \
  --hdfs-staging "${HDFS_STAGING}" \
  --output-path "${BASE_PATH}/processed/${RUN_DATE}"

SPARK_EXIT_CODE=$?

if [ ${SPARK_EXIT_CODE} -ne 0 ]; then
  echo "[$(date)] ERROR: Spark job failed with exit code ${SPARK_EXIT_CODE}"
  exit ${SPARK_EXIT_CODE}
fi

echo "[$(date)] Spark processing completed successfully"

##############################################################################
# Stage 5: Load processed data to Hive
##############################################################################

echo "[$(date)] Stage 5: Loading processed data to Hive..."

# Create Hive tables and load data
beeline -u "jdbc:hive2://hiveserver2:10000/default" \
  -n etl_user \
  --hiveconf run_date="${RUN_DATE}" \
  --hiveconf base_path="${BASE_PATH}" \
  -f /opt/etl/sql/load_enriched_data.hql

# Alternative: Direct LOAD DATA INPATH
hive -e "
  LOAD DATA INPATH '${BASE_PATH}/processed/${RUN_DATE}/customers/' 
  OVERWRITE INTO TABLE analytics.enriched_customers 
  PARTITION(load_date='${RUN_DATE}');
  
  LOAD DATA INPATH '${BASE_PATH}/processed/${RUN_DATE}/transactions/' 
  OVERWRITE INTO TABLE analytics.enriched_transactions 
  PARTITION(load_date='${RUN_DATE}');
"

echo "[$(date)] Data loaded to Hive tables"

##############################################################################
# Stage 6: Export results back to RDBMS for reporting
##############################################################################

echo "[$(date)] Stage 6: Exporting aggregated results to RDBMS..."

# Export summary data from Hive to PostgreSQL
hive -e "
  SELECT 
    customer_id,
    total_transactions,
    total_amount,
    last_transaction_date
  FROM analytics.customer_daily_summary
  WHERE report_date = '${RUN_DATE}'
" | psql -h "${POSTGRES_HOST}" -U etl_user -d reporting -c "
  COPY reporting.customer_summary_staging FROM STDIN WITH CSV;
"

# Merge into final table
psql -h "${POSTGRES_HOST}" -U etl_user -d reporting <<EOSQL
MERGE INTO reporting.customer_summary t
USING reporting.customer_summary_staging s
ON t.customer_id = s.customer_id AND t.report_date = '${RUN_DATE}'
WHEN MATCHED THEN
  UPDATE SET 
    total_transactions = s.total_transactions,
    total_amount = s.total_amount,
    last_transaction_date = s.last_transaction_date
WHEN NOT MATCHED THEN
  INSERT (customer_id, report_date, total_transactions, total_amount, last_transaction_date)
  VALUES (s.customer_id, '${RUN_DATE}', s.total_transactions, s.total_amount, s.last_transaction_date);

TRUNCATE TABLE reporting.customer_summary_staging;
EOSQL

echo "[$(date)] Exported results to PostgreSQL"

##############################################################################
# Stage 7: Upload summary files back to SFTP
##############################################################################

echo "[$(date)] Stage 7: Uploading summary files back to SFTP..."

# Create summary CSVs from Hive
hdfs dfs -cat "${BASE_PATH}/reports/${RUN_DATE}/customer_summary/*" \
  > "${LOCAL_STAGING}/customer_summary_${RUN_DATE}.csv"

# Upload to SFTP
sftp -i "${SFTP_KEY}" -o StrictHostKeyChecking=no "${SFTP_USER}@${SFTP_HOST}" <<EOF
cd /reports/${RUN_DATE}
put ${LOCAL_STAGING}/customer_summary_${RUN_DATE}.csv
bye
EOF

echo "[$(date)] Uploaded summary files to SFTP"

##############################################################################
# Stage 8: Cleanup and archival
##############################################################################

echo "[$(date)] Stage 8: Cleanup and archival..."

# Archive local files
tar -czf "/archive/sftp_staging_${RUN_DATE}.tar.gz" "${LOCAL_STAGING}"
rm -rf "${LOCAL_STAGING}"

# Archive HDFS staging (move to cold storage after 30 days)
ARCHIVE_DATE=$(date -d "${RUN_DATE} - 30 days" +%Y-%m-%d)
if hdfs dfs -test -d "${HDFS_STAGING}/../${ARCHIVE_DATE}"; then
  hdfs dfs -mv "${HDFS_STAGING}/../${ARCHIVE_DATE}" "hdfs://namenode:8020/archive/staging/${ARCHIVE_DATE}"
  echo "[$(date)] Archived HDFS staging for ${ARCHIVE_DATE}"
fi

# Drop old Hive partitions (beyond retention period)
RETENTION_DAYS=90
DROP_DATE=$(date -d "${RUN_DATE} - ${RETENTION_DAYS} days" +%Y-%m-%d)
hive -e "
  ALTER TABLE analytics.enriched_customers DROP IF EXISTS PARTITION(load_date='${DROP_DATE}');
  ALTER TABLE analytics.enriched_transactions DROP IF EXISTS PARTITION(load_date='${DROP_DATE}');
"

echo "[$(date)] Cleanup completed"
echo "[$(date)] SFTP ETL Pipeline completed successfully for ${RUN_DATE}"

exit 0


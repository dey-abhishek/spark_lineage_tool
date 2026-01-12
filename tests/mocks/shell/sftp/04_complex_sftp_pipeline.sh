#!/bin/bash
################################################################################
# Complex SFTP Pipeline with Multiple Hosts
# Demonstrates multi-source SFTP data ingestion with transformations
################################################################################

set -e

# Configuration
VENDOR1_HOST="vendor1.sftp.com"
VENDOR2_HOST="vendor2.sftp.com"
VENDOR3_HOST="vendor3.sftp.com"
SFTP_USER="data_ingest"
LOCAL_STAGING="/data/staging/sftp_ingest"
HDFS_LANDING="/data/landing/external"
RUN_DATE=$(date +%Y-%m-%d)
PREV_DATE=$(date -d "${RUN_DATE} - 1 day" +%Y-%m-%d)

# Create staging directories
mkdir -p "${LOCAL_STAGING}/vendor1"
mkdir -p "${LOCAL_STAGING}/vendor2"
mkdir -p "${LOCAL_STAGING}/vendor3"

echo "=== Step 1: Download from Vendor 1 - Sales Data ==="
sftp ${SFTP_USER}@${VENDOR1_HOST} <<EOF
cd /exports/sales/${RUN_DATE}
lcd ${LOCAL_STAGING}/vendor1
mget sales_*.csv
mget transactions_*.parquet
bye
EOF

echo "=== Step 2: Download from Vendor 2 - Customer Data ==="
sftp ${SFTP_USER}@${VENDOR2_HOST} <<EOF
cd /feeds/customers
lcd ${LOCAL_STAGING}/vendor2
get customer_master_${RUN_DATE}.json
get customer_delta_${PREV_DATE}_${RUN_DATE}.json
bye
EOF

echo "=== Step 3: Download from Vendor 3 - Inventory Data ==="
sftp ${SFTP_USER}@${VENDOR3_HOST} <<EOF
cd /data/inventory/daily
lcd ${LOCAL_STAGING}/vendor3
mget inventory_${RUN_DATE}_*.csv
get stock_levels_${RUN_DATE}.txt
bye
EOF

echo "=== Step 4: Upload to HDFS ==="
hdfs dfs -mkdir -p ${HDFS_LANDING}/vendor1/${RUN_DATE}
hdfs dfs -mkdir -p ${HDFS_LANDING}/vendor2/${RUN_DATE}
hdfs dfs -mkdir -p ${HDFS_LANDING}/vendor3/${RUN_DATE}

hdfs dfs -put ${LOCAL_STAGING}/vendor1/*.csv ${HDFS_LANDING}/vendor1/${RUN_DATE}/
hdfs dfs -put ${LOCAL_STAGING}/vendor1/*.parquet ${HDFS_LANDING}/vendor1/${RUN_DATE}/
hdfs dfs -put ${LOCAL_STAGING}/vendor2/*.json ${HDFS_LANDING}/vendor2/${RUN_DATE}/
hdfs dfs -put ${LOCAL_STAGING}/vendor3/*.csv ${HDFS_LANDING}/vendor3/${RUN_DATE}/
hdfs dfs -put ${LOCAL_STAGING}/vendor3/*.txt ${HDFS_LANDING}/vendor3/${RUN_DATE}/

echo "=== Step 5: Cleanup local staging ==="
rm -rf ${LOCAL_STAGING}/*

echo "SFTP pipeline completed successfully"


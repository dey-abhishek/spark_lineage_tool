#!/bin/bash
################################################################################
# Pipeline Stage 1: SFTP Data Ingestion
# Downloads raw data from multiple SFTP vendors to HDFS landing zone
################################################################################

set -e

# Configuration
RUN_DATE=${1:-$(date +%Y-%m-%d)}
ENV=${2:-prod}
LANDING_ZONE="/data/landing/${ENV}/${RUN_DATE}"

echo "Starting SFTP ingestion for date: ${RUN_DATE}"

# Create landing zone
hdfs dfs -mkdir -p ${LANDING_ZONE}/vendor1
hdfs dfs -mkdir -p ${LANDING_ZONE}/vendor2
hdfs dfs -mkdir -p ${LANDING_ZONE}/vendor3

# Vendor 1: Customer data (CSV)
echo "Ingesting customer data from Vendor 1..."
sftp -i /keys/vendor1.pem vendor1@sftp.vendor1.com << EOF
cd /exports/customers
get customer_master_${RUN_DATE}.csv /tmp/customer_master.csv
get customer_transactions_${RUN_DATE}.csv /tmp/customer_transactions.csv
bye
EOF

hdfs dfs -put -f /tmp/customer_master.csv ${LANDING_ZONE}/vendor1/
hdfs dfs -put -f /tmp/customer_transactions.csv ${LANDING_ZONE}/vendor1/

# Vendor 2: Product data (JSON)
echo "Ingesting product data from Vendor 2..."
sftp vendor2@sftp.vendor2.com << EOF
cd /feeds/products
mget product_catalog_${RUN_DATE}*.json /tmp/
bye
EOF

hdfs dfs -put -f /tmp/product_catalog_${RUN_DATE}*.json ${LANDING_ZONE}/vendor2/

# Vendor 3: Inventory data (Parquet)
echo "Ingesting inventory data from Vendor 3..."
scp -i /keys/vendor3.pem \
    vendor3@sftp.vendor3.com:/inventory/stock_${RUN_DATE}.parquet \
    /tmp/inventory.parquet

hdfs dfs -put -f /tmp/inventory.parquet ${LANDING_ZONE}/vendor3/

# Cleanup temp files
rm -f /tmp/customer_master.csv /tmp/customer_transactions.csv
rm -f /tmp/product_catalog_*.json /tmp/inventory.parquet

echo "SFTP ingestion completed successfully!"
echo "Data landed in: ${LANDING_ZONE}"


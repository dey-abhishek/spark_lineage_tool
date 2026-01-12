#!/bin/bash
################################################################################
# Pipeline Stage 5: Export Results back to SFTP
# Exports analytics results to partner SFTP servers
################################################################################

set -e

# Configuration
RUN_DATE=${1:-$(date +%Y-%m-%d)}
ENV=${2:-prod}
EXPORT_DIR="/data/exports/${ENV}"

echo "Starting SFTP export for date: ${RUN_DATE}"

# Download exports from HDFS to local
echo "Downloading exports from HDFS..."
hdfs dfs -get ${EXPORT_DIR}/top_customers/${RUN_DATE}/*.csv /tmp/top_customers_${RUN_DATE}.csv
hdfs dfs -get ${EXPORT_DIR}/product_summary/${RUN_DATE}/*.csv /tmp/product_summary_${RUN_DATE}.csv

# Export to Partner 1: Top customers
echo "Exporting top customers to Partner 1..."
sftp -i /keys/partner1.pem partner1@sftp.partner1.com << EOF
cd /incoming/customers
put /tmp/top_customers_${RUN_DATE}.csv
bye
EOF

# Export to Partner 2: Product summary
echo "Exporting product summary to Partner 2..."
sftp partner2@sftp.partner2.com << EOF
cd /reports/products
put /tmp/product_summary_${RUN_DATE}.csv
bye
EOF

# Archive exports
echo "Archiving exports..."
hdfs dfs -mkdir -p /data/archive/${ENV}/${RUN_DATE}
hdfs dfs -cp ${EXPORT_DIR}/top_customers/${RUN_DATE} /data/archive/${ENV}/${RUN_DATE}/top_customers
hdfs dfs -cp ${EXPORT_DIR}/product_summary/${RUN_DATE} /data/archive/${ENV}/${RUN_DATE}/product_summary

# Cleanup temp files
rm -f /tmp/top_customers_${RUN_DATE}.csv /tmp/product_summary_${RUN_DATE}.csv

echo "SFTP export completed successfully!"


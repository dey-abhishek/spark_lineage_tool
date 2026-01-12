#!/bin/bash
################################################################################
# Basic SFTP GET Operations
# Demonstrates simple file downloads from SFTP server
################################################################################

set -e

# Configuration
SFTP_HOST="sftp.vendor.com"
SFTP_USER="data_reader"
SFTP_PORT="22"
REMOTE_DIR="/exports/daily/sales"
LOCAL_DIR="/data/incoming/sales"
RUN_DATE=$(date +%Y-%m-%d)

# Create local directory
mkdir -p "${LOCAL_DIR}"

# Basic SFTP get - single file
sftp -P ${SFTP_PORT} ${SFTP_USER}@${SFTP_HOST} <<EOF
cd ${REMOTE_DIR}
get sales_${RUN_DATE}.csv ${LOCAL_DIR}/
bye
EOF

# SFTP get with wildcard - multiple files
sftp ${SFTP_USER}@${SFTP_HOST} <<EOF
cd ${REMOTE_DIR}
mget transactions_*.csv
bye
EOF

# SFTP get with specific pattern
sftp ${SFTP_USER}@${SFTP_HOST} <<EOF
cd /exports/daily/inventory
get inventory_snapshot_${RUN_DATE}.json ${LOCAL_DIR}/inventory.json
bye
EOF

echo "SFTP download completed successfully"


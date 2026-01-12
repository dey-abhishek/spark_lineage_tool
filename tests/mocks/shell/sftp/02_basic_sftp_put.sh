#!/bin/bash
################################################################################
# Basic SFTP PUT Operations
# Demonstrates simple file uploads to SFTP server
################################################################################

set -e

# Configuration
SFTP_HOST="partner.sftp.com"
SFTP_USER="data_sender"
REMOTE_DIR="/incoming/processed"
LOCAL_DIR="/data/exports/processed"
RUN_DATE=$(date +%Y-%m-%d)

# SFTP put - single file upload
sftp ${SFTP_USER}@${SFTP_HOST} <<EOF
cd ${REMOTE_DIR}
put ${LOCAL_DIR}/customer_report_${RUN_DATE}.csv
bye
EOF

# SFTP put - multiple files with pattern
sftp ${SFTP_USER}@${SFTP_HOST} <<EOF
cd ${REMOTE_DIR}/reports
mput ${LOCAL_DIR}/daily_*.csv
bye
EOF

# SFTP put with rename
sftp ${SFTP_USER}@${SFTP_HOST} <<EOF
cd ${REMOTE_DIR}
put ${LOCAL_DIR}/summary.csv summary_${RUN_DATE}.csv
bye
EOF

echo "SFTP upload completed successfully"


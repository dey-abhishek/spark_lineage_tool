#!/bin/bash
################################################################################
# Bidirectional SFTP Data Exchange
# Demonstrates both download and upload in a single script
################################################################################

set -e

# Configuration
PARTNER_HOST="partner.exchange.com"
SFTP_USER="data_exchange"
REMOTE_INBOX="/inbox/from_partner"
REMOTE_OUTBOX="/outbox/to_partner"
LOCAL_INBOX="/data/partner_data/inbox"
LOCAL_OUTBOX="/data/partner_data/outbox"
RUN_DATE=$(date +%Y-%m-%d)
BATCH_ID=$(date +%s)

# Create local directories
mkdir -p "${LOCAL_INBOX}"
mkdir -p "${LOCAL_OUTBOX}"

echo "=== Step 1: Download incoming data from partner ==="
sftp ${SFTP_USER}@${PARTNER_HOST} <<EOF
cd ${REMOTE_INBOX}
lcd ${LOCAL_INBOX}
mget order_updates_*.csv
mget inventory_feed_*.json
mget customer_changes_*.xml
bye
EOF

echo "=== Step 2: Process files (placeholder) ==="
# Processing would happen here
sleep 1

echo "=== Step 3: Upload processed results to partner ==="
sftp ${SFTP_USER}@${PARTNER_HOST} <<EOF
cd ${REMOTE_OUTBOX}
lcd ${LOCAL_OUTBOX}
put processed_orders_${RUN_DATE}.csv
put order_confirmations_${BATCH_ID}.json
put shipping_updates_${RUN_DATE}.xml
bye
EOF

echo "=== Step 4: Archive downloaded files ==="
ARCHIVE_DIR="${LOCAL_INBOX}/archive/${RUN_DATE}"
mkdir -p "${ARCHIVE_DIR}"
mv ${LOCAL_INBOX}/*.csv ${ARCHIVE_DIR}/
mv ${LOCAL_INBOX}/*.json ${ARCHIVE_DIR}/
mv ${LOCAL_INBOX}/*.xml ${ARCHIVE_DIR}/

echo "Bidirectional SFTP exchange completed"


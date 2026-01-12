#!/bin/bash
################################################################################
# SFTP with Error Handling and Retries
# Demonstrates robust SFTP operations with error handling
################################################################################

set -e

# Configuration
SFTP_HOST="sftp.vendor.com"
SFTP_USER="data_ingest"
REMOTE_DIR="/exports/daily"
LOCAL_DIR="/data/incoming"
RUN_DATE=$(date +%Y-%m-%d)
MAX_RETRIES=3
RETRY_DELAY=60

# Function to download with retry
download_with_retry() {
    local remote_file=$1
    local local_path=$2
    local attempt=1
    
    while [ $attempt -le $MAX_RETRIES ]; do
        echo "Download attempt $attempt of $MAX_RETRIES for ${remote_file}"
        
        if sftp ${SFTP_USER}@${SFTP_HOST} <<EOF
cd ${REMOTE_DIR}
get ${remote_file} ${local_path}/
bye
EOF
        then
            echo "Download successful: ${remote_file}"
            return 0
        else
            echo "Download failed, waiting ${RETRY_DELAY} seconds..."
            sleep $RETRY_DELAY
            attempt=$((attempt + 1))
        fi
    done
    
    echo "ERROR: Failed to download ${remote_file} after ${MAX_RETRIES} attempts"
    return 1
}

# Create local directory
mkdir -p "${LOCAL_DIR}"

# Download critical files with retry
download_with_retry "sales_${RUN_DATE}.csv" "${LOCAL_DIR}"
download_with_retry "transactions_${RUN_DATE}.parquet" "${LOCAL_DIR}"
download_with_retry "customers_${RUN_DATE}.json" "${LOCAL_DIR}"

# Verify downloads
for file in sales transactions customers; do
    if [ ! -f "${LOCAL_DIR}/${file}_${RUN_DATE}.*" ]; then
        echo "ERROR: Missing file ${file}_${RUN_DATE}"
        exit 1
    fi
done

echo "All SFTP downloads completed and verified"


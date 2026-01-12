#!/bin/bash
################################################################################
# SCP with Complex Variable Resolution
# Demonstrates SCP operations with environment variables and parameter expansion
################################################################################

set -e

# Load environment-specific configuration
ENV=${1:-prod}
source /etc/config/${ENV}/sftp_config.sh

# Configuration with variables
REMOTE_HOST="${BACKUP_HOST:-backup.prod.com}"
REMOTE_USER="${BACKUP_USER:-backup_svc}"
SSH_KEY="${SSH_KEY_PATH:-/keys/backup_key}"
REMOTE_BASE="${REMOTE_BASE_PATH:-/backups}"
LOCAL_BASE="${LOCAL_BASE_PATH:-/data/backups}"
RUN_DATE=${2:-$(date +%Y-%m-%d)}
RUN_TIMESTAMP=$(date +%Y%m%d_%H%M%S)

# Construct paths with parameter expansion
REMOTE_PATH="${REMOTE_BASE}/${ENV}/daily/${RUN_DATE}"
LOCAL_PATH="${LOCAL_BASE}/${ENV}/${RUN_DATE}"

# Create local directory
mkdir -p "${LOCAL_PATH}"

# Download database dumps
for db in customers orders products transactions; do
    echo "Downloading ${db} backup..."
    scp -i ${SSH_KEY} \
        ${REMOTE_USER}@${REMOTE_HOST}:${REMOTE_PATH}/${db}_${RUN_DATE}.sql.gz \
        ${LOCAL_PATH}/
done

# Download compressed archives
scp -i ${SSH_KEY} -C \
    ${REMOTE_USER}@${REMOTE_HOST}:${REMOTE_PATH}/full_backup_${RUN_TIMESTAMP}.tar.gz \
    ${LOCAL_PATH}/

# Download logs with port specification
scp -i ${SSH_KEY} -P 2222 \
    ${REMOTE_USER}@${REMOTE_HOST}:${REMOTE_PATH}/logs/*.log \
    ${LOCAL_PATH}/logs/

# Upload verification file
echo "${RUN_TIMESTAMP}" > ${LOCAL_PATH}/download_complete.txt
scp -i ${SSH_KEY} \
    ${LOCAL_PATH}/download_complete.txt \
    ${REMOTE_USER}@${REMOTE_HOST}:${REMOTE_PATH}/verified/

echo "SCP operations with variables completed"


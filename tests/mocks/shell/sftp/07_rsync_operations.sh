#!/bin/bash
################################################################################
# RSYNC Operations for Large File Transfers
# Demonstrates rsync for efficient file synchronization
################################################################################

set -e

# Configuration
REMOTE_HOST="fileserver.company.com"
REMOTE_USER="sync_user"
SSH_KEY="/keys/rsync_key"
RUN_DATE=$(date +%Y-%m-%d)

# RSYNC download - directory with checksum verification
rsync -avz -e "ssh -i ${SSH_KEY}" \
    ${REMOTE_USER}@${REMOTE_HOST}:/data/exports/${RUN_DATE}/ \
    /data/incoming/${RUN_DATE}/

# RSYNC download - specific file types with exclusions
rsync -avz --include='*.csv' --include='*.parquet' --exclude='*' \
    -e "ssh -i ${SSH_KEY}" \
    ${REMOTE_USER}@${REMOTE_HOST}:/data/warehouse/daily/ \
    /data/warehouse_sync/

# RSYNC upload - compressed transfer
rsync -avz --compress -e "ssh -i ${SSH_KEY}" \
    /data/processed/${RUN_DATE}/ \
    ${REMOTE_USER}@${REMOTE_HOST}:/data/incoming/processed/${RUN_DATE}/

# RSYNC with bandwidth limit - 10MB/s
rsync -avz --bwlimit=10000 -e "ssh -i ${SSH_KEY}" \
    ${REMOTE_USER}@${REMOTE_HOST}:/backups/large_dataset.tar.gz \
    /data/backups/

# RSYNC with delete - mirror remote directory
rsync -avz --delete -e "ssh -i ${SSH_KEY}" \
    ${REMOTE_USER}@${REMOTE_HOST}:/data/master_data/ \
    /data/master_sync/

echo "RSYNC operations completed"


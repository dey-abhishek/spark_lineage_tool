#!/bin/bash
################################################################################
# Basic SCP Operations
# Demonstrates SCP download and upload operations
################################################################################

set -e

# Configuration
REMOTE_HOST="backup.company.com"
REMOTE_USER="backup_user"
SSH_KEY="/home/user/.ssh/id_rsa"
RUN_DATE=$(date +%Y%m%d)

# SCP download - single file
scp ${REMOTE_USER}@${REMOTE_HOST}:/backups/database_${RUN_DATE}.sql /data/backups/

# SCP download with custom SSH key
scp -i ${SSH_KEY} ${REMOTE_USER}@${REMOTE_HOST}:/backups/logs/app_${RUN_DATE}.log /var/log/imports/

# SCP download - multiple files with wildcard
scp ${REMOTE_USER}@${REMOTE_HOST}:/exports/data/*.csv /data/incoming/

# SCP upload - single file
scp /data/exports/report_${RUN_DATE}.pdf ${REMOTE_USER}@${REMOTE_HOST}:/reports/daily/

# SCP upload with SSH key
scp -i ${SSH_KEY} /data/processed/summary.json ${REMOTE_USER}@${REMOTE_HOST}:/incoming/processed/

# SCP recursive directory
scp -r ${REMOTE_USER}@${REMOTE_HOST}:/archives/${RUN_DATE}/ /data/archives/

echo "SCP operations completed successfully"


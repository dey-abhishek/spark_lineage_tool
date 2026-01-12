#!/bin/bash
# HDFS copy operations

SOURCE_PATH="/data/raw/input"
TARGET_PATH="/data/processed/output"
DATE=$(date +%Y-%m-%d)

# Copy files
hdfs dfs -cp ${SOURCE_PATH}/${DATE}/* ${TARGET_PATH}/${DATE}/

# Move old files
hdfs dfs -mv /data/archive/old/* /data/backup/

echo "HDFS operations complete"


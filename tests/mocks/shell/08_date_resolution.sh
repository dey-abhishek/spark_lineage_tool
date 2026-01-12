#!/bin/bash
# Shell script demonstrating date expression resolution

# Date expressions that should be resolved
RUNDATE=$(date +%Y-%m-%d)
COMPACT_DATE=$(date +%Y%m%d)
YEAR=$(date +%Y)
MONTH=$(date +%m)

# Variable with date expression default (common pattern)
INPUT_DATE=${1:-$(date +%Y-%m-%d)}
OUTPUT_DATE=${2:-$(date +%Y%m%d)}

# F-string-like pattern in shell
BASE_PATH="/data/production"
INPUT_PATH="${BASE_PATH}/raw/${RUNDATE}"
OUTPUT_PATH="${BASE_PATH}/processed/${RUNDATE}"

# HDFS operations with resolved dates
hdfs dfs -get ${INPUT_PATH}/transactions/*.parquet /tmp/input
hdfs dfs -put /tmp/output/${COMPACT_DATE}/*.parquet ${OUTPUT_PATH}/

# Spark job with resolved parameters
spark-submit \
  --class com.example.ETL \
  --conf spark.app.name="ETL_${RUNDATE}" \
  /path/to/job.jar \
  --input "${INPUT_PATH}" \
  --output "${OUTPUT_PATH}" \
  --date "${RUNDATE}"

# Hive query with date partitions
hive -e "
  INSERT OVERWRITE TABLE analytics.daily_summary 
  PARTITION (date='${RUNDATE}')
  SELECT * FROM raw.transactions 
  WHERE date = '${RUNDATE}'
"

echo "ETL completed for date: ${RUNDATE}"


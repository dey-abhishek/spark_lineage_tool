#!/bin/bash
# Complex shell script with conditional logic and loops
# Tests: HDFS operations, spark-submit, hive, conditionals, loops

set -e

# Configuration
RUN_DATE=${1:-$(date +%Y-%m-%d)}
ENV=${2:-prod}
BASE_PATH="/data/${ENV}"

echo "Starting ETL pipeline for ${RUN_DATE} in ${ENV} environment"

# Check if input exists
if hdfs dfs -test -d ${BASE_PATH}/raw/transactions/${RUN_DATE}; then
    echo "Input data found for ${RUN_DATE}"
else
    echo "ERROR: Input data not found"
    exit 1
fi

# Copy files with filtering
hdfs dfs -cp ${BASE_PATH}/raw/transactions/${RUN_DATE}/*.parquet ${BASE_PATH}/staging/transactions/${RUN_DATE}/

# Process multiple file types in loop
for file_type in transactions users products; do
    echo "Processing ${file_type}..."
    
    if [ "${file_type}" = "transactions" ]; then
        INPUT="${BASE_PATH}/staging/${file_type}/${RUN_DATE}"
        OUTPUT="${BASE_PATH}/processed/${file_type}/${RUN_DATE}"
    else
        INPUT="${BASE_PATH}/raw/${file_type}"
        OUTPUT="${BASE_PATH}/processed/${file_type}"
    fi
    
    # Run Spark job
    spark-submit \
        --master yarn \
        --deploy-mode cluster \
        --num-executors 10 \
        --executor-memory 4G \
        --driver-memory 2G \
        --conf spark.sql.shuffle.partitions=200 \
        /jobs/process_${file_type}.py \
        --input ${INPUT} \
        --output ${OUTPUT} \
        --date ${RUN_DATE}
    
    if [ $? -eq 0 ]; then
        echo "${file_type} processed successfully"
    else
        echo "ERROR: ${file_type} processing failed"
        exit 1
    fi
done

# Run Hive aggregations
hive -e "
    SET hive.exec.dynamic.partition=true;
    SET hive.exec.dynamic.partition.mode=nonstrict;
    
    INSERT OVERWRITE TABLE ${ENV}_analytics.daily_summary
    PARTITION (date='${RUN_DATE}')
    SELECT 
        SUM(amount) as total_sales,
        COUNT(*) as txn_count
    FROM ${ENV}_processed.transactions
    WHERE date = '${RUN_DATE}';
"

# Run from file
hive -f /sql/aggregations.hql \
    --hivevar env=${ENV} \
    --hivevar run_date=${RUN_DATE}

# Distcp to backup
hadoop distcp \
    -update \
    -delete \
    ${BASE_PATH}/processed/transactions/${RUN_DATE} \
    /backup/${ENV}/transactions/${RUN_DATE}

# Cleanup staging
hdfs dfs -rm -r ${BASE_PATH}/staging/transactions/${RUN_DATE}

# Archive old data (older than 30 days)
ARCHIVE_DATE=$(date -d "${RUN_DATE} -30 days" +%Y-%m-%d)
if hdfs dfs -test -d ${BASE_PATH}/processed/transactions/${ARCHIVE_DATE}; then
    hdfs dfs -mv ${BASE_PATH}/processed/transactions/${ARCHIVE_DATE} \
                /archive/transactions/${ARCHIVE_DATE}
fi

echo "Pipeline completed successfully for ${RUN_DATE}"


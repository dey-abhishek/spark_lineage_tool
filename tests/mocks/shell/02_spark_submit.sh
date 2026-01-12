#!/bin/bash
# Spark submit with arguments

RUN_DATE=$1
INPUT_PATH="/data/raw/${RUN_DATE}"
OUTPUT_PATH="/data/processed/${RUN_DATE}"

spark-submit \
  --master yarn \
  --deploy-mode cluster \
  --num-executors 10 \
  --executor-memory 4G \
  --driver-memory 2G \
  --conf spark.sql.shuffle.partitions=200 \
  /path/to/spark_job.py \
  --input ${INPUT_PATH} \
  --output ${OUTPUT_PATH} \
  --date ${RUN_DATE}

echo "Spark job submitted for date: ${RUN_DATE}"


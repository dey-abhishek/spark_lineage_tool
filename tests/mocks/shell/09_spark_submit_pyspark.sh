#!/bin/bash
# Test spark-submit with PySpark jobs
# Various configurations and argument patterns

set -e

RUN_DATE=${1:-2024-01-15}
ENV=${2:-prod}

echo "Running PySpark ETL jobs for ${RUN_DATE}"

# Simple spark-submit with minimal args
spark-submit \
    --master yarn \
    --deploy-mode cluster \
    /jobs/simple_etl.py

# Spark-submit with extensive configurations
spark-submit \
    --master yarn \
    --deploy-mode cluster \
    --name "Customer ETL Job" \
    --num-executors 10 \
    --executor-memory 4G \
    --executor-cores 4 \
    --driver-memory 2G \
    --driver-cores 2 \
    --conf spark.sql.shuffle.partitions=200 \
    --conf spark.dynamicAllocation.enabled=true \
    --conf spark.sql.adaptive.enabled=true \
    --py-files /libs/utils.zip \
    --files /configs/app.conf \
    /jobs/customer_etl.py \
    --input-path /data/${ENV}/raw/customers/${RUN_DATE} \
    --output-path /data/${ENV}/processed/customers/${RUN_DATE} \
    --date ${RUN_DATE} \
    --env ${ENV}

# Spark-submit with equals-style configs
spark-submit --master=yarn --deploy-mode=cluster \
    --name="Transaction Processing" \
    --num-executors=20 \
    --executor-memory=8G \
    /jobs/transaction_processor.py \
    --input /data/${ENV}/raw/transactions \
    --output /data/${ENV}/processed/transactions \
    --partition-date ${RUN_DATE}

# Spark-submit with multiple conf parameters
spark-submit \
    --master yarn \
    --conf spark.sql.sources.partitionOverwriteMode=dynamic \
    --conf spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version=2 \
    --conf spark.sql.parquet.compression.codec=snappy \
    /jobs/optimize_tables.py \
    --tables customers,transactions,orders

# Spark-submit with environment variables
export SPARK_HOME=/opt/spark
export HADOOP_CONF_DIR=/etc/hadoop/conf

${SPARK_HOME}/bin/spark-submit \
    --master yarn \
    --deploy-mode cluster \
    --queue production \
    /jobs/daily_aggregation.py \
    ${RUN_DATE} ${ENV}

# Spark-submit in conditional
if [ "${ENV}" = "prod" ]; then
    spark-submit \
        --master yarn \
        --deploy-mode cluster \
        --driver-memory 4G \
        /jobs/prod_only_report.py \
        --date ${RUN_DATE}
fi

# Spark-submit in loop
for dataset in customers orders products; do
    echo "Processing ${dataset}..."
    spark-submit \
        --master yarn \
        --deploy-mode cluster \
        --name "Process ${dataset}" \
        /jobs/generic_processor.py \
        --dataset ${dataset} \
        --date ${RUN_DATE}
done

echo "All PySpark jobs completed"


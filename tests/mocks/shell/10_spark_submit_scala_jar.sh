#!/bin/bash
# Test spark-submit with Scala/Java JAR files
# Different JAR patterns and configurations

set -e

RUN_DATE=${1:-2024-01-15}
ENV=${2:-prod}

echo "Running Scala Spark JAR jobs for ${RUN_DATE}"

# Simple JAR execution
spark-submit \
    --master yarn \
    --deploy-mode cluster \
    --class com.example.SimpleETL \
    /jars/simple-etl.jar

# JAR with extensive configurations
spark-submit \
    --master yarn \
    --deploy-mode cluster \
    --class com.company.etl.CustomerProcessor \
    --name "Customer Processing JAR" \
    --num-executors 15 \
    --executor-memory 6G \
    --executor-cores 3 \
    --driver-memory 3G \
    --conf spark.sql.shuffle.partitions=300 \
    --conf spark.speculation=true \
    --conf spark.task.maxFailures=4 \
    --jars /libs/mysql-connector.jar,/libs/postgres-driver.jar \
    /jars/customer-etl-1.0.0.jar \
    --input-path hdfs:///data/${ENV}/raw/customers/${RUN_DATE} \
    --output-path hdfs:///data/${ENV}/processed/customers/${RUN_DATE} \
    --run-date ${RUN_DATE} \
    --environment ${ENV}

# JAR with Hive support
spark-submit \
    --master yarn \
    --deploy-mode cluster \
    --class com.company.spark.HiveIntegration \
    --conf spark.sql.warehouse.dir=/user/hive/warehouse \
    --conf spark.sql.catalogImplementation=hive \
    --conf hive.metastore.uris=thrift://metastore:9083 \
    /jars/hive-integration-2.1.0.jar \
    --source-table ${ENV}_raw.transactions \
    --target-table ${ENV}_processed.transaction_summary \
    --partition-date ${RUN_DATE}

# JAR with Delta Lake
spark-submit \
    --master yarn \
    --deploy-mode cluster \
    --class com.company.delta.DeltaWriter \
    --packages io.delta:delta-core_2.12:2.0.0 \
    --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
    --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
    /jars/delta-etl-1.5.0.jar \
    --source /data/raw/events \
    --target /data/delta/events_processed \
    --checkpoint-location /data/delta/checkpoints/events

# JAR with Kafka streaming
spark-submit \
    --master yarn \
    --deploy-mode cluster \
    --class com.company.streaming.KafkaProcessor \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.0 \
    --conf spark.streaming.kafka.maxRatePerPartition=1000 \
    /jars/kafka-streaming-app.jar \
    --kafka-brokers kafka1:9092,kafka2:9092 \
    --input-topic raw-events \
    --output-path /data/processed/streaming/events \
    --checkpoint /data/checkpoints/kafka-events

# JAR with custom classpath
export HADOOP_CLASSPATH=/opt/custom/libs/*

spark-submit \
    --master yarn \
    --deploy-mode cluster \
    --class com.company.CustomMain \
    --driver-class-path /opt/custom/libs/* \
    --conf spark.executor.extraClassPath=/opt/custom/libs/* \
    /jars/custom-app-3.0.0-fat.jar \
    arg1 arg2 arg3

# JAR with YARN queue and priority
spark-submit \
    --master yarn \
    --deploy-mode cluster \
    --queue high-priority \
    --class com.company.CriticalJob \
    --conf spark.yarn.priority=1 \
    --conf spark.yarn.maxAppAttempts=2 \
    /jars/critical-job.jar \
    --mode production \
    --date ${RUN_DATE}

# JAR with resource files
spark-submit \
    --master yarn \
    --deploy-mode cluster \
    --class com.company.ConfigDrivenETL \
    --files /configs/prod.conf,/configs/schema.json \
    --archives /libs/dependencies.zip \
    /jars/config-driven-etl.jar \
    --config-file prod.conf \
    --schema-file schema.json

# Conditional JAR execution based on environment
if [ "${ENV}" = "prod" ]; then
    JAR_FILE="/jars/production-etl-2.0.0.jar"
    MAIN_CLASS="com.company.prod.ProductionETL"
else
    JAR_FILE="/jars/dev-etl-2.0.0.jar"
    MAIN_CLASS="com.company.dev.DevETL"
fi

spark-submit \
    --master yarn \
    --deploy-mode cluster \
    --class ${MAIN_CLASS} \
    ${JAR_FILE} \
    --env ${ENV} \
    --date ${RUN_DATE}

echo "All Scala/Java JAR jobs completed"


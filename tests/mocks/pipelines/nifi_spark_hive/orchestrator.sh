#!/bin/bash
################################################################################
# Master Orchestrator for NiFi + Spark + Hive Pipeline
# Coordinates NiFi flow execution with Spark and Hive processing
################################################################################

set -e

# Configuration
RUN_DATE=${1:-$(date +%Y-%m-%d)}
ENV=${2:-prod}
PIPELINE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
NIFI_URL="http://nifi-cluster:8080/nifi-api"

echo "═══════════════════════════════════════════════════════════════"
echo "  Starting NiFi + Spark + Hive Pipeline"
echo "  Date: ${RUN_DATE}"
echo "  Environment: ${ENV}"
echo "═══════════════════════════════════════════════════════════════"

# Stage 1: Trigger NiFi Flow
echo ""
echo "Stage 1/4: NiFi Data Ingestion"
echo "───────────────────────────────────────────────────────────────"
echo "NiFi flow configuration: ${PIPELINE_DIR}/01_nifi_ingestion.json"
echo "NiFi will ingest data from:"
echo "  - S3: raw-data-bucket/sales/daily/${RUN_DATE}/"
echo "  - HDFS: /logs/application/${RUN_DATE}/"
echo "  - Kafka: user-events, clickstream topics"

# Note: In production, you would trigger NiFi via REST API
# curl -X PUT "${NIFI_URL}/processors/fetch-s3-raw/run-status" \
#     -H "Content-Type: application/json" \
#     -d '{"state": "RUNNING"}'

echo "Waiting for NiFi ingestion to complete..."
sleep 5  # Simulated wait

# Stage 2: Spark Processing
echo ""
echo "Stage 2/4: Spark Processing Layer"
echo "───────────────────────────────────────────────────────────────"
spark-submit \
    --master yarn \
    --deploy-mode cluster \
    --num-executors 20 \
    --executor-cores 4 \
    --executor-memory 16G \
    --driver-memory 8G \
    --conf spark.sql.adaptive.enabled=true \
    --conf spark.sql.adaptive.coalescePartitions.enabled=true \
    --conf spark.dynamicAllocation.enabled=true \
    --conf spark.dynamicAllocation.minExecutors=5 \
    --conf spark.dynamicAllocation.maxExecutors=50 \
    ${PIPELINE_DIR}/02_spark_processing.py ${RUN_DATE} ${ENV}

# Stage 3: Hive Analytics
echo ""
echo "Stage 3/4: Hive Analytics Layer"
echo "───────────────────────────────────────────────────────────────"
hive -hiveconf RUN_DATE=${RUN_DATE} -hiveconf ENV=${ENV} \
    -hiveconf hive.exec.dynamic.partition=true \
    -hiveconf hive.exec.dynamic.partition.mode=nonstrict \
    -f ${PIPELINE_DIR}/03_hive_analytics.hql

# Stage 4: Spark ML
echo ""
echo "Stage 4/4: Spark ML and Predictions"
echo "───────────────────────────────────────────────────────────────"
spark-submit \
    --master yarn \
    --deploy-mode cluster \
    --num-executors 15 \
    --executor-cores 4 \
    --executor-memory 12G \
    --driver-memory 8G \
    --conf spark.sql.adaptive.enabled=true \
    --packages org.apache.spark:spark-mllib_2.12:3.3.0 \
    ${PIPELINE_DIR}/04_spark_ml.py ${RUN_DATE} ${ENV}

echo ""
echo "═══════════════════════════════════════════════════════════════"
echo "  NiFi + Spark + Hive Pipeline Completed Successfully!"
echo "  Duration: $SECONDS seconds"
echo "  Output locations:"
echo "    - Staging: staging_${ENV}.*"
echo "    - Analytics: analytics_${ENV}.*"
echo "    - ML: ml_${ENV}.*"
echo "    - Exports: /data/ml_exports/${ENV}/*/${RUN_DATE}/"
echo "═══════════════════════════════════════════════════════════════"


#!/bin/bash
################################################################################
# Master Pipeline Orchestrator
# Executes the complete SFTP → Spark → Hive → Analytics → Export pipeline
################################################################################

set -e

# Configuration
RUN_DATE=${1:-$(date +%Y-%m-%d)}
ENV=${2:-prod}
PIPELINE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

echo "═══════════════════════════════════════════════════════════════"
echo "  Starting Complete Data Pipeline"
echo "  Date: ${RUN_DATE}"
echo "  Environment: ${ENV}"
echo "═══════════════════════════════════════════════════════════════"

# Stage 1: SFTP Ingestion
echo ""
echo "Stage 1/5: SFTP Ingestion"
echo "───────────────────────────────────────────────────────────────"
bash ${PIPELINE_DIR}/01_sftp_ingestion.sh ${RUN_DATE} ${ENV}

# Stage 2: Spark Staging
echo ""
echo "Stage 2/5: Spark Staging Layer"
echo "───────────────────────────────────────────────────────────────"
spark-submit \
    --master yarn \
    --deploy-mode client \
    --num-executors 10 \
    --executor-cores 4 \
    --executor-memory 8G \
    --driver-memory 4G \
    --conf spark.sql.shuffle.partitions=200 \
    --conf spark.dynamicAllocation.enabled=true \
    ${PIPELINE_DIR}/02_spark_staging.py ${RUN_DATE} ${ENV}

# Stage 3: Hive Enrichment
echo ""
echo "Stage 3/5: Hive Enrichment Layer"
echo "───────────────────────────────────────────────────────────────"
hive -hiveconf RUN_DATE=${RUN_DATE} -hiveconf ENV=${ENV} \
    -f ${PIPELINE_DIR}/03_hive_enrichment.hql

# Stage 4: Spark Analytics
echo ""
echo "Stage 4/5: Spark Analytics Layer"
echo "───────────────────────────────────────────────────────────────"
spark-submit \
    --master yarn \
    --deploy-mode client \
    --num-executors 15 \
    --executor-cores 4 \
    --executor-memory 8G \
    --driver-memory 4G \
    --conf spark.sql.adaptive.enabled=true \
    --conf spark.sql.adaptive.coalescePartitions.enabled=true \
    ${PIPELINE_DIR}/04_spark_analytics.py ${RUN_DATE} ${ENV}

# Stage 5: Export to SFTP
echo ""
echo "Stage 5/5: Export to SFTP"
echo "───────────────────────────────────────────────────────────────"
bash ${PIPELINE_DIR}/05_export_to_sftp.sh ${RUN_DATE} ${ENV}

echo ""
echo "═══════════════════════════════════════════════════════════════"
echo "  Pipeline Completed Successfully!"
echo "  Duration: $SECONDS seconds"
echo "═══════════════════════════════════════════════════════════════"


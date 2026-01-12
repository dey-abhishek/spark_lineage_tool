#!/bin/bash
################################################################################
# Master Orchestrator - Comprehensive Multi-Technology Pipeline
# Coordinates: NiFi → Spark → Hive → Scala → Python → Export
################################################################################

set -e

# Configuration
RUN_DATE=${1:-$(date +%Y-%m-%d)}
ENV=${2:-prod}
PIPELINE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

echo "╔═══════════════════════════════════════════════════════════════╗"
echo "║                                                               ║"
echo "║   COMPREHENSIVE MULTI-TECHNOLOGY DATA PIPELINE                ║"
echo "║                                                               ║"
echo "║   Sources: SFTP, S3, Kafka, RDBMS                            ║"
echo "║   Processing: NiFi, PySpark, Scala, Hive                     ║"
echo "║   Exports: SFTP, JDBC, HDFS Archive                          ║"
echo "║                                                               ║"
echo "╚═══════════════════════════════════════════════════════════════╝"
echo ""
echo "Run Date: ${RUN_DATE}"
echo "Environment: ${ENV}"
echo "Pipeline Directory: ${PIPELINE_DIR}"
echo ""

START_TIME=$(date +%s)

# Stage 1: NiFi Multi-Source Ingestion
echo "═══════════════════════════════════════════════════════════════"
echo "  STAGE 1/5: NiFi Multi-Source Data Ingestion"
echo "═══════════════════════════════════════════════════════════════"
echo ""
echo "NiFi Flow: ${PIPELINE_DIR}/01_nifi_multi_source.json"
echo ""
echo "Ingesting from:"
echo "  • SFTP: vendor1.data.com:/exports/transactions/${RUN_DATE}/"
echo "  • SFTP: vendor2.data.com:/feeds/customers/${RUN_DATE}/"
echo "  • S3:   s3://external-data-lake/partners/*/data/${RUN_DATE}/"
echo "  • Kafka: transactions, events, metrics topics"
echo "  • RDBMS: Oracle PROD_DB.CUSTOMER_MASTER"
echo ""
echo "Landing Zone:"
echo "  • /data/raw/transactions/${ENV}/${RUN_DATE}/"
echo "  • /data/raw/customers/${ENV}/${RUN_DATE}/"
echo "  • /data/raw/partners/${ENV}/${RUN_DATE}/"
echo "  • /data/raw/realtime/${ENV}/${RUN_DATE}/"
echo ""
sleep 3

# Stage 2: Spark Unified Processing (PySpark)
echo ""
echo "═══════════════════════════════════════════════════════════════"
echo "  STAGE 2/5: Spark Unified Processing (PySpark)"
echo "═══════════════════════════════════════════════════════════════"
echo ""

spark-submit \
    --master yarn \
    --deploy-mode cluster \
    --name "Comprehensive-Unified-Processing-${ENV}-${RUN_DATE}" \
    --num-executors 30 \
    --executor-cores 4 \
    --executor-memory 16G \
    --driver-memory 8G \
    --conf spark.sql.adaptive.enabled=true \
    --conf spark.sql.adaptive.coalescePartitions.enabled=true \
    --conf spark.dynamicAllocation.enabled=true \
    --conf spark.dynamicAllocation.minExecutors=10 \
    --conf spark.dynamicAllocation.maxExecutors=100 \
    --conf spark.sql.shuffle.partitions=400 \
    --conf spark.sql.sources.partitionOverwriteMode=dynamic \
    ${PIPELINE_DIR}/02_spark_unified_processing.py ${RUN_DATE} ${ENV}

# Stage 3: Hive Gold Layer (HiveQL)
echo ""
echo "═══════════════════════════════════════════════════════════════"
echo "  STAGE 3/5: Hive Gold Layer Processing"
echo "═══════════════════════════════════════════════════════════════"
echo ""

hive \
    -hiveconf RUN_DATE=${RUN_DATE} \
    -hiveconf ENV=${ENV} \
    -hiveconf hive.exec.dynamic.partition=true \
    -hiveconf hive.exec.dynamic.partition.mode=nonstrict \
    -hiveconf hive.exec.max.dynamic.partitions=10000 \
    -hiveconf hive.exec.max.dynamic.partitions.pernode=1000 \
    -f ${PIPELINE_DIR}/03_hive_gold_layer.hql

# Stage 4: Advanced Analytics (Scala Spark + ML)
echo ""
echo "═══════════════════════════════════════════════════════════════"
echo "  STAGE 4/5: Advanced Analytics (Scala Spark + ML)"
echo "═══════════════════════════════════════════════════════════════"
echo ""

spark-submit \
    --master yarn \
    --deploy-mode cluster \
    --name "Comprehensive-Analytics-${ENV}-${RUN_DATE}" \
    --class com.company.pipeline.ComprehensiveAnalytics \
    --num-executors 25 \
    --executor-cores 4 \
    --executor-memory 12G \
    --driver-memory 8G \
    --conf spark.sql.adaptive.enabled=true \
    --conf spark.ml.kmeans.initSteps=5 \
    --packages org.apache.spark:spark-mllib_2.12:3.3.0 \
    ${PIPELINE_DIR}/comprehensive-analytics.jar \
    ${RUN_DATE} ${ENV}

# Stage 5: Export Layer (PySpark with SFTP/JDBC)
echo ""
echo "═══════════════════════════════════════════════════════════════"
echo "  STAGE 5/5: Export Layer (Multi-Destination)"
echo "═══════════════════════════════════════════════════════════════"
echo ""

spark-submit \
    --master yarn \
    --deploy-mode cluster \
    --name "Comprehensive-Export-${ENV}-${RUN_DATE}" \
    --num-executors 15 \
    --executor-cores 2 \
    --executor-memory 8G \
    --driver-memory 4G \
    --packages com.springml:spark-sftp_2.11:1.1.5,org.postgresql:postgresql:42.3.1 \
    --conf spark.sql.adaptive.enabled=true \
    ${PIPELINE_DIR}/05_spark_export_layer.py ${RUN_DATE} ${ENV}

# Calculate duration
END_TIME=$(date +%s)
DURATION=$((END_TIME - START_TIME))
MINUTES=$((DURATION / 60))
SECONDS=$((DURATION % 60))

# Final Summary
echo ""
echo "╔═══════════════════════════════════════════════════════════════╗"
echo "║                                                               ║"
echo "║         COMPREHENSIVE PIPELINE COMPLETED SUCCESSFULLY!        ║"
echo "║                                                               ║"
echo "╚═══════════════════════════════════════════════════════════════╝"
echo ""
echo "Pipeline Summary:"
echo "───────────────────────────────────────────────────────────────"
echo "  Run Date:    ${RUN_DATE}"
echo "  Environment: ${ENV}"
echo "  Duration:    ${MINUTES}m ${SECONDS}s"
echo ""
echo "Data Flow:"
echo "  SFTP/S3/Kafka/RDBMS → NiFi → HDFS Landing"
echo "  → Spark Unified (unified_${ENV}.*)"
echo "  → Hive Gold Layer (gold_${ENV}.*)"
echo "  → Scala ML Analytics (analytics_${ENV}.*)"
echo "  → Multi-Destination Export (SFTP/JDBC/HDFS)"
echo ""
echo "Output Locations:"
echo "  • Unified Tables:    unified_${ENV}.*"
echo "  • Gold Tables:       gold_${ENV}.*"
echo "  • Analytics Tables:  analytics_${ENV}.*"
echo "  • Export Manifests:  export_${ENV}.manifest"
echo "  • SFTP Exports:      export.partner.com, alerts.company.com"
echo "  • JDBC Exports:      reporting-db/analytics"
echo "  • Archives:          /data/archive/${ENV}/*/${RUN_DATE}/"
echo ""
echo "═══════════════════════════════════════════════════════════════"


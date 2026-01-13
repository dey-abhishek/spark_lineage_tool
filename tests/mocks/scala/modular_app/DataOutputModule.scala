package com.company.etl.modules

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.streaming.Trigger

/**
 * DataOutputModule - Handles all data writing operations
 * Writes to Hive, HDFS, JDBC, Kafka
 */
object DataOutputModule {
  
  /**
   * Write enriched transactions to Hive Gold layer
   */
  def writeEnrichedTransactions(df: DataFrame, env: String, runDate: String): Unit = {
    val tableName = s"${env}_gold.enriched_transactions"
    println(s"Writing enriched transactions to: $tableName")
    
    df.write
      .mode(SaveMode.Append)
      .partitionBy("run_date")
      .format("parquet")
      .saveAsTable(tableName)
  }
  
  /**
   * Write customer metrics to HDFS
   */
  def writeCustomerMetrics(df: DataFrame, basePath: String = "/data/gold", runDate: String): Unit = {
    val path = s"${basePath}/customer_metrics/date=${runDate}"
    println(s"Writing customer metrics to: $path")
    
    df.write
      .mode(SaveMode.Overwrite)
      .partitionBy("region")
      .parquet(path)
  }
  
  /**
   * Write fraud alerts to JDBC (PostgreSQL)
   */
  def writeFraudAlerts(df: DataFrame, jdbcUrl: String = "jdbc:postgresql://prod-pg:5432/alerts"): Unit = {
    println(s"Writing fraud alerts to: $jdbcUrl")
    
    df.write
      .mode(SaveMode.Append)
      .format("jdbc")
      .option("url", jdbcUrl)
      .option("dbtable", "fraud_alerts")
      .option("user", "etl_user")
      .option("password", sys.env.getOrElse("PG_PASSWORD", ""))
      .option("driver", "org.postgresql.Driver")
      .save()
  }
  
  /**
   * Write product summary to Hive reporting layer
   */
  def writeProductSummary(df: DataFrame, env: String): Unit = {
    val tableName = s"${env}_reports.product_daily_summary"
    println(s"Writing product summary to: $tableName")
    
    df.write
      .mode(SaveMode.Overwrite)
      .format("orc")
      .option("compression", "snappy")
      .saveAsTable(tableName)
  }
  
  /**
   * Write processed events to Kafka
   */
  def writeEventsToKafka(df: DataFrame, topic: String = "processed-events"): Unit = {
    println(s"Writing processed events to Kafka topic: $topic")
    
    df.selectExpr("CAST(customer_id AS STRING) AS key", "to_json(struct(*)) AS value")
      .writeStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "kafka-broker:9092")
      .option("topic", topic)
      .option("checkpointLocation", s"/tmp/checkpoints/${topic}")
      .trigger(Trigger.ProcessingTime("30 seconds"))
      .start()
  }
  
  /**
   * Write aggregated data to Delta Lake
   */
  def writeToDeltalake(df: DataFrame, basePath: String = "/data/delta", tableName: String): Unit = {
    val path = s"${basePath}/${tableName}"
    println(s"Writing to Delta Lake: $path")
    
    df.write
      .format("delta")
      .mode(SaveMode.Overwrite)
      .option("mergeSchema", "true")
      .save(path)
  }
  
  /**
   * Export reports to S3 as CSV
   */
  def exportToS3(df: DataFrame, s3Bucket: String = "s3a://company-reports", reportName: String, runDate: String): Unit = {
    val path = s"${s3Bucket}/daily/${reportName}/date=${runDate}"
    println(s"Exporting report to S3: $path")
    
    df.write
      .mode(SaveMode.Overwrite)
      .option("header", "true")
      .option("compression", "gzip")
      .csv(path)
  }
}


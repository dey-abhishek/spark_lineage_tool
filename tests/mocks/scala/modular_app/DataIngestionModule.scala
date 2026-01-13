package com.company.etl.modules

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

/**
 * DataIngestionModule - Handles all data ingestion operations
 * Reads from multiple sources: HDFS, Hive, JDBC
 */
object DataIngestionModule {
  
  /**
   * Read customer data from Hive
   */
  def readCustomers(spark: SparkSession, env: String): DataFrame = {
    println(s"Reading customers from ${env}_raw.customers")
    spark.table(s"${env}_raw.customers")
      .filter(col("status") === "active")
      .select("customer_id", "name", "email", "registration_date", "region")
  }
  
  /**
   * Read transaction data from HDFS Parquet
   */
  def readTransactions(spark: SparkSession, runDate: String, basePath: String = "/data/raw"): DataFrame = {
    val path = s"${basePath}/transactions/date=${runDate}/*.parquet"
    println(s"Reading transactions from: $path")
    
    spark.read
      .option("mergeSchema", "true")
      .parquet(path)
      .select("transaction_id", "customer_id", "amount", "timestamp", "status")
  }
  
  /**
   * Read product catalog from JDBC (MySQL)
   */
  def readProducts(spark: SparkSession, jdbcUrl: String = "jdbc:mysql://prod-mysql:3306/catalog"): DataFrame = {
    println(s"Reading products from: $jdbcUrl")
    
    spark.read
      .format("jdbc")
      .option("url", jdbcUrl)
      .option("dbtable", "products")
      .option("user", "etl_user")
      .option("password", sys.env.getOrElse("MYSQL_PASSWORD", ""))
      .option("driver", "com.mysql.jdbc.Driver")
      .load()
      .select("product_id", "product_name", "category", "price", "supplier_id")
  }
  
  /**
   * Read event stream from Kafka
   */
  def readEvents(spark: SparkSession, topic: String = "customer-events"): DataFrame = {
    println(s"Reading events from Kafka topic: $topic")
    
    spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "kafka-broker:9092")
      .option("subscribe", topic)
      .option("startingOffsets", "latest")
      .load()
      .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)", "timestamp")
  }
  
  /**
   * Read reference data from S3
   */
  def readReferenceData(spark: SparkSession, s3Bucket: String = "s3a://company-data"): DataFrame = {
    val path = s"${s3Bucket}/reference/regions/*.json"
    println(s"Reading reference data from: $path")
    
    spark.read
      .option("multiLine", "true")
      .json(path)
  }
}


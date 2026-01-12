package com.example.spark.sftp

import org.apache.spark.sql.{SparkSession, SaveMode}
import org.apache.spark.sql.functions._

/**
 * Scala Spark with SFTP Data Source
 * Uses spark-sftp library to read from SFTP servers
 */
object SFTPDataIngestion {
  
  def main(args: Array[String]): Unit = {
    // Configuration
    val sftpHost = "sftp.vendor.com"
    val sftpPort = "22"
    val sftpUser = "data_reader"
    val sftpKeyPath = "/keys/sftp_key"
    val remoteBase = "/exports/daily"
    val runDate = java.time.LocalDate.now().toString
    
    // Create Spark session with SFTP support
    val spark = SparkSession.builder()
      .appName("SFTP Data Ingestion - Scala")
      .config("spark.jars.packages", "com.springml:spark-sftp_2.12:1.1.3")
      .getOrCreate()
    
    // Read CSV from SFTP
    val salesDF = spark.read
      .format("com.springml.spark.sftp")
      .option("host", sftpHost)
      .option("port", sftpPort)
      .option("username", sftpUser)
      .option("privateKeyFile", sftpKeyPath)
      .option("fileType", "csv")
      .option("inferSchema", "true")
      .option("header", "true")
      .load(s"$remoteBase/sales/sales_$runDate.csv")
    
    // Read Parquet from SFTP with wildcard
    val transactionsDF = spark.read
      .format("com.springml.spark.sftp")
      .option("host", sftpHost)
      .option("username", sftpUser)
      .option("privateKeyFile", sftpKeyPath)
      .option("fileType", "parquet")
      .load(s"$remoteBase/transactions/transactions_*.parquet")
    
    // Process data
    val processedDF = salesDF.join(
      transactionsDF,
      salesDF("order_id") === transactionsDF("transaction_id"),
      "left"
    ).withColumn("processing_time", current_timestamp())
    
    // Write to HDFS
    processedDF.write
      .mode(SaveMode.Overwrite)
      .partitionBy("date")
      .parquet(s"/data/processed/sales_transactions/$runDate")
    
    // Write to Hive
    processedDF.write
      .mode(SaveMode.Overwrite)
      .saveAsTable("analytics.daily_sales_transactions")
    
    spark.stop()
  }
}


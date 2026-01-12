package com.example.spark.sftp

import org.apache.spark.sql.{SparkSession, SaveMode}
import org.apache.spark.sql.functions._

/**
 * Scala Spark with SFTP Write Operations
 * Demonstrates writing DataFrames to SFTP servers
 */
object SFTPDataExport {
  
  def main(args: Array[String]): Unit = {
    val sftpHost = "partner.sftp.com"
    val sftpPort = "22"
    val sftpUser = "data_sender"
    val sftpPassword = sys.env.getOrElse("SFTP_PASSWORD", "")
    val remoteOutbox = "/incoming/reports"
    val runDate = java.time.LocalDate.now().toString
    
    val spark = SparkSession.builder()
      .appName("SFTP Data Export - Scala")
      .config("spark.jars.packages", "com.springml:spark-sftp_2.12:1.1.3")
      .getOrCreate()
    
    // Read processed data from Hive
    val dailyReportDF = spark.table("analytics.daily_summary")
    
    // Filter for current date
    val reportDF = dailyReportDF.filter(col("report_date") === lit(runDate))
    
    // Write CSV to SFTP
    reportDF.write
      .format("com.springml.spark.sftp")
      .option("host", sftpHost)
      .option("port", sftpPort)
      .option("username", sftpUser)
      .option("password", sftpPassword)
      .option("fileType", "csv")
      .option("header", "true")
      .mode(SaveMode.Overwrite)
      .save(s"$remoteOutbox/daily_report_$runDate.csv")
    
    // Aggregate and write JSON summary
    val summaryDF = reportDF
      .groupBy("category")
      .agg(
        sum("amount").as("total_amount"),
        count("*").as("record_count")
      )
    
    summaryDF.write
      .format("com.springml.spark.sftp")
      .option("host", sftpHost)
      .option("username", sftpUser)
      .option("password", sftpPassword)
      .option("fileType", "json")
      .mode(SaveMode.Overwrite)
      .save(s"$remoteOutbox/summary_$runDate.json")
    
    // Write Parquet for detailed data
    val detailedDF = reportDF.select(
      "transaction_id",
      "customer_id",
      "amount",
      "category",
      "report_date"
    )
    
    detailedDF.write
      .format("com.springml.spark.sftp")
      .option("host", sftpHost)
      .option("username", sftpUser)
      .option("password", sftpPassword)
      .option("fileType", "parquet")
      .mode(SaveMode.Overwrite)
      .save(s"$remoteOutbox/detailed_$runDate.parquet")
    
    spark.stop()
  }
}


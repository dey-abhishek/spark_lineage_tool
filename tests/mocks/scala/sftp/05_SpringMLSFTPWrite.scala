package com.example.spark.sftp

import org.apache.spark.sql.{SparkSession, SaveMode}
import org.apache.spark.sql.functions._

/**
 * Scala Spark with springml spark-sftp library - Write Operations
 * Based on: https://github.com/springml/spark-sftp
 */
object SpringMLSFTPWrite {
  
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("SpringML SFTP Write - Scala")
      .config("spark.jars.packages", "com.springml:spark-sftp_2.11:1.1.5")
      .getOrCreate()
    
    import spark.implicits._
    
    // Read data from Hive
    val sourceDF = spark.table("analytics.customer_summary")
    
    // Add export timestamp
    val outputDF = sourceDF.withColumn("export_time", current_timestamp())
    
    // Write CSV to SFTP with password
    outputDF.write
      .format("com.springml.spark.sftp")
      .option("host", "partner.sftp.com")
      .option("username", "data_export")
      .option("password", "export_pass")
      .option("fileType", "csv")
      .option("header", "true")
      .option("delimiter", ",")
      .mode(SaveMode.Overwrite)
      .save("/outgoing/customer_export.csv")
    
    // Write JSON to SFTP with PEM
    val summaryDF = outputDF.groupBy("region").count()
    
    summaryDF.write
      .format("com.springml.spark.sftp")
      .option("host", "backup.company.com")
      .option("username", "backup_user")
      .option("pem", "/keys/backup_key.pem")
      .option("fileType", "json")
      .mode(SaveMode.Overwrite)
      .save("/backups/summary.json")
    
    // Write Parquet to SFTP
    outputDF.select("customer_id", "total_amount", "order_count").write
      .format("com.springml.spark.sftp")
      .option("host", "archive.example.com")
      .option("username", "archiver")
      .option("password", "archive123")
      .option("fileType", "parquet")
      .mode(SaveMode.Overwrite)
      .save("/archive/customers.parquet")
    
    // Write Avro to SFTP with PEM and passphrase
    outputDF.select("customer_id", "region", "total_amount").write
      .format("com.springml.spark.sftp")
      .option("host", "longterm.storage.com")
      .option("username", "avro_writer")
      .option("pem", "/keys/longterm_key.pem")
      .option("pemPassphrase", "secure_passphrase")
      .option("fileType", "avro")
      .mode(SaveMode.Overwrite)
      .save("/storage/customers.avro")
    
    spark.stop()
  }
}


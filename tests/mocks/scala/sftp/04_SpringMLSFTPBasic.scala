package com.example.spark.sftp

import org.apache.spark.sql.{SparkSession, SaveMode}
import org.apache.spark.sql.functions._

/**
 * Scala Spark with springml spark-sftp library - Basic Usage
 * Based on: https://github.com/springml/spark-sftp
 */
object SpringMLSFTPBasic {
  
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("SpringML SFTP Basic - Scala")
      .config("spark.jars.packages", "com.springml:spark-sftp_2.11:1.1.5")
      .getOrCreate()
    
    // Example 1: Read CSV from SFTP with password authentication
    val csvDF = spark.read
      .format("com.springml.spark.sftp")
      .option("host", "sftp.example.com")
      .option("username", "sftp_user")
      .option("password", "sftp_password")
      .option("fileType", "csv")
      .option("inferSchema", "true")
      .option("delimiter", ",")
      .option("header", "true")
      .load("/data/input/sample.csv")
    
    csvDF.show()
    
    // Example 2: Read CSV with PEM file authentication
    val pemDF = spark.read
      .format("com.springml.spark.sftp")
      .option("host", "sftp.example.com")
      .option("username", "sftp_user")
      .option("pem", "/path/to/private_key.pem")
      .option("fileType", "csv")
      .option("header", "true")
      .load("/data/input/customers.csv")
    
    // Example 3: Read JSON from SFTP
    val jsonDF = spark.read
      .format("com.springml.spark.sftp")
      .option("host", "sftp.example.com")
      .option("username", "data_user")
      .option("password", "pass123")
      .option("fileType", "json")
      .load("/data/input/transactions.json")
    
    // Example 4: Read Parquet from SFTP
    val parquetDF = spark.read
      .format("com.springml.spark.sftp")
      .option("host", "data.vendor.com")
      .option("username", "spark_reader")
      .option("pem", "/keys/id_rsa")
      .option("fileType", "parquet")
      .load("/exports/daily/sales.parquet")
    
    // Example 5: Read Avro from SFTP
    val avroDF = spark.read
      .format("com.springml.spark.sftp")
      .option("host", "archive.company.com")
      .option("username", "avro_reader")
      .option("pem", "/keys/archive_key.pem")
      .option("pemPassphrase", "key_password")
      .option("fileType", "avro")
      .load("/archives/events.avro")
    
    // Write to HDFS
    csvDF.write
      .mode(SaveMode.Overwrite)
      .parquet("/data/processed/customers")
    
    // Write to Hive
    jsonDF.write
      .mode(SaveMode.Overwrite)
      .saveAsTable("raw.transactions")
    
    spark.stop()
  }
}


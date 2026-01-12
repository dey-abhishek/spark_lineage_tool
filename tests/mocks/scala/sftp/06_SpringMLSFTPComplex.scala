package com.example.spark.sftp

import org.apache.spark.sql.{SparkSession, SaveMode, DataFrame}
import org.apache.spark.sql.functions._
import java.time.LocalDate

/**
 * Scala Spark with springml spark-sftp library - Complex Pipeline
 * Multiple SFTP sources with different authentication methods
 * Based on: https://github.com/springml/spark-sftp
 */
object SpringMLSFTPComplex {
  
  case class SFTPConfig(
    host: String,
    username: String,
    authType: String, // "password" or "pem"
    credential: String, // password or pem path
    pemPassphrase: Option[String] = None
  )
  
  def main(args: Array[String]): Unit = {
    val env = if (args.length > 0) args(0) else "production"
    val runDate = if (args.length > 1) args(1) else LocalDate.now().toString
    
    val spark = SparkSession.builder()
      .appName(s"SFTP Multi-Source Pipeline - $env")
      .config("spark.jars.packages", "com.springml:spark-sftp_2.11:1.1.5")
      .getOrCreate()
    
    // Define SFTP sources
    val sources = Map(
      "customers" -> SFTPConfig(
        "vendor1.data.com",
        "customer_sync",
        "pem",
        "/keys/vendor1.pem",
        Some("key_password")
      ),
      "transactions" -> SFTPConfig(
        "vendor2.data.com",
        "transaction_reader",
        "password",
        "trans_pass_2024"
      ),
      "products" -> SFTPConfig(
        "vendor3.data.com",
        "catalog_sync",
        "pem",
        "/keys/vendor3_rsa"
      ),
      "inventory" -> SFTPConfig(
        "inventory.vendor.com",
        "inv_reader",
        "password",
        "inv_secret"
      )
    )
    
    // Read customer master data (CSV with PEM)
    val customersDF = readFromSFTP(
      spark,
      sources("customers"),
      s"/exports/customers/customer_master_$runDate.csv",
      "csv",
      Map("header" -> "true", "inferSchema" -> "true", "delimiter" -> "|")
    ).withColumn("source", lit("vendor1"))
     .withColumn("load_date", current_date())
    
    // Read transaction data (JSON with password)
    val transactionsDF = readFromSFTP(
      spark,
      sources("transactions"),
      s"/feeds/transactions/trans_$runDate.json",
      "json"
    ).withColumn("source", lit("vendor2"))
     .withColumn("load_date", current_date())
    
    // Read product catalog (Parquet with PEM)
    val productsDF = readFromSFTP(
      spark,
      sources("products"),
      "/catalog/products/latest/*.parquet",
      "parquet"
    ).withColumn("source", lit("vendor3"))
     .withColumn("load_date", current_date())
    
    // Read inventory (Avro with password)
    val inventoryDF = readFromSFTP(
      spark,
      sources("inventory"),
      s"/inventory/stock_$runDate.avro",
      "avro"
    ).withColumn("source", lit("inventory_vendor"))
     .withColumn("load_date", current_date())
    
    // Write to HDFS landing zones
    customersDF.write
      .mode(SaveMode.Overwrite)
      .partitionBy("load_date")
      .parquet(s"/data/landing/$env/customers")
    
    transactionsDF.write
      .mode(SaveMode.Overwrite)
      .partitionBy("load_date")
      .parquet(s"/data/landing/$env/transactions")
    
    productsDF.write
      .mode(SaveMode.Overwrite)
      .partitionBy("load_date")
      .parquet(s"/data/landing/$env/products")
    
    inventoryDF.write
      .mode(SaveMode.Overwrite)
      .partitionBy("load_date")
      .parquet(s"/data/landing/$env/inventory")
    
    // Write to Hive staging tables
    customersDF.write.mode(SaveMode.Overwrite).saveAsTable(s"staging_$env.sftp_customers")
    transactionsDF.write.mode(SaveMode.Overwrite).saveAsTable(s"staging_$env.sftp_transactions")
    productsDF.write.mode(SaveMode.Overwrite).saveAsTable(s"staging_$env.sftp_products")
    inventoryDF.write.mode(SaveMode.Overwrite).saveAsTable(s"staging_$env.sftp_inventory")
    
    // Join and aggregate
    val enrichedDF = transactionsDF
      .join(customersDF, Seq("customer_id"), "left")
      .join(productsDF, Seq("product_id"), "left")
      .join(inventoryDF, Seq("product_id"), "left")
    
    // Write enriched data back to SFTP for partner (CSV with PEM)
    enrichedDF.select(
      "transaction_id",
      "customer_id",
      "product_id",
      "quantity",
      "amount"
    ).write
      .format("com.springml.spark.sftp")
      .option("host", "partner.exchange.com")
      .option("username", "data_exchange")
      .option("pem", "/keys/partner_exchange.pem")
      .option("fileType", "csv")
      .option("header", "true")
      .option("delimiter", ",")
      .mode(SaveMode.Overwrite)
      .save(s"/incoming/enriched_transactions_$runDate.csv")
    
    // Write summary to another SFTP location (JSON with password)
    val summaryDF = enrichedDF
      .groupBy("customer_id", "product_id")
      .agg(
        sum("amount").as("total_amount"),
        sum("quantity").as("total_quantity")
      )
    
    summaryDF.write
      .format("com.springml.spark.sftp")
      .option("host", "reporting.example.com")
      .option("username", "report_writer")
      .option("password", "report_pass")
      .option("fileType", "json")
      .mode(SaveMode.Overwrite)
      .save(s"/reports/daily_summary_$runDate.json")
    
    spark.stop()
  }
  
  def readFromSFTP(
    spark: SparkSession,
    config: SFTPConfig,
    path: String,
    fileType: String,
    extraOptions: Map[String, String] = Map()
  ): DataFrame = {
    var reader = spark.read
      .format("com.springml.spark.sftp")
      .option("host", config.host)
      .option("username", config.username)
      .option("fileType", fileType)
    
    // Add authentication options
    reader = config.authType match {
      case "password" =>
        reader.option("password", config.credential)
      case "pem" =>
        var pemReader = reader.option("pem", config.credential)
        config.pemPassphrase.foreach(pass => 
          pemReader = pemReader.option("pemPassphrase", pass)
        )
        pemReader
      case _ => reader
    }
    
    // Add extra options
    extraOptions.foreach { case (key, value) =>
      reader = reader.option(key, value)
    }
    
    reader.load(path)
  }
}


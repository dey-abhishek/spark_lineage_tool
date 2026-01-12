package com.example.spark.sftp

import org.apache.spark.sql.{SparkSession, SaveMode, DataFrame}
import org.apache.spark.sql.functions._

/**
 * Scala Spark with Multiple SFTP Sources
 * Complex ETL pipeline with multiple SFTP endpoints
 */
object MultiSFTPPipeline {
  
  case class VendorConfig(
    host: String,
    user: String,
    keyPath: Option[String],
    password: Option[String],
    basePath: String
  )
  
  def main(args: Array[String]): Unit = {
    val env = if (args.length > 0) args(0) else "prod"
    val runDate = if (args.length > 1) args(1) else java.time.LocalDate.now().toString
    
    // Vendor configurations
    val vendors = Map(
      "vendor1" -> VendorConfig(
        "vendor1.sftp.com",
        "data_ingest",
        Some("/keys/vendor1_key"),
        None,
        s"/exports/customers"
      ),
      "vendor2" -> VendorConfig(
        "vendor2.sftp.com",
        "data_sync",
        Some("/keys/vendor2_key"),
        None,
        s"/feeds/orders"
      ),
      "vendor3" -> VendorConfig(
        "vendor3.sftp.com",
        "spark_user",
        None,
        Some(sys.env.getOrElse("VENDOR3_PASSWORD", "default")),
        s"/data/products"
      )
    )
    
    val spark = SparkSession.builder()
      .appName(s"Multi-SFTP Pipeline - $env")
      .config("spark.jars.packages", "com.springml:spark-sftp_2.12:1.1.3")
      .getOrCreate()
    
    // Read from Vendor 1 - Customer data (CSV)
    val customersDF = readFromSFTP(
      spark,
      vendors("vendor1"),
      s"${vendors("vendor1").basePath}/customer_master_$runDate.csv",
      "csv"
    ).withColumn("vendor", lit("vendor1"))
     .withColumn("ingestion_timestamp", current_timestamp())
    
    // Read from Vendor 2 - Orders (JSON)
    val ordersDF = readFromSFTP(
      spark,
      vendors("vendor2"),
      s"${vendors("vendor2").basePath}/orders_$runDate.json",
      "json"
    ).withColumn("vendor", lit("vendor2"))
     .withColumn("ingestion_timestamp", current_timestamp())
    
    // Read from Vendor 3 - Products (Parquet with wildcard)
    val productsDF = readFromSFTP(
      spark,
      vendors("vendor3"),
      s"${vendors("vendor3").basePath}/products_*.parquet",
      "parquet"
    ).withColumn("vendor", lit("vendor3"))
     .withColumn("ingestion_timestamp", current_timestamp())
    
    // Write to HDFS landing zones
    customersDF.write
      .mode(SaveMode.Overwrite)
      .partitionBy("vendor")
      .parquet(s"/data/landing/$env/customers/$runDate")
    
    ordersDF.write
      .mode(SaveMode.Overwrite)
      .partitionBy("vendor")
      .parquet(s"/data/landing/$env/orders/$runDate")
    
    productsDF.write
      .mode(SaveMode.Overwrite)
      .partitionBy("vendor")
      .parquet(s"/data/landing/$env/products/$runDate")
    
    // Write to Hive staging tables
    customersDF.write
      .mode(SaveMode.Overwrite)
      .saveAsTable(s"staging_$env.external_customers")
    
    ordersDF.write
      .mode(SaveMode.Overwrite)
      .saveAsTable(s"staging_$env.external_orders")
    
    productsDF.write
      .mode(SaveMode.Overwrite)
      .saveAsTable(s"staging_$env.external_products")
    
    spark.stop()
  }
  
  def readFromSFTP(
    spark: SparkSession,
    config: VendorConfig,
    path: String,
    fileType: String
  ): DataFrame = {
    val reader = spark.read
      .format("com.springml.spark.sftp")
      .option("host", config.host)
      .option("username", config.user)
      .option("fileType", fileType)
    
    val authenticatedReader = (config.keyPath, config.password) match {
      case (Some(keyPath), _) => reader.option("privateKeyFile", keyPath)
      case (_, Some(password)) => reader.option("password", password)
      case _ => reader
    }
    
    authenticatedReader.load(path)
  }
}


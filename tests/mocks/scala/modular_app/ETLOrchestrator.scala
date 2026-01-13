package com.company.etl

import org.apache.spark.sql.SparkSession
import com.company.etl.modules.{DataIngestionModule, TransformationModule, DataOutputModule}

/**
 * ETLOrchestrator - Main application orchestrating the entire ETL pipeline
 * Coordinates all modules to execute the complete data flow
 */
object ETLOrchestrator {
  
  def main(args: Array[String]): Unit = {
    // Parse command-line arguments
    val env = if (args.length > 0) args(0) else "prod"
    val runDate = if (args.length > 1) args(1) else java.time.LocalDate.now().toString
    val basePath = if (args.length > 2) args(2) else "/data"
    
    println(s"Starting ETL Pipeline - env: $env, runDate: $runDate, basePath: $basePath")
    
    // Initialize Spark session
    val spark = SparkSession.builder()
      .appName(s"ETL-Pipeline-${env}-${runDate}")
      .config("spark.sql.warehouse.dir", s"${basePath}/warehouse")
      .config("spark.sql.adaptive.enabled", "true")
      .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
      .enableHiveSupport()
      .getOrCreate()
    
    try {
      // === INGESTION PHASE ===
      println("\n=== Starting Ingestion Phase ===")
      
      val customers = DataIngestionModule.readCustomers(spark, env)
      val transactions = DataIngestionModule.readTransactions(spark, runDate, s"${basePath}/raw")
      val products = DataIngestionModule.readProducts(spark)
      val referenceData = DataIngestionModule.readReferenceData(spark, s"s3a://company-data-${env}")
      
      customers.cache()
      products.cache()
      
      println(s"Ingested ${customers.count()} customers")
      println(s"Ingested ${transactions.count()} transactions")
      println(s"Ingested ${products.count()} products")
      
      // === TRANSFORMATION PHASE ===
      println("\n=== Starting Transformation Phase ===")
      
      val enrichedTransactions = TransformationModule.enrichTransactions(transactions, customers, products)
      val customerMetrics = TransformationModule.calculateCustomerMetrics(enrichedTransactions, runDate)
      val fraudAlerts = TransformationModule.detectFraudulentTransactions(enrichedTransactions)
      val productSummary = TransformationModule.generateProductSummary(enrichedTransactions, runDate)
      
      println(s"Enriched ${enrichedTransactions.count()} transactions")
      println(s"Calculated metrics for ${customerMetrics.count()} customers")
      println(s"Detected ${fraudAlerts.count()} fraud alerts")
      println(s"Generated summary for ${productSummary.count()} products")
      
      // === OUTPUT PHASE ===
      println("\n=== Starting Output Phase ===")
      
      DataOutputModule.writeEnrichedTransactions(enrichedTransactions, env, runDate)
      DataOutputModule.writeCustomerMetrics(customerMetrics, s"${basePath}/gold", runDate)
      DataOutputModule.writeFraudAlerts(fraudAlerts)
      DataOutputModule.writeProductSummary(productSummary, env)
      DataOutputModule.writeToDeltalake(customerMetrics, s"${basePath}/delta", "customer_metrics")
      DataOutputModule.exportToS3(productSummary, s"s3a://company-reports-${env}", "product_summary", runDate)
      
      println("\n=== ETL Pipeline Completed Successfully ===")
      
      // === STREAMING PHASE (Optional) ===
      if (env == "prod") {
        println("\n=== Starting Streaming Phase ===")
        val events = DataIngestionModule.readEvents(spark)
        val processedEvents = TransformationModule.processEvents(events)
        DataOutputModule.writeEventsToKafka(processedEvents, "processed-customer-events")
        
        // Keep streaming job running
        spark.streams.awaitAnyTermination()
      }
      
    } catch {
      case e: Exception =>
        println(s"ETL Pipeline failed: ${e.getMessage}")
        e.printStackTrace()
        System.exit(1)
    } finally {
      spark.stop()
    }
  }
}


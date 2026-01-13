package com.company.etl.modules

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

/**
 * TransformationModule - Handles all data transformation logic
 * Applies business rules, aggregations, and enrichment
 */
object TransformationModule {
  
  /**
   * Enrich transactions with customer and product details
   */
  def enrichTransactions(transactions: DataFrame, customers: DataFrame, products: DataFrame): DataFrame = {
    println("Enriching transactions with customer and product data")
    
    transactions
      .join(customers, Seq("customer_id"), "left")
      .join(products, Seq("product_id"), "left")
      .withColumn("enriched_timestamp", current_timestamp())
      .select(
        "transaction_id", "customer_id", "customer_name", "email",
        "product_id", "product_name", "category", "amount", "timestamp", "region"
      )
  }
  
  /**
   * Calculate customer metrics and aggregations
   */
  def calculateCustomerMetrics(transactions: DataFrame, runDate: String): DataFrame = {
    println(s"Calculating customer metrics for: $runDate")
    
    val windowSpec = Window.partitionBy("customer_id").orderBy(desc("timestamp"))
    
    transactions
      .groupBy("customer_id", "region")
      .agg(
        count("transaction_id").as("transaction_count"),
        sum("amount").as("total_spent"),
        avg("amount").as("avg_transaction_value"),
        max("timestamp").as("last_transaction_date"),
        countDistinct("category").as("unique_categories")
      )
      .withColumn("run_date", lit(runDate))
      .withColumn("calculated_at", current_timestamp())
  }
  
  /**
   * Apply fraud detection rules
   */
  def detectFraudulentTransactions(enrichedTransactions: DataFrame): DataFrame = {
    println("Applying fraud detection rules")
    
    enrichedTransactions
      .withColumn("is_high_value", when(col("amount") > 10000, true).otherwise(false))
      .withColumn("is_suspicious", 
        when(
          col("amount") > 5000 && 
          col("timestamp").cast("date") === current_date(), 
          true
        ).otherwise(false)
      )
      .filter(col("is_suspicious") === true || col("is_high_value") === true)
      .select("transaction_id", "customer_id", "amount", "timestamp", "is_suspicious", "is_high_value")
  }
  
  /**
   * Generate daily product summary
   */
  def generateProductSummary(transactions: DataFrame, runDate: String): DataFrame = {
    println(s"Generating product summary for: $runDate")
    
    transactions
      .filter(col("status") === "completed")
      .groupBy("product_id", "product_name", "category")
      .agg(
        count("transaction_id").as("sales_count"),
        sum("amount").as("total_revenue"),
        avg("amount").as("avg_price"),
        countDistinct("customer_id").as("unique_customers")
      )
      .withColumn("report_date", lit(runDate))
      .orderBy(desc("total_revenue"))
  }
  
  /**
   * Process real-time events
   */
  def processEvents(events: DataFrame): DataFrame = {
    println("Processing real-time events")
    
    events
      .selectExpr("CAST(value AS STRING) as json_value")
      .select(from_json(col("json_value"), 
        "struct<event_id:string,customer_id:string,event_type:string,timestamp:long>").as("data"))
      .select("data.*")
      .withColumn("processed_at", current_timestamp())
  }
}


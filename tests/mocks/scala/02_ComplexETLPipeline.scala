// Complex Scala Spark ETL with multiple transformations
package com.company.etl

import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

object ComplexETLPipeline {
  
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("ComplexScalaETL")
      .enableHiveSupport()
      .getOrCreate()
    
    import spark.implicits._
    
    // Configuration
    val runDate = args(0)
    val env = args.lift(1).getOrElse("prod")
    val basePath = s"/data/$env"
    
    // Read from multiple sources
    val dfTransactions = spark.read
      .parquet(s"$basePath/raw/transactions/$runDate")
    
    val dfUsers = spark.table(s"${env}_raw.users")
    
    val dfProducts = spark.read
      .format("delta")
      .load(s"$basePath/raw/products")
    
    // Complex joins with broadcasting
    val dfEnriched = dfTransactions
      .join(broadcast(dfUsers), Seq("user_id"), "left")
      .join(dfProducts, Seq("product_id"), "left")
    
    // Window functions
    val windowSpec = Window
      .partitionBy("user_id")
      .orderBy($"transaction_date".desc)
    
    val dfRanked = dfEnriched
      .withColumn("row_num", row_number().over(windowSpec))
      .withColumn("running_total", sum("amount").over(windowSpec))
    
    // Aggregations
    val dfSummary = dfRanked
      .groupBy("user_id", "category")
      .agg(
        sum("amount").as("total_amount"),
        avg("amount").as("avg_amount"),
        count("*").as("txn_count")
      )
    
    // Multiple outputs
    dfEnriched.write
      .mode("overwrite")
      .partitionBy("date")
      .parquet(s"$basePath/processed/enriched/$runDate")
    
    dfSummary.write
      .mode("overwrite")
      .saveAsTable(s"${env}_analytics.user_summary")
    
    dfRanked
      .filter($"row_num" === 1)
      .write
      .mode("overwrite")
      .orc(s"$basePath/processed/latest_transactions/$runDate")
    
    spark.stop()
  }
}


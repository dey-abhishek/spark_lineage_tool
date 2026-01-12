// Scala with string interpolation and case classes
package com.company.model

import org.apache.spark.sql.SparkSession

case class Config(
  inputPath: String,
  outputPath: String,
  database: String
)

object InterpolatedPaths {
  
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("InterpolatedPaths")
      .getOrCreate()
    
    val runDate = "2024-01-01"
    val env = "prod"
    val database = s"${env}_analytics"
    
    // String interpolation for paths
    val inputPath = s"/data/$env/raw/transactions/$runDate"
    val outputPath = s"/data/$env/processed/summary/$runDate"
    val tableName = s"$database.daily_summary"
    
    val config = Config(
      inputPath = s"/data/$env/config",
      outputPath = s"/data/$env/output",
      database = database
    )
    
    // Read with interpolated paths
    val df = spark.read.parquet(inputPath)
    
    // Process
    val dfProcessed = df.groupBy("category").count()
    
    // Write with interpolated paths
    dfProcessed.write.mode("overwrite").parquet(outputPath)
    dfProcessed.write.mode("overwrite").saveAsTable(tableName)
    
    // Use config
    val dfConfig = spark.read.json(config.inputPath)
    dfConfig.write.parquet(config.outputPath)
    
    spark.stop()
  }
}


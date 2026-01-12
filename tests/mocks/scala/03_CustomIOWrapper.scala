// Scala Spark with custom IO wrappers
package com.company.data

import org.apache.spark.sql.{DataFrame, SparkSession}

object CustomIOWrapper {
  
  class DataReader(spark: SparkSession) {
    def readTable(database: String, table: String): DataFrame = {
      spark.table(s"$database.$table")
    }
    
    def readPath(path: String, format: String = "parquet"): DataFrame = {
      spark.read.format(format).load(path)
    }
  }
  
  class DataWriter(df: DataFrame) {
    def writeToPath(path: String, format: String = "parquet"): Unit = {
      df.write.mode("overwrite").format(format).save(path)
    }
    
    def writeToTable(database: String, table: String): Unit = {
      df.write.mode("overwrite").saveAsTable(s"$database.$table")
    }
  }
  
  implicit class DataFrameOps(df: DataFrame) {
    def customWrite: DataWriter = new DataWriter(df)
  }
}

object CustomIOJob {
  import CustomIOWrapper._
  
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("CustomIOJob")
      .getOrCreate()
    
    val reader = new DataReader(spark)
    
    // Read using custom wrapper
    val dfInput = reader.readTable("prod", "transactions")
    val dfConfig = reader.readPath("/data/config/mappings", "json")
    
    // Process
    val dfProcessed = dfInput.join(dfConfig, "txn_type")
    
    // Write using custom wrapper
    dfProcessed.customWrite.writeToPath("/data/processed/output")
    dfProcessed.customWrite.writeToTable("analytics", "processed_txns")
    
    spark.stop()
  }
}


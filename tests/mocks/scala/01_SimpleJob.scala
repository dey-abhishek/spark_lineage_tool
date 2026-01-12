// Simple Scala Spark job
package com.example.lineage

import org.apache.spark.sql.SparkSession

object SimpleJob {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      .appName("SimpleScalaJob")
      .getOrCreate()
    
    // Read
    val df = spark.read.parquet("/data/raw/users")
    
    // Transform
    val filtered = df.filter($"age" > 18)
    
    // Write
    filtered.write.mode("overwrite").parquet("/data/processed/adults")
    
    spark.stop()
  }
}


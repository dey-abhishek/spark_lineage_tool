package com.company.pipeline

import org.apache.spark.sql.{SparkSession, SaveMode}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.ml.feature.{VectorAssembler, StandardScaler}
import org.apache.spark.ml.clustering.KMeans

/**
 * Comprehensive Pipeline Stage 4: Advanced Scala Spark Analytics
 * ML-driven customer segmentation and predictive analytics
 */
object ComprehensiveAnalytics {
  
  def main(args: Array[String]): Unit = {
    val runDate = if (args.length > 0) args(0) else java.time.LocalDate.now().toString
    val env = if (args.length > 1) args(1) else "prod"
    
    val spark = SparkSession.builder()
      .appName(s"Comprehensive Analytics - $env - $runDate")
      .enableHiveSupport()
      .config("spark.sql.adaptive.enabled", "true")
      .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
      .getOrCreate()
    
    import spark.implicits._
    
    println(s"═══════════════════════════════════════════════════════════════")
    println(s"  Advanced Analytics Pipeline - $runDate")
    println(s"═══════════════════════════════════════════════════════════════")
    
    // Read Gold Layer Data
    println("\n[1/5] Loading gold layer data...")
    val customerMasterDF = spark.table(s"gold_$env.customer_master")
    val transactionSummaryDF = spark.table(s"gold_$env.transaction_summary")
    val productPerformanceDF = spark.table(s"gold_$env.product_performance")
    
    // Analytics 1: Customer Segmentation using K-Means
    println("[2/5] Performing customer segmentation...")
    
    val featureDF = customerMasterDF
      .select(
        "customer_id",
        "total_transactions",
        "total_revenue",
        "avg_transaction_value",
        "event_count"
      )
      .na.fill(0.0)
    
    val assembler = new VectorAssembler()
      .setInputCols(Array("total_transactions", "total_revenue", "avg_transaction_value", "event_count"))
      .setOutputCol("features")
    
    val assembledDF = assembler.transform(featureDF)
    
    val scaler = new StandardScaler()
      .setInputCol("features")
      .setOutputCol("scaled_features")
      .setWithStd(true)
      .setWithMean(false)
    
    val scalerModel = scaler.fit(assembledDF)
    val scaledDF = scalerModel.transform(assembledDF)
    
    val kmeans = new KMeans()
      .setK(5)
      .setSeed(1L)
      .setFeaturesCol("scaled_features")
      .setPredictionCol("segment_id")
    
    val kmeansModel = kmeans.fit(scaledDF)
    val segmentedDF = kmeansModel.transform(scaledDF)
    
    val customerSegments = segmentedDF
      .select("customer_id", "segment_id")
      .withColumn("segment_name", 
        when($"segment_id" === 0, "High Value")
        .when($"segment_id" === 1, "Medium Value")
        .when($"segment_id" === 2, "Growing")
        .when($"segment_id" === 3, "At Risk")
        .otherwise("New Customer"))
    
    customerSegments.write
      .mode(SaveMode.Overwrite)
      .saveAsTable(s"analytics_$env.customer_segments")
    
    // Analytics 2: Product Affinity Analysis
    println("[3/5] Analyzing product affinity...")
    
    val productAffinity = transactionSummaryDF
      .groupBy("customer_id")
      .agg(
        collect_list("product_id").as("purchased_products"),
        sum("daily_transaction_count").as("total_purchases"),
        avg("avg_transaction_amount").as("avg_spend_per_transaction")
      )
    
    productAffinity.write
      .mode(SaveMode.Overwrite)
      .saveAsTable(s"analytics_$env.product_affinity")
    
    // Analytics 3: Time Series Analysis
    println("[4/5] Creating time series trends...")
    
    val windowSpec = Window
      .partitionBy("customer_id")
      .orderBy("transaction_date")
      .rowsBetween(-6, 0)
    
    val trendAnalysis = transactionSummaryDF
      .withColumn("revenue_7day_avg", avg("daily_revenue").over(windowSpec))
      .withColumn("transaction_7day_avg", avg("daily_transaction_count").over(windowSpec))
      .withColumn("revenue_trend",
        when($"daily_revenue" > $"revenue_7day_avg" * 1.2, "Growing")
        .when($"daily_revenue" < $"revenue_7day_avg" * 0.8, "Declining")
        .otherwise("Stable"))
    
    trendAnalysis.write
      .mode(SaveMode.Overwrite)
      .partitionBy("transaction_date")
      .saveAsTable(s"analytics_$env.customer_trends")
    
    // Analytics 4: Churn Prediction Features
    println("[5/5] Creating churn prediction features...")
    
    val daysSinceLastTransaction = datediff(
      to_date(lit(runDate)),
      col("last_transaction_date")
    )
    
    val churnFeatures = customerMasterDF
      .withColumn("days_since_last_transaction", daysSinceLastTransaction)
      .withColumn("transaction_frequency", 
        $"total_transactions" / greatest(daysSinceLastTransaction, lit(1)))
      .withColumn("churn_risk_score",
        when($"days_since_last_transaction" > 90, lit(0.9))
        .when($"days_since_last_transaction" > 60, lit(0.7))
        .when($"days_since_last_transaction" > 30, lit(0.5))
        .when($"transaction_frequency" < 0.1, lit(0.6))
        .otherwise(lit(0.2)))
      .withColumn("churn_category",
        when($"churn_risk_score" > 0.7, "High Risk")
        .when($"churn_risk_score" > 0.4, "Medium Risk")
        .otherwise("Low Risk"))
    
    churnFeatures.write
      .mode(SaveMode.Overwrite)
      .saveAsTable(s"analytics_$env.churn_prediction")
    
    // Export Results
    println("\nExporting analytics results...")
    
    customerSegments.write
      .mode(SaveMode.Overwrite)
      .option("header", "true")
      .csv(s"/data/analytics_exports/$env/segments/$runDate")
    
    churnFeatures
      .filter($"churn_category" === "High Risk")
      .select("customer_id", "customer_name", "churn_risk_score", "days_since_last_transaction")
      .write
      .mode(SaveMode.Overwrite)
      .option("header", "true")
      .csv(s"/data/analytics_exports/$env/churn_alerts/$runDate")
    
    // Summary Statistics
    println("\n═══════════════════════════════════════════════════════════════")
    println("  Analytics Summary:")
    println("═══════════════════════════════════════════════════════════════")
    
    customerSegments.groupBy("segment_name").count().orderBy($"count".desc).show()
    
    churnFeatures.groupBy("churn_category").count().orderBy($"count".desc).show()
    
    spark.stop()
  }
}


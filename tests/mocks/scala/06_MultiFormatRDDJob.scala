// Scala Spark job with multiple formats, RDDs, UDFs, and Hive integration
package com.company.etl

import org.apache.spark.sql.{DataFrame, SparkSession, Row}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.expressions.UserDefinedFunction

object MultiFormatRDDJob {
  
  // ========== Define UDFs ==========
  
  def maskEmail(email: String): String = {
    if (email == null || !email.contains("@")) return email
    val parts = email.split("@")
    s"${"*" * (parts(0).length - 2)}${parts(0).takeRight(2)}@${parts(1)}"
  }
  
  def calculateTax(amount: Double, state: String): Double = {
    val taxRates = Map(
      "CA" -> 0.0725,
      "NY" -> 0.08875,
      "TX" -> 0.0625,
      "FL" -> 0.06
    )
    amount * taxRates.getOrElse(state, 0.05)
  }
  
  def validateCreditCard(cardNumber: String): Boolean = {
    if (cardNumber == null || !cardNumber.forall(_.isDigit)) return false
    // Simplified Luhn algorithm
    val digits = cardNumber.map(_.asDigit)
    val sum = digits.reverse.zipWithIndex.map { case (d, i) =>
      if (i % 2 == 1) {
        val doubled = d * 2
        if (doubled > 9) doubled - 9 else doubled
      } else d
    }.sum
    sum % 10 == 0
  }
  
  def parseJson(jsonStr: String): Map[String, String] = {
    import scala.util.parsing.json.JSON
    if (jsonStr == null) return Map.empty
    JSON.parseFull(jsonStr) match {
      case Some(m: Map[String, String] @unchecked) => m
      case _ => Map.empty
    }
  }
  
  def main(args: Array[String]): Unit = {
    val runDate = if (args.length > 0) args(0) else "2024-01-01"
    val env = if (args.length > 1) args(1) else "prod"
    
    val spark = SparkSession.builder()
      .appName("MultiFormatRDDJob")
      .config("spark.sql.avro.compression.codec", "snappy")
      .config("spark.sql.orc.compression.codec", "zlib")
      .enableHiveSupport()
      .getOrCreate()
    
    import spark.implicits._
    
    val basePath = s"/data/$env"
    val sc = spark.sparkContext
    
    // ========== Register UDFs ==========
    
    val maskEmailUDF = udf(maskEmail _)
    val calculateTaxUDF = udf(calculateTax _)
    val validateCCUDF = udf(validateCreditCard _)
    val parseJsonUDF = udf(parseJson _)
    
    spark.udf.register("mask_email", maskEmail _)
    spark.udf.register("calculate_tax", calculateTax _)
    
    // ========== READ: Multiple Formats ==========
    
    // 1. Read Parquet
    val dfParquet = spark.read
      .option("mergeSchema", "true")
      .parquet(s"$basePath/raw/transactions/$runDate/*.parquet")
    
    // 2. Read JSON
    val dfJson = spark.read
      .option("multiLine", "true")
      .json(s"$basePath/raw/events/$runDate/*.json")
    
    // 3. Read Avro
    val dfAvro = spark.read
      .format("avro")
      .load(s"$basePath/raw/users/$runDate/*.avro")
    
    // 4. Read ORC
    val dfOrc = spark.read
      .orc(s"$basePath/raw/products/$runDate/*.orc")
    
    // 5. Read CSV
    val dfCsv = spark.read
      .option("header", "true")
      .option("delimiter", "|")
      .option("inferSchema", "true")
      .csv(s"$basePath/raw/metadata/$runDate/*.csv")
    
    // 6. Read Text files
    val dfText = spark.read
      .text(s"$basePath/raw/logs/$runDate/*.log")
    
    // 7. Read Binary files
    val dfBinary = spark.read
      .format("binaryFile")
      .load(s"$basePath/raw/images/$runDate/*.jpg")
    
    // 8. Read from Hive tables
    val dfHiveCustomers = spark.table(s"${env}_warehouse.customers")
    val dfHiveOrders = spark.sql(s"SELECT * FROM ${env}_warehouse.orders WHERE order_date = '$runDate'")
    
    // ========== RDD Operations ==========
    
    // 9. Convert DataFrame to RDD
    val rddTransactions: RDD[(String, Double)] = dfParquet.rdd.map(row => 
      (row.getAs[String]("transaction_id"), row.getAs[Double]("amount"))
    )
    
    // 10. Read text as RDD
    val rddLogs: RDD[String] = sc.textFile(s"$basePath/raw/app_logs/$runDate/*.txt")
    
    // 11. RDD transformations
    val rddParsed = rddLogs
      .filter(_.contains("ERROR"))
      .map(_.split("\\|"))
      .filter(_.length >= 5)
      .map(parts => (parts(0), parts(1), parts(2), parts(3), parts(4)))
    
    // 12. Convert RDD to DataFrame
    val dfErrors = rddParsed.toDF("timestamp", "level", "component", "message", "stack_trace")
    
    // 13. Read sequence file
    val rddSequence = sc.sequenceFile[String, String](s"$basePath/raw/sequence/$runDate/part-*")
    
    // 14. Read object file
    val rddObjects = sc.objectFile[Any](s"$basePath/raw/pickled/$runDate/*.pickle")
    
    // ========== Apply UDFs ==========
    
    // Apply masking UDF
    val dfCustomersMasked = dfHiveCustomers
      .withColumn("email_masked", maskEmailUDF($"email"))
      .withColumn("phone_masked", lit("***-***-") + substring($"phone", -4, 4))
    
    // Apply tax calculation UDF
    val dfWithTax = dfParquet
      .withColumn("tax_amount", calculateTaxUDF($"amount", $"state"))
      .withColumn("total_with_tax", $"amount" + $"tax_amount")
    
    // Apply validation UDF
    val dfPayments = spark.read.parquet(s"$basePath/raw/payments/$runDate")
    val dfPaymentsValidated = dfPayments
      .withColumn("is_valid_card", validateCCUDF($"card_number"))
    
    // Parse JSON with UDF
    val dfEventsParsed = dfJson
      .withColumn("payload_map", parseJsonUDF($"payload"))
    
    // ========== Complex Joins ==========
    
    val dfEnriched = dfWithTax
      .join(dfHiveCustomers, Seq("customer_id"), "left")
      .join(dfHiveOrders, Seq("order_id"), "left")
      .join(dfAvro, Seq("user_id"), "left")
      .join(dfOrc, Seq("product_id"), "left")
    
    // ========== Use UDFs in SQL ==========
    
    dfEnriched.createOrReplaceTempView("enriched_data")
    
    val dfSqlResult = spark.sql(s"""
      SELECT 
        customer_id,
        transaction_id,
        amount,
        calculate_tax(amount, state) as tax,
        mask_email(email) as masked_email,
        transaction_date
      FROM enriched_data
      WHERE transaction_date = '$runDate'
    """)
    
    // ========== WRITE: Multiple Formats ==========
    
    // 15. Write Parquet with partitioning
    dfEnriched.write
      .mode("overwrite")
      .partitionBy("run_date", "region")
      .parquet(s"$basePath/processed/enriched_transactions/$runDate")
    
    // 16. Write JSON
    dfJson.write
      .mode("append")
      .json(s"$basePath/processed/events_json/$runDate")
    
    // 17. Write Avro
    dfAvro.write
      .format("avro")
      .mode("overwrite")
      .save(s"$basePath/processed/users_avro/$runDate")
    
    // 18. Write ORC
    dfOrc.write
      .option("compression", "zlib")
      .mode("overwrite")
      .orc(s"$basePath/processed/products_orc/$runDate")
    
    // 19. Write CSV
    dfCsv.write
      .option("header", "true")
      .option("delimiter", ",")
      .mode("overwrite")
      .csv(s"$basePath/processed/metadata_csv/$runDate")
    
    // 20. Write Text
    dfText.write
      .mode("overwrite")
      .text(s"$basePath/processed/logs_text/$runDate")
    
    // 21. Write to Hive table
    dfEnriched.write
      .mode("overwrite")
      .saveAsTable(s"${env}_analytics.enriched_transactions")
    
    // 22. Insert into Hive table
    dfEnriched.write
      .mode("append")
      .insertInto(s"${env}_analytics.daily_transactions")
    
    // 23. Create Hive table with specific format
    dfEnriched.write
      .mode("overwrite")
      .format("orc")
      .option("path", s"$basePath/warehouse/transactions_orc")
      .saveAsTable(s"${env}_warehouse.transactions_orc")
    
    // ========== RDD Output ==========
    
    // 24. Save RDD as text
    val rddFormatted = rddParsed.map { case (ts, lvl, comp, msg, stack) =>
      s"$ts,$lvl,$comp,$msg,$stack"
    }
    rddFormatted.saveAsTextFile(s"$basePath/processed/error_logs/$runDate")
    
    // 25. Save as sequence file
    val rddKeyValue = rddTransactions.map { case (id, amt) => (id, amt.toString) }
    rddKeyValue.saveAsSequenceFile(s"$basePath/processed/transactions_seq/$runDate")
    
    // 26. Save as object file
    val rddObjectsFiltered = rddObjects.filter(_ != null)
    rddObjectsFiltered.saveAsObjectFile(s"$basePath/processed/objects/$runDate")
    
    // ========== Mixed Format Output ==========
    
    val dfSummary = dfEnriched
      .groupBy("customer_id", "state")
      .agg(
        count("*").as("transaction_count"),
        sum("total_with_tax").as("total_spent"),
        avg("total_with_tax").as("avg_transaction")
      )
    
    // Write to multiple formats
    dfSummary.write.mode("overwrite").parquet(s"$basePath/reports/summary.parquet")
    dfSummary.write.mode("overwrite").json(s"$basePath/reports/summary.json")
    dfSummary.write.mode("overwrite").format("avro").save(s"$basePath/reports/summary.avro")
    dfSummary.write.mode("overwrite").orc(s"$basePath/reports/summary.orc")
    
    // Write to Hive
    dfSummary.write.mode("overwrite").saveAsTable(s"${env}_reports.transaction_summary")
    
    // Write with UDF-transformed data
    dfSqlResult.write
      .mode("overwrite")
      .partitionBy("transaction_date")
      .format("orc")
      .saveAsTable(s"${env}_analytics.transactions_with_udfs")
    
    spark.stop()
  }
}


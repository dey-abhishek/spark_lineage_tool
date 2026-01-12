// Modularized Scala Spark job with JDBC sources and external dependencies
package com.company.etl

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger
import java.util.Properties

// Import custom packages (demonstrates modularization)
import com.company.etl.readers.{JDBCReader, SFTPReader, RestAPIReader}
import com.company.etl.writers.{JDBCWriter, HDFSWriter, DeltaWriter}
import com.company.etl.transformers.{DataQualityValidator, BusinessRuleEngine}
import com.company.etl.utils.{ConfigManager, SecretVault, AuditLogger}
import com.company.etl.schemas.SchemaRegistry

object ModularEnterpriseETL {
  
  case class ETLConfig(
    runDate: String,
    environment: String,
    oracleHost: String,
    postgresHost: String,
    mysqlHost: String,
    sftpHost: String,
    basePath: String
  )
  
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("ModularEnterpriseETL")
      .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
      .config("spark.sql.adaptive.enabled", "true")
      .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
      .enableHiveSupport()
      .getOrCreate()
    
    import spark.implicits._
    
    // Load configuration
    val configManager = new ConfigManager(args(0))
    val config = configManager.loadETLConfig[ETLConfig]()
    val secretVault = new SecretVault(configManager.getVaultUrl())
    val auditLogger = new AuditLogger(spark)
    
    // Initialize readers
    val jdbcReader = new JDBCReader(spark, secretVault)
    val sftpReader = new SFTPReader(secretVault)
    
    try {
      auditLogger.logStart("ModularEnterpriseETL", config.runDate)
      
      // Read from Oracle
      val oracleProps = new Properties()
      oracleProps.put("user", secretVault.getSecret("oracle/username"))
      oracleProps.put("password", secretVault.getSecret("oracle/password"))
      oracleProps.put("driver", "oracle.jdbc.OracleDriver")
      oracleProps.put("fetchsize", "10000")
      
      val dfCustomers = spark.read
        .jdbc(
          s"jdbc:oracle:thin:@${config.oracleHost}:1521:ORCL",
          "(SELECT * FROM CUSTOMER_MASTER WHERE UPDATED_DATE >= TO_DATE(?, 'YYYY-MM-DD'))",
          oracleProps
        )
      
      auditLogger.logDatasetRead("oracle.customer_master", dfCustomers.count())
      
      // Read from PostgreSQL with custom query
      val postgresUrl = s"jdbc:postgresql://${config.postgresHost}:5432/analytics"
      val postgresQuery = s"""
        (SELECT 
          t.transaction_id,
          t.customer_id,
          t.product_id,
          t.amount,
          t.transaction_timestamp,
          p.product_category,
          p.product_subcategory
        FROM transactions.fact_transactions t
        JOIN products.dim_products p ON t.product_id = p.product_id
        WHERE DATE(t.transaction_timestamp) >= '${config.runDate}'
        ) as txn_data
      """
      
      val postgresProps = new Properties()
      postgresProps.put("user", secretVault.getSecret("postgres/username"))
      postgresProps.put("password", secretVault.getSecret("postgres/password"))
      postgresProps.put("driver", "org.postgresql.Driver")
      
      val dfTransactions = jdbcReader.readWithRetry(
        postgresUrl,
        postgresQuery,
        postgresProps,
        maxRetries = 3
      )
      
      auditLogger.logDatasetRead("postgres.transactions", dfTransactions.count())
      
      // Read from MySQL
      val mysqlUrl = s"jdbc:mysql://${config.mysqlHost}:3306/ecommerce"
      val dfProducts = spark.read
        .format("jdbc")
        .option("url", mysqlUrl)
        .option("dbtable", "products.catalog")
        .option("user", secretVault.getSecret("mysql/username"))
        .option("password", secretVault.getSecret("mysql/password"))
        .option("driver", "com.mysql.cj.jdbc.Driver")
        .option("numPartitions", "10")
        .load()
      
      // Read from SFTP
      val sftpFiles = sftpReader.downloadFiles(
        host = config.sftpHost,
        remotePath = s"/data/exports/${config.runDate}/",
        localPath = s"/tmp/sftp_staging/${config.runDate}/",
        filePattern = "*.parquet"
      )
      
      val dfExternalData = spark.read
        .parquet(s"/tmp/sftp_staging/${config.runDate}/*.parquet")
      
      auditLogger.logDatasetRead("sftp.external_data", dfExternalData.count())
      
      // Read from REST API (custom reader)
      val apiReader = new RestAPIReader(spark, secretVault)
      val dfEnrichmentData = apiReader.fetchData(
        endpoint = s"https://api.company.com/v1/enrichment/${config.runDate}",
        authToken = secretVault.getSecret("api/token"),
        schema = SchemaRegistry.getSchema("enrichment_v2")
      )
      
      // Apply data quality checks
      val validator = new DataQualityValidator()
      val dfCustomersValidated = validator.validateAndFlag(
        dfCustomers,
        rules = Seq(
          "customer_id IS NOT NULL",
          "email RLIKE '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}$'",
          "registration_date <= CURRENT_DATE"
        )
      )
      
      val dfTransactionsValidated = validator.validateAndFlag(
        dfTransactions,
        rules = Seq(
          "transaction_id IS NOT NULL",
          "amount > 0",
          "transaction_timestamp IS NOT NULL"
        )
      )
      
      // Complex join logic
      val dfEnriched = dfTransactionsValidated
        .join(dfCustomersValidated, Seq("customer_id"), "left")
        .join(dfProducts, Seq("product_id"), "left")
        .join(dfExternalData, Seq("customer_id"), "left")
        .join(broadcast(dfEnrichmentData), Seq("customer_id"), "left")
      
      // Apply business rules
      val ruleEngine = new BusinessRuleEngine()
      val dfWithBusinessRules = ruleEngine.applyRules(
        dfEnriched,
        rulesPath = s"${config.basePath}/config/business_rules.json"
      )
      
      // Write to multiple destinations
      
      // 1. Write to Delta Lake
      val deltaWriter = new DeltaWriter(spark)
      deltaWriter.mergeData(
        sourceDf = dfWithBusinessRules,
        targetPath = s"${config.basePath}/delta/customer_360",
        mergeKey = "customer_id",
        updateCondition = "source.transaction_timestamp > target.last_transaction_timestamp"
      )
      
      // 2. Write to HDFS (Parquet with partitioning)
      dfWithBusinessRules.write
        .mode("overwrite")
        .partitionBy("run_date", "product_category")
        .parquet(s"${config.basePath}/warehouse/enriched_transactions/${config.runDate}")
      
      // 3. Write to Hive table
      dfWithBusinessRules.write
        .mode("overwrite")
        .insertInto(s"${config.environment}_analytics.enriched_transactions")
      
      // 4. Write aggregated metrics back to PostgreSQL
      val dfMetrics = dfWithBusinessRules
        .groupBy("customer_id", "product_category")
        .agg(
          count("transaction_id").as("transaction_count"),
          sum("amount").as("total_amount"),
          avg("amount").as("avg_transaction_value"),
          max("transaction_timestamp").as("last_transaction_date")
        )
      
      val jdbcWriter = new JDBCWriter(secretVault)
      jdbcWriter.writeToPostgres(
        df = dfMetrics,
        url = postgresUrl,
        table = "analytics.customer_metrics_daily",
        mode = "append"
      )
      
      // 5. Write to MySQL for operational reporting
      dfMetrics
        .filter($"transaction_count" > 10)
        .write
        .mode("overwrite")
        .jdbc(
          mysqlUrl,
          "reporting.high_value_customers",
          connectionProperties = new Properties() {
            put("user", secretVault.getSecret("mysql/username"))
            put("password", secretVault.getSecret("mysql/password"))
            put("driver", "com.mysql.cj.jdbc.Driver")
          }
        )
      
      // Streaming enrichment (read from Kafka, enrich with JDBC, write to Delta)
      val dfKafka = spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", "kafka1:9092,kafka2:9092")
        .option("subscribe", "realtime_events")
        .option("startingOffsets", "latest")
        .load()
      
      val dfKafkaParsed = dfKafka
        .select(from_json($"value".cast("string"), SchemaRegistry.getSchema("realtime_event")).as("data"))
        .select("data.*")
      
      // Enrich streaming data with cached JDBC lookup
      dfCustomersValidated.createOrReplaceTempView("customer_lookup")
      
      val dfStreamEnriched = dfKafkaParsed.join(
        broadcast(dfCustomersValidated),
        Seq("customer_id"),
        "left"
      )
      
      val streamingQuery = dfStreamEnriched.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", s"${config.basePath}/checkpoints/realtime_events")
        .trigger(Trigger.ProcessingTime("30 seconds"))
        .start(s"${config.basePath}/delta/realtime_enriched_events")
      
      auditLogger.logSuccess("ModularEnterpriseETL", config.runDate)
      
      // Wait for streaming to run (or remove this for batch-only)
      // streamingQuery.awaitTermination()
      
    } catch {
      case e: Exception =>
        auditLogger.logFailure("ModularEnterpriseETL", config.runDate, e.getMessage)
        throw e
    } finally {
      spark.stop()
    }
  }
}

// Companion object with helper methods
object JDBCHelpers {
  
  def createConnectionProperties(username: String, password: String, driver: String): Properties = {
    val props = new Properties()
    props.put("user", username)
    props.put("password", password)
    props.put("driver", driver)
    props
  }
  
  def getOracleUrl(host: String, port: Int, sid: String): String = {
    s"jdbc:oracle:thin:@$host:$port:$sid"
  }
  
  def getPostgresUrl(host: String, port: Int, database: String): String = {
    s"jdbc:postgresql://$host:$port/$database"
  }
  
  def getMySQLUrl(host: String, port: Int, database: String): String = {
    s"jdbc:mysql://$host:$port/$database"
  }
}


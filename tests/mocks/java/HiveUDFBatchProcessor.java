package com.company.batch;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.conf.Configuration;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;

/**
 * Batch processor that creates Hive UDFs and runs ETL jobs.
 * Demonstrates UDF registration, data reading/writing, and HDFS operations.
 */
public class HiveUDFBatchProcessor {
    
    private static final String HDFS_INPUT = "hdfs://prod/data/raw/transactions";
    private static final String HDFS_OUTPUT = "hdfs://prod/data/processed/transactions_clean";
    private static final String HIVE_DB = "analytics";
    private static final String HIVE_TABLE = "transactions_clean";
    
    public static void main(String[] args) throws Exception {
        String runDate = args.length > 0 ? args[0] : "2024-01-15";
        String environment = args.length > 1 ? args[1] : "prod";
        
        // HDFS operations
        Configuration conf = new Configuration();
        conf.addResource(new Path("/etc/hadoop/conf/hdfs-site.xml"));
        FileSystem fs = FileSystem.get(conf);
        
        // Check if input exists
        Path inputPath = new Path(HDFS_INPUT + "/" + runDate);
        if (!fs.exists(inputPath)) {
            System.err.println("Input path does not exist: " + inputPath);
            System.exit(1);
        }
        
        // Create output directory
        Path outputPath = new Path(HDFS_OUTPUT + "/" + runDate);
        if (fs.exists(outputPath)) {
            fs.delete(outputPath, true);
        }
        fs.mkdirs(outputPath);
        
        // Connect to Hive
        Class.forName("org.apache.hive.jdbc.HiveDriver");
        Connection hiveConn = DriverManager.getConnection(
            "jdbc:hive2://localhost:10000/default", "hive", ""
        );
        Statement stmt = hiveConn.createStatement();
        
        // Register UDFs
        stmt.execute("CREATE TEMPORARY FUNCTION clean_string AS 'com.company.etl.udfs.StringCleanerUDF'");
        stmt.execute("CREATE TEMPORARY FUNCTION parse_date AS 'com.company.etl.udfs.DateParserUDF'");
        stmt.execute("CREATE TEMPORARY FUNCTION calc_stats AS 'com.company.etl.udafs.AggregateStatsUDAF'");
        
        // Create external table pointing to HDFS input
        String createTableSQL = String.format(
            "CREATE EXTERNAL TABLE IF NOT EXISTS %s.raw_transactions_%s (" +
            "  transaction_id STRING, " +
            "  customer_name STRING, " +
            "  transaction_date STRING, " +
            "  amount DOUBLE " +
            ") " +
            "STORED AS PARQUET " +
            "LOCATION '%s'",
            HIVE_DB, runDate.replace("-", ""), inputPath.toString()
        );
        stmt.execute(createTableSQL);
        
        // Process data with UDFs
        String processSQL = String.format(
            "INSERT OVERWRITE DIRECTORY '%s' " +
            "STORED AS PARQUET " +
            "SELECT " +
            "  transaction_id, " +
            "  clean_string(customer_name) as customer_name_clean, " +
            "  parse_date(transaction_date, 'MM/dd/yyyy') as transaction_date_std, " +
            "  amount " +
            "FROM %s.raw_transactions_%s " +
            "WHERE transaction_date IS NOT NULL",
            outputPath.toString(), HIVE_DB, runDate.replace("-", "")
        );
        stmt.execute(processSQL);
        
        // Create final Hive table
        String finalTableSQL = String.format(
            "CREATE TABLE IF NOT EXISTS %s.%s (" +
            "  transaction_id STRING, " +
            "  customer_name_clean STRING, " +
            "  transaction_date_std STRING, " +
            "  amount DOUBLE " +
            ") " +
            "PARTITIONED BY (run_date STRING) " +
            "STORED AS PARQUET",
            HIVE_DB, HIVE_TABLE
        );
        stmt.execute(finalTableSQL);
        
        // Load processed data into final table
        String loadSQL = String.format(
            "LOAD DATA INPATH '%s' " +
            "INTO TABLE %s.%s " +
            "PARTITION (run_date='%s')",
            outputPath.toString(), HIVE_DB, HIVE_TABLE, runDate
        );
        stmt.execute(loadSQL);
        
        // Generate statistics with UDAF
        String statsSQL = String.format(
            "INSERT OVERWRITE TABLE %s.transaction_stats " +
            "PARTITION (run_date='%s') " +
            "SELECT " +
            "  customer_name_clean, " +
            "  calc_stats(amount) as stats " +
            "FROM %s.%s " +
            "WHERE run_date = '%s' " +
            "GROUP BY customer_name_clean",
            HIVE_DB, runDate, HIVE_DB, HIVE_TABLE, runDate
        );
        stmt.execute(statsSQL);
        
        // Cleanup
        stmt.close();
        hiveConn.close();
        fs.close();
        
        System.out.println("Batch processing completed for " + runDate);
    }
}


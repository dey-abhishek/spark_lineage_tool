package com.company.spark.udfs;

import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

/**
 * Spark Java UDF to transform strings.
 * 
 * Usage in PySpark:
 *   spark.udf.registerJavaFunction("transform_string", 
 *                                  "com.company.spark.udfs.SparkStringTransformUDF", 
 *                                  StringType())
 */
public class SparkStringTransformUDF implements UDF1<String, String> {
    
    @Override
    public String call(String input) throws Exception {
        if (input == null) {
            return null;
        }
        return input.toUpperCase().replaceAll("[^A-Z0-9]", "_");
    }
    
    /**
     * Example Spark job using this UDF
     */
    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
            .appName("UDF Example")
            .getOrCreate();
        
        // Register UDF
        spark.udf().register("transform_string", new SparkStringTransformUDF(), DataTypes.StringType);
        
        // Read data
        Dataset<Row> inputData = spark.read()
            .parquet("hdfs://data/raw/customer_names.parquet");
        
        // Apply UDF
        inputData.createOrReplaceTempView("customers");
        Dataset<Row> transformed = spark.sql(
            "SELECT customer_id, transform_string(customer_name) as clean_name FROM customers"
        );
        
        // Write output
        transformed.write()
            .mode("overwrite")
            .parquet("hdfs://data/processed/customer_names_clean.parquet");
        
        spark.stop();
    }
}


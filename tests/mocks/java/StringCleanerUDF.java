package com.company.etl.udfs;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

/**
 * Hive UDF to clean and standardize string values.
 * 
 * Usage in Hive:
 *   CREATE TEMPORARY FUNCTION clean_string AS 'com.company.etl.udfs.StringCleanerUDF';
 *   SELECT clean_string(customer_name) FROM raw.customers;
 */
public class StringCleanerUDF extends UDF {
    
    public Text evaluate(Text input) {
        if (input == null) {
            return null;
        }
        
        String cleaned = input.toString()
            .trim()
            .toLowerCase()
            .replaceAll("[^a-z0-9\\s]", "")
            .replaceAll("\\s+", " ");
        
        return new Text(cleaned);
    }
    
    public Text evaluate(Text input, Text defaultValue) {
        Text result = evaluate(input);
        return (result == null || result.toString().isEmpty()) ? defaultValue : result;
    }
}


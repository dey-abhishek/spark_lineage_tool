package com.company.etl.udfs;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Hive UDF to parse various date formats into standard format.
 * 
 * Usage:
 *   CREATE TEMPORARY FUNCTION parse_date AS 'com.company.etl.udfs.DateParserUDF';
 *   SELECT parse_date(order_date, 'MM/dd/yyyy') FROM raw.orders;
 */
public class DateParserUDF extends UDF {
    
    public String evaluate(Text dateString, Text formatPattern) {
        if (dateString == null || formatPattern == null) {
            return null;
        }
        
        try {
            SimpleDateFormat inputFormat = new SimpleDateFormat(formatPattern.toString());
            SimpleDateFormat outputFormat = new SimpleDateFormat("yyyy-MM-dd");
            
            Date date = inputFormat.parse(dateString.toString());
            return outputFormat.format(date);
        } catch (Exception e) {
            return null;
        }
    }
    
    public String evaluate(Text dateString) {
        // Default format
        return evaluate(dateString, new Text("yyyy-MM-dd"));
    }
}


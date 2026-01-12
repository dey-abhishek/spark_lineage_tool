-- Complex Hive script with UDFs, custom SerDes, and external tables
-- This demonstrates real-world data processing with custom serialization

-- Create temporary function for data transformation
CREATE TEMPORARY FUNCTION parse_json AS 'org.apache.hive.hcatalog.data.JsonSerDe';
CREATE TEMPORARY FUNCTION decrypt_pii AS 'com.company.udf.DecryptUDF';
CREATE TEMPORARY FUNCTION mask_sensitive AS 'com.company.udf.MaskingUDF';

-- External table with custom SerDe for JSON data
CREATE EXTERNAL TABLE IF NOT EXISTS raw_events_json (
    event_id STRING,
    user_id STRING,
    event_type STRING,
    payload STRING,
    event_timestamp BIGINT
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
WITH SERDEPROPERTIES (
    "ignore.malformed.json" = "true",
    "dots.in.keys" = "true"
)
STORED AS TEXTFILE
LOCATION '/data/raw/events_json/${runDate}';

-- External table with Avro SerDe
CREATE EXTERNAL TABLE IF NOT EXISTS raw_transactions_avro (
    transaction_id STRING,
    account_id STRING,
    amount DECIMAL(18,2),
    currency STRING,
    transaction_time TIMESTAMP
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.avro.AvroSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat'
LOCATION '/data/raw/transactions_avro/${runDate}'
TBLPROPERTIES ('avro.schema.url'='hdfs:///schemas/transaction_v2.avsc');

-- External table with ORC and custom compression
CREATE EXTERNAL TABLE IF NOT EXISTS processed_user_events (
    user_id STRING,
    event_count BIGINT,
    last_event_date DATE,
    encrypted_email STRING,
    masked_phone STRING
)
STORED AS ORC
LOCATION '/data/processed/user_events'
TBLPROPERTIES (
    "orc.compress"="SNAPPY",
    "orc.bloom.filter.columns"="user_id",
    "orc.create.index"="true"
);

-- Complex transformation with UDFs
INSERT OVERWRITE TABLE processed_user_events
PARTITION(event_date='${runDate}')
SELECT 
    e.user_id,
    COUNT(DISTINCT e.event_id) as event_count,
    MAX(FROM_UNIXTIME(e.event_timestamp)) as last_event_date,
    -- Use custom UDF to decrypt PII
    decrypt_pii(u.encrypted_email, '${encryption_key}') as encrypted_email,
    -- Use custom UDF to mask sensitive data
    mask_sensitive(u.phone_number, 'PHONE') as masked_phone
FROM raw_events_json e
INNER JOIN analytics.users u ON e.user_id = u.user_id
WHERE e.event_type IN ('login', 'purchase', 'view')
    AND FROM_UNIXTIME(e.event_timestamp) >= '${startDate}'
    AND FROM_UNIXTIME(e.event_timestamp) < '${endDate}'
GROUP BY 
    e.user_id,
    decrypt_pii(u.encrypted_email, '${encryption_key}'),
    mask_sensitive(u.phone_number, 'PHONE');

-- Create table with RegexSerDe for parsing log files
CREATE EXTERNAL TABLE IF NOT EXISTS raw_application_logs (
    log_timestamp STRING,
    log_level STRING,
    thread_name STRING,
    class_name STRING,
    message STRING
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.RegexSerDe'
WITH SERDEPROPERTIES (
    "input.regex" = "(\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2})\\s+(\\w+)\\s+\\[([^\\]]+)\\]\\s+([^:]+):\\s+(.*)",
    "output.format.string" = "%1$s %2$s %3$s %4$s %5$s"
)
STORED AS TEXTFILE
LOCATION '/data/raw/logs/${app_name}/${runDate}';

-- Aggregate logs into summary table
INSERT OVERWRITE TABLE analytics.application_log_summary
PARTITION(log_date='${runDate}', app_name='${app_name}')
SELECT 
    log_level,
    COUNT(*) as log_count,
    COLLECT_SET(class_name) as affected_classes,
    MIN(log_timestamp) as first_occurrence,
    MAX(log_timestamp) as last_occurrence
FROM raw_application_logs
WHERE log_level IN ('ERROR', 'FATAL', 'WARN')
GROUP BY log_level;

-- Table with custom InputFormat/OutputFormat
CREATE EXTERNAL TABLE IF NOT EXISTS raw_parquet_encrypted (
    record_id STRING,
    encrypted_payload BINARY,
    checksum STRING
)
STORED AS INPUTFORMAT 'com.company.hadoop.EncryptedParquetInputFormat'
OUTPUTFORMAT 'com.company.hadoop.EncryptedParquetOutputFormat'
LOCATION '/data/secure/encrypted_records/${runDate}';

-- Multi-format aggregation with UDFs
CREATE TABLE analytics.user_activity_summary
STORED AS PARQUET
LOCATION '/data/warehouse/user_activity_summary'
AS
SELECT 
    a.user_id,
    COUNT(DISTINCT a.session_id) as session_count,
    SUM(t.amount) as total_transaction_amount,
    -- Custom UDF for geohashing
    com.company.udf.GeoHashUDF(a.latitude, a.longitude, 7) as location_hash,
    -- Custom UDAF for percentile calculation
    com.company.udaf.PercentileUDAF(a.session_duration, 0.95) as p95_session_duration,
    MAX(e.last_event_date) as last_activity_date
FROM raw_events_json a
LEFT JOIN raw_transactions_avro t ON a.user_id = t.account_id
LEFT JOIN processed_user_events e ON a.user_id = e.user_id
WHERE a.event_type = 'session_end'
GROUP BY 
    a.user_id,
    com.company.udf.GeoHashUDF(a.latitude, a.longitude, 7);

-- Cleanup temporary functions
DROP TEMPORARY FUNCTION IF EXISTS parse_json;
DROP TEMPORARY FUNCTION IF EXISTS decrypt_pii;
DROP TEMPORARY FUNCTION IF EXISTS mask_sensitive;


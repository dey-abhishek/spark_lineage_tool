-- Hive script with various custom file formats and storage handlers
-- Demonstrates integration with HBase, Kafka, and custom storage

-- HBase-backed table
CREATE EXTERNAL TABLE IF NOT EXISTS hbase_user_profiles (
    user_id STRING,
    profile_data MAP<STRING, STRING>,
    preferences MAP<STRING, STRING>,
    last_updated TIMESTAMP
)
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES (
    "hbase.columns.mapping" = ":key,profile:data,profile:preferences,profile:updated",
    "hbase.table.default.storage.type" = "binary"
)
TBLPROPERTIES (
    "hbase.table.name" = "user_profiles",
    "hbase.mapred.output.outputtable" = "user_profiles"
);

-- Read from HBase and write to HDFS
INSERT OVERWRITE DIRECTORY '/data/exports/user_profiles/${runDate}'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
SELECT 
    user_id,
    profile_data['name'] as name,
    profile_data['email'] as email,
    preferences['notification_enabled'] as notifications,
    last_updated
FROM hbase_user_profiles
WHERE last_updated >= '${startDate}';

-- Kafka-backed table (requires custom storage handler)
CREATE EXTERNAL TABLE IF NOT EXISTS kafka_clickstream (
    event_id STRING,
    user_id STRING,
    page_url STRING,
    referrer_url STRING,
    event_timestamp BIGINT,
    session_id STRING
)
STORED BY 'org.apache.hadoop.hive.kafka.KafkaStorageHandler'
TBLPROPERTIES (
    "kafka.topic" = "clickstream_events",
    "kafka.bootstrap.servers" = "kafka1:9092,kafka2:9092,kafka3:9092",
    "kafka.serde.class" = "org.apache.hadoop.hive.serde2.JsonSerDe"
);

-- Process Kafka stream and materialize to HDFS
INSERT OVERWRITE TABLE analytics.hourly_clickstream_summary
PARTITION(event_hour='${runHour}')
SELECT 
    page_url,
    COUNT(DISTINCT user_id) as unique_visitors,
    COUNT(DISTINCT session_id) as unique_sessions,
    COUNT(*) as total_clicks,
    AVG(CASE WHEN referrer_url IS NOT NULL THEN 1 ELSE 0 END) as referral_rate
FROM kafka_clickstream
WHERE FROM_UNIXTIME(event_timestamp, 'yyyy-MM-dd HH') = '${runHour}'
GROUP BY page_url;

-- Elasticsearch-backed table
CREATE EXTERNAL TABLE IF NOT EXISTS elasticsearch_products (
    product_id STRING,
    product_name STRING,
    category STRING,
    price DOUBLE,
    tags ARRAY<STRING>,
    attributes MAP<STRING, STRING>
)
STORED BY 'org.elasticsearch.hadoop.hive.EsStorageHandler'
TBLPROPERTIES (
    "es.resource" = "products/product",
    "es.nodes" = "es-node1,es-node2,es-node3",
    "es.port" = "9200",
    "es.query" = "?q=category:electronics"
);

-- Query Elasticsearch and join with Hive data
INSERT OVERWRITE TABLE analytics.product_performance
SELECT 
    p.product_id,
    p.product_name,
    p.category,
    p.price,
    COALESCE(s.total_sales, 0) as total_sales,
    COALESCE(s.total_revenue, 0.0) as total_revenue,
    COALESCE(r.avg_rating, 0.0) as avg_rating,
    COALESCE(r.review_count, 0) as review_count
FROM elasticsearch_products p
LEFT JOIN analytics.daily_sales_summary s ON p.product_id = s.product_id
LEFT JOIN analytics.product_reviews r ON p.product_id = r.product_id
WHERE s.sale_date >= DATE_SUB('${runDate}', 30);

-- Druid-backed table for real-time analytics
CREATE EXTERNAL TABLE IF NOT EXISTS druid_realtime_metrics (
    metric_timestamp TIMESTAMP,
    metric_name STRING,
    metric_value DOUBLE,
    dimensions MAP<STRING, STRING>
)
STORED BY 'org.apache.hadoop.hive.druid.DruidStorageHandler'
TBLPROPERTIES (
    "druid.datasource" = "metrics_datasource",
    "druid.broker.address.default" = "druid-broker:8082"
);

-- Custom Parquet with dictionary encoding
CREATE TABLE analytics.optimized_fact_table (
    fact_id BIGINT,
    dimension_key STRING,
    metric_value DECIMAL(20,6),
    event_date DATE
)
PARTITIONED BY (year INT, month INT)
STORED AS PARQUET
LOCATION '/data/warehouse/optimized_facts'
TBLPROPERTIES (
    "parquet.compression" = "SNAPPY",
    "parquet.enable.dictionary" = "true",
    "parquet.page.size" = "1048576",
    "parquet.block.size" = "134217728"
);

-- LZO compressed text with index
CREATE EXTERNAL TABLE IF NOT EXISTS raw_logs_lzo (
    log_line STRING
)
STORED AS INPUTFORMAT 'com.hadoop.mapred.DeprecatedLzoTextInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION '/data/raw/logs_lzo/${runDate}';

-- Process LZO logs
INSERT OVERWRITE TABLE analytics.parsed_logs
PARTITION(log_date='${runDate}')
SELECT 
    REGEXP_EXTRACT(log_line, '^(\\S+)', 1) as ip_address,
    REGEXP_EXTRACT(log_line, 'GET\\s+(\\S+)', 1) as request_path,
    CAST(REGEXP_EXTRACT(log_line, '\\s+(\\d{3})\\s+', 1) AS INT) as status_code,
    CAST(REGEXP_EXTRACT(log_line, '\\s+(\\d+)$', 1) AS BIGINT) as response_size
FROM raw_logs_lzo
WHERE log_line LIKE '%GET%';


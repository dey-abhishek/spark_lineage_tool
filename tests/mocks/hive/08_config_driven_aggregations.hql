-- Hive script that receives parameters from shell script
-- Demonstrates config-driven Hive aggregations

-- Parameters passed via --hiveconf:
-- ${hiveconf:run_date} - Run date
-- ${hiveconf:env} - Environment (dev/test/prod)
-- ${hiveconf:analytics_db} - Analytics database name
-- ${hiveconf:reports_db} - Reports database name

SET hive.execution.engine=tez;
SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;
SET hive.exec.max.dynamic.partitions=10000;

-- Create database if not exists (using parameter)
CREATE DATABASE IF NOT EXISTS ${hiveconf:analytics_db}
LOCATION '/data/warehouse/${hiveconf:env}/analytics';

CREATE DATABASE IF NOT EXISTS ${hiveconf:reports_db}
LOCATION '/data/warehouse/${hiveconf:env}/reports';

-- Aggregate customer transactions from analytics table
INSERT OVERWRITE TABLE ${hiveconf:reports_db}.customer_daily_summary
PARTITION(report_date='${hiveconf:run_date}')
SELECT
    customer_id,
    customer_name,
    customer_tier,
    region,
    COUNT(DISTINCT transaction_id) as transaction_count,
    SUM(amount) as total_amount,
    AVG(amount) as avg_transaction_value,
    MAX(transaction_date) as last_transaction_date,
    MIN(transaction_date) as first_transaction_date,
    COLLECT_SET(product_category) as product_categories
FROM ${hiveconf:analytics_db}.enriched_transactions_daily
WHERE run_date = '${hiveconf:run_date}'
GROUP BY
    customer_id,
    customer_name,
    customer_tier,
    region;

-- Create regional summary (parameters in database/table names)
INSERT OVERWRITE TABLE ${hiveconf:reports_db}.regional_performance
PARTITION(report_date='${hiveconf:run_date}')
SELECT
    region,
    customer_tier,
    COUNT(DISTINCT customer_id) as unique_customers,
    COUNT(DISTINCT transaction_id) as total_transactions,
    SUM(amount) as total_revenue,
    AVG(amount) as avg_transaction_value,
    PERCENTILE_APPROX(amount, 0.5) as median_transaction_value,
    PERCENTILE_APPROX(amount, 0.95) as p95_transaction_value
FROM ${hiveconf:analytics_db}.enriched_transactions_daily
WHERE run_date = '${hiveconf:run_date}'
GROUP BY
    region,
    customer_tier;

-- Calculate customer lifetime value using window functions
INSERT OVERWRITE TABLE ${hiveconf:reports_db}.customer_ltv
SELECT
    customer_id,
    customer_name,
    region,
    SUM(amount) OVER (
        PARTITION BY customer_id
        ORDER BY transaction_date
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) as cumulative_spend,
    COUNT(*) OVER (
        PARTITION BY customer_id
        ORDER BY transaction_date
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) as cumulative_transactions,
    DATEDIFF(MAX(transaction_date) OVER (PARTITION BY customer_id), 
             MIN(transaction_date) OVER (PARTITION BY customer_id)) as customer_tenure_days,
    ROW_NUMBER() OVER (
        PARTITION BY customer_id 
        ORDER BY transaction_date DESC
    ) as transaction_rank
FROM ${hiveconf:analytics_db}.enriched_transactions_daily
WHERE run_date = '${hiveconf:run_date}';

-- Create materialized view for fraud patterns
-- Uses parameters in source table reference
CREATE TABLE IF NOT EXISTS ${hiveconf:analytics_db}.fraud_patterns (
    pattern_id STRING,
    pattern_type STRING,
    customer_count BIGINT,
    transaction_count BIGINT,
    total_amount DECIMAL(18,2),
    detection_date DATE
)
STORED AS ORC
LOCATION '/data/warehouse/${hiveconf:env}/analytics/fraud_patterns';

INSERT INTO TABLE ${hiveconf:analytics_db}.fraud_patterns
SELECT
    CONCAT(region, '_', customer_tier, '_', DATE_FORMAT(FROM_UNIXTIME(UNIX_TIMESTAMP()), 'yyyyMMdd')) as pattern_id,
    CASE 
        WHEN AVG(amount) > 5000 THEN 'HIGH_VALUE'
        WHEN COUNT(*) > 100 THEN 'HIGH_FREQUENCY'
        WHEN DATEDIFF(MAX(transaction_date), MIN(transaction_date)) < 1 THEN 'RAPID_SUCCESSION'
        ELSE 'NORMAL'
    END as pattern_type,
    COUNT(DISTINCT customer_id) as customer_count,
    COUNT(*) as transaction_count,
    SUM(amount) as total_amount,
    '${hiveconf:run_date}' as detection_date
FROM ${hiveconf:analytics_db}.enriched_transactions_daily
WHERE run_date = '${hiveconf:run_date}'
GROUP BY region, customer_tier
HAVING pattern_type != 'NORMAL';

-- Export to external table for downstream consumption
-- Uses parameter-driven LOCATION
CREATE EXTERNAL TABLE IF NOT EXISTS ${hiveconf:reports_db}.customer_export_${hiveconf:run_date} (
    customer_id STRING,
    customer_name STRING,
    total_transactions BIGINT,
    total_amount DECIMAL(18,2),
    customer_tier STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/data/exports/${hiveconf:env}/customers/${hiveconf:run_date}';

INSERT OVERWRITE TABLE ${hiveconf:reports_db}.customer_export_${hiveconf:run_date}
SELECT
    customer_id,
    customer_name,
    transaction_count as total_transactions,
    total_amount,
    customer_tier
FROM ${hiveconf:reports_db}.customer_daily_summary
WHERE report_date = '${hiveconf:run_date}'
ORDER BY total_amount DESC
LIMIT 10000;

-- Archive old partitions (using parameter)
-- Drop partitions older than 90 days
ALTER TABLE ${hiveconf:analytics_db}.enriched_transactions_daily
DROP IF EXISTS PARTITION (run_date < DATE_SUB('${hiveconf:run_date}', 90));

ALTER TABLE ${hiveconf:reports_db}.customer_daily_summary
DROP IF EXISTS PARTITION (report_date < DATE_SUB('${hiveconf:run_date}', 90));


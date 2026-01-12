--------------------------------------------------------------------------------
-- Pipeline Stage 3: Hive Analytical Processing
-- Creates analytical views from NiFi + Spark processed data
--------------------------------------------------------------------------------

SET hivevar:run_date=${hiveconf:RUN_DATE};
SET hivevar:env=${hiveconf:ENV};

-- Create sales analytics
DROP TABLE IF EXISTS analytics_${hivevar:env}.sales_analysis;

CREATE TABLE analytics_${hivevar:env}.sales_analysis AS
SELECT 
    s.customer_id,
    s.product_id,
    s.sale_date,
    s.quantity,
    s.amount,
    e.event_type,
    e.event_data,
    COUNT(*) as transaction_count,
    SUM(s.amount) as total_revenue,
    AVG(s.amount) as avg_transaction_value
FROM staging_${hivevar:env}.sales_from_nifi s
LEFT JOIN staging_${hivevar:env}.user_events e
    ON s.customer_id = e.user_id
    AND s.load_date = e.load_date
WHERE s.load_date = '${hivevar:run_date}'
GROUP BY 
    s.customer_id, s.product_id, s.sale_date, 
    s.quantity, s.amount, e.event_type, e.event_data;

-- Create user behavior analysis
DROP TABLE IF EXISTS analytics_${hivevar:env}.user_behavior;

CREATE TABLE analytics_${hivevar:env}.user_behavior AS
SELECT 
    e.user_id,
    e.event_type,
    COUNT(DISTINCT e.event_id) as event_count,
    COUNT(DISTINCT s.sale_id) as purchase_count,
    SUM(s.amount) as total_spent,
    MIN(e.timestamp) as first_event,
    MAX(e.timestamp) as last_event,
    e.load_date
FROM staging_${hivevar:env}.user_events e
LEFT JOIN staging_${hivevar:env}.sales_from_nifi s
    ON e.user_id = s.customer_id
    AND e.load_date = s.load_date
WHERE e.load_date = '${hivevar:run_date}'
GROUP BY e.user_id, e.event_type, e.load_date;

-- Create application performance metrics from logs
DROP TABLE IF EXISTS analytics_${hivevar:env}.app_performance;

CREATE TABLE analytics_${hivevar:env}.app_performance AS
SELECT 
    load_date,
    COUNT(*) as total_log_entries,
    SUM(CASE WHEN value LIKE '%ERROR%' THEN 1 ELSE 0 END) as error_count,
    SUM(CASE WHEN value LIKE '%WARN%' THEN 1 ELSE 0 END) as warning_count,
    SUM(CASE WHEN value LIKE '%INFO%' THEN 1 ELSE 0 END) as info_count
FROM staging_${hivevar:env}.application_logs
WHERE load_date = '${hivevar:run_date}'
GROUP BY load_date;

-- Insert into historical tracking
INSERT INTO TABLE analytics_${hivevar:env}.daily_metrics
PARTITION (metric_date='${hivevar:run_date}')
SELECT 
    'sales_analysis' as metric_name,
    COUNT(*) as record_count,
    SUM(total_revenue) as total_value,
    CURRENT_TIMESTAMP() as computed_timestamp
FROM analytics_${hivevar:env}.sales_analysis

UNION ALL

SELECT 
    'user_behavior' as metric_name,
    COUNT(*) as record_count,
    SUM(total_spent) as total_value,
    CURRENT_TIMESTAMP() as computed_timestamp
FROM analytics_${hivevar:env}.user_behavior;


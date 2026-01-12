--------------------------------------------------------------------------------
-- Comprehensive Pipeline Stage 3: Hive Master Data Management
-- Creates gold layer tables with business logic
--------------------------------------------------------------------------------

SET hivevar:run_date=${hiveconf:RUN_DATE};
SET hivevar:env=${hiveconf:ENV};

-- Create Gold Customer Master
DROP TABLE IF EXISTS gold_${hivevar:env}.customer_master;

CREATE TABLE gold_${hivevar:env}.customer_master AS
SELECT 
    c.customer_id,
    c.customer_name,
    c.email,
    c.segment,
    c.event_count,
    t.total_transactions,
    t.total_revenue,
    t.avg_transaction_value,
    t.first_transaction_date,
    t.last_transaction_date,
    p.partner_score,
    CASE 
        WHEN t.total_revenue > 10000 THEN 'VIP'
        WHEN t.total_revenue > 5000 THEN 'Premium'
        WHEN t.total_revenue > 1000 THEN 'Standard'
        ELSE 'Basic'
    END as customer_tier,
    c.load_date
FROM unified_${hivevar:env}.customer_360 c
LEFT JOIN (
    SELECT 
        customer_id,
        COUNT(*) as total_transactions,
        SUM(amount) as total_revenue,
        AVG(amount) as avg_transaction_value,
        MIN(transaction_date) as first_transaction_date,
        MAX(transaction_date) as last_transaction_date
    FROM unified_${hivevar:env}.transactions
    WHERE load_date = '${hivevar:run_date}'
    GROUP BY customer_id
) t ON c.customer_id = t.customer_id
LEFT JOIN (
    SELECT 
        customer_id,
        AVG(partner_metric) as partner_score
    FROM unified_${hivevar:env}.partner_data
    WHERE load_date = '${hivevar:run_date}'
    GROUP BY customer_id
) p ON c.customer_id = p.customer_id
WHERE c.load_date = '${hivevar:run_date}';

-- Create Gold Transaction Summary
DROP TABLE IF EXISTS gold_${hivevar:env}.transaction_summary;

CREATE TABLE gold_${hivevar:env}.transaction_summary AS
SELECT 
    t.transaction_date,
    t.customer_id,
    c.customer_tier,
    COUNT(t.transaction_id) as daily_transaction_count,
    SUM(t.amount) as daily_revenue,
    AVG(t.amount) as avg_transaction_amount,
    MAX(t.amount) as max_transaction_amount,
    r.event_count as daily_events,
    t.load_date
FROM unified_${hivevar:env}.transactions t
LEFT JOIN gold_${hivevar:env}.customer_master c
    ON t.customer_id = c.customer_id
LEFT JOIN (
    SELECT 
        customer_id,
        TO_DATE(event_timestamp) as event_date,
        COUNT(*) as event_count
    FROM unified_${hivevar:env}.realtime_events
    WHERE load_date = '${hivevar:run_date}'
    GROUP BY customer_id, TO_DATE(event_timestamp)
) r ON t.customer_id = r.customer_id 
    AND t.transaction_date = r.event_date
WHERE t.load_date = '${hivevar:run_date}'
GROUP BY 
    t.transaction_date, t.customer_id, c.customer_tier, 
    r.event_count, t.load_date;

-- Create Gold Product Performance
DROP TABLE IF EXISTS gold_${hivevar:env}.product_performance;

CREATE TABLE gold_${hivevar:env}.product_performance AS
SELECT 
    product_id,
    COUNT(DISTINCT customer_id) as unique_customers,
    COUNT(*) as total_transactions,
    SUM(amount) as total_revenue,
    AVG(amount) as avg_price,
    MIN(transaction_date) as first_sale_date,
    MAX(transaction_date) as last_sale_date,
    load_date
FROM unified_${hivevar:env}.transactions
WHERE load_date = '${hivevar:run_date}'
GROUP BY product_id, load_date;

-- Insert into Historical Gold Table
INSERT INTO TABLE gold_${hivevar:env}.customer_history
PARTITION (snapshot_date='${hivevar:run_date}')
SELECT 
    customer_id,
    customer_name,
    customer_tier,
    total_transactions,
    total_revenue,
    CURRENT_TIMESTAMP() as snapshot_timestamp
FROM gold_${hivevar:env}.customer_master;

-- Create Daily KPIs
INSERT OVERWRITE TABLE gold_${hivevar:env}.daily_kpis
PARTITION (metric_date='${hivevar:run_date}')
SELECT 
    'total_customers' as kpi_name,
    COUNT(DISTINCT customer_id) as kpi_value,
    'Customer Count' as kpi_description,
    CURRENT_TIMESTAMP() as computed_at
FROM gold_${hivevar:env}.customer_master

UNION ALL

SELECT 
    'total_revenue' as kpi_name,
    SUM(total_revenue) as kpi_value,
    'Total Revenue' as kpi_description,
    CURRENT_TIMESTAMP() as computed_at
FROM gold_${hivevar:env}.customer_master

UNION ALL

SELECT 
    'avg_customer_value' as kpi_name,
    AVG(total_revenue) as kpi_value,
    'Average Customer Value' as kpi_description,
    CURRENT_TIMESTAMP() as computed_at
FROM gold_${hivevar:env}.customer_master;


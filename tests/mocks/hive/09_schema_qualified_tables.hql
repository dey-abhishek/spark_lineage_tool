-- Test: Schema-Qualified Table Names in Hive SQL
-- Tests extraction of database.table patterns in Hive queries

-- Read from schema-qualified tables and write to another
INSERT OVERWRITE TABLE analytics_db.customer_summary
SELECT 
    c.customer_id,
    c.customer_name,
    c.customer_email,
    COUNT(DISTINCT o.order_id) as total_orders,
    SUM(o.order_amount) as total_spent,
    AVG(o.order_amount) as avg_order_value
FROM prod_db.customers c
LEFT JOIN prod_db.orders o ON c.customer_id = o.customer_id
GROUP BY c.customer_id, c.customer_name, c.customer_email;

-- Create table with schema qualification
CREATE TABLE IF NOT EXISTS warehouse_db.order_summary
STORED AS PARQUET
LOCATION '/data/warehouse/order_summary'
AS
SELECT 
    order_date,
    COUNT(*) as order_count,
    SUM(order_amount) as daily_revenue
FROM prod_db.orders
GROUP BY order_date;

-- Insert with multiple source tables
INSERT INTO TABLE analytics_db.product_performance
SELECT 
    p.product_id,
    p.product_name,
    p.category,
    COUNT(DISTINCT o.order_id) as times_ordered,
    SUM(oi.quantity) as total_quantity,
    SUM(oi.line_total) as total_revenue
FROM catalog_db.products p
JOIN prod_db.order_items oi ON p.product_id = oi.product_id
JOIN prod_db.orders o ON oi.order_id = o.order_id
WHERE o.order_date >= '2024-01-01'
GROUP BY p.product_id, p.product_name, p.category;

-- Create view with schema-qualified sources
CREATE VIEW analytics_db.customer_360 AS
SELECT 
    c.*,
    cs.total_orders,
    cs.total_spent,
    cs.avg_order_value
FROM prod_db.customers c
LEFT JOIN analytics_db.customer_summary cs ON c.customer_id = cs.customer_id;

-- Complex join across multiple schemas
INSERT OVERWRITE TABLE reporting_db.monthly_metrics
SELECT 
    DATE_FORMAT(o.order_date, 'yyyy-MM') as month,
    p.category,
    COUNT(DISTINCT c.customer_id) as unique_customers,
    COUNT(DISTINCT o.order_id) as total_orders,
    SUM(oi.line_total) as revenue
FROM prod_db.orders o
JOIN prod_db.order_items oi ON o.order_id = oi.order_id
JOIN catalog_db.products p ON oi.product_id = p.product_id
JOIN prod_db.customers c ON o.customer_id = c.customer_id
GROUP BY DATE_FORMAT(o.order_date, 'yyyy-MM'), p.category;


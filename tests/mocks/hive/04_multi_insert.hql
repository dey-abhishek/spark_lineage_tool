-- Multi-insert from single source
-- Tests: FROM...INSERT pattern, multiple targets

FROM prod.transactions t
LEFT JOIN prod.users u ON t.user_id = u.user_id
LEFT JOIN prod.products p ON t.product_id = p.product_id

-- Insert daily sales
INSERT OVERWRITE TABLE analytics.daily_sales
PARTITION (date)
SELECT 
    SUM(t.amount) as total_sales,
    COUNT(DISTINCT t.transaction_id) as txn_count,
    COUNT(DISTINCT t.user_id) as unique_customers,
    t.transaction_date as date
GROUP BY t.transaction_date

-- Insert user metrics
INSERT INTO TABLE analytics.user_metrics
SELECT 
    u.user_id,
    u.user_name,
    COUNT(t.transaction_id) as txn_count,
    SUM(t.amount) as total_spent,
    AVG(t.amount) as avg_transaction
GROUP BY u.user_id, u.user_name

-- Insert product performance
INSERT INTO TABLE analytics.product_performance
SELECT 
    p.product_id,
    p.product_name,
    p.category,
    COUNT(t.transaction_id) as times_sold,
    SUM(t.quantity) as total_quantity
GROUP BY p.product_id, p.product_name, p.category;


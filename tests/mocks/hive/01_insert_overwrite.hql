-- Simple INSERT OVERWRITE
INSERT OVERWRITE TABLE analytics.user_summary
PARTITION (date='2024-01-01')
SELECT 
    user_id,
    COUNT(*) as transaction_count,
    SUM(amount) as total_amount
FROM prod.transactions
WHERE date = '2024-01-01'
GROUP BY user_id;


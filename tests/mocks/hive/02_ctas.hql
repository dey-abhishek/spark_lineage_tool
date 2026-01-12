-- CREATE TABLE AS SELECT
CREATE EXTERNAL TABLE IF NOT EXISTS analytics.enriched_transactions
STORED AS PARQUET
LOCATION '/data/processed/enriched_transactions'
AS
SELECT 
    t.transaction_id,
    t.amount,
    u.user_name,
    p.product_name
FROM prod.transactions t
JOIN prod.users u ON t.user_id = u.user_id
JOIN prod.products p ON t.product_id = p.product_id;


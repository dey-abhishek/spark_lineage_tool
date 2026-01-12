--------------------------------------------------------------------------------
-- Pipeline Stage 3: Hive Enrichment Layer
-- Enriches staging data and creates analytical tables
--------------------------------------------------------------------------------

SET hivevar:run_date=${hiveconf:RUN_DATE};
SET hivevar:env=${hiveconf:ENV};

-- Create enriched customer table
DROP TABLE IF EXISTS enriched_${hivevar:env}.customers;

CREATE TABLE enriched_${hivevar:env}.customers AS
SELECT 
    cm.customer_id,
    cm.customer_name,
    cm.email,
    cm.phone,
    cm.address,
    cm.city,
    cm.state,
    cm.country,
    cm.registration_date,
    COUNT(DISTINCT ct.transaction_id) as total_transactions,
    SUM(ct.transaction_amount) as total_spend,
    AVG(ct.transaction_amount) as avg_transaction_amount,
    MAX(ct.transaction_date) as last_transaction_date,
    cm.load_date
FROM staging_${hivevar:env}.customer_master cm
LEFT JOIN staging_${hivevar:env}.customer_transactions ct
    ON cm.customer_id = ct.customer_id
WHERE cm.load_date = '${hivevar:run_date}'
GROUP BY 
    cm.customer_id, cm.customer_name, cm.email, cm.phone,
    cm.address, cm.city, cm.state, cm.country,
    cm.registration_date, cm.load_date;

-- Create enriched product table with inventory
DROP TABLE IF EXISTS enriched_${hivevar:env}.products_with_inventory;

CREATE TABLE enriched_${hivevar:env}.products_with_inventory AS
SELECT 
    p.product_id,
    p.product_name,
    p.category,
    p.sub_category,
    p.brand,
    p.price,
    p.description,
    i.warehouse_id,
    i.quantity_available,
    i.quantity_reserved,
    i.reorder_point,
    i.last_stock_update,
    p.load_date
FROM staging_${hivevar:env}.products p
LEFT JOIN staging_${hivevar:env}.inventory i
    ON p.product_id = i.product_id
WHERE p.load_date = '${hivevar:run_date}'
    AND i.load_date = '${hivevar:run_date}';

-- Create customer transaction summary
DROP TABLE IF EXISTS enriched_${hivevar:env}.customer_transaction_summary;

CREATE TABLE enriched_${hivevar:env}.customer_transaction_summary AS
SELECT 
    ct.customer_id,
    ct.transaction_date,
    COUNT(ct.transaction_id) as transaction_count,
    SUM(ct.transaction_amount) as daily_spend,
    AVG(ct.transaction_amount) as avg_transaction,
    MAX(ct.transaction_amount) as max_transaction,
    MIN(ct.transaction_amount) as min_transaction,
    ct.load_date
FROM staging_${hivevar:env}.customer_transactions ct
WHERE ct.load_date = '${hivevar:run_date}'
GROUP BY ct.customer_id, ct.transaction_date, ct.load_date;

-- Insert into historical tracking table
INSERT INTO TABLE enriched_${hivevar:env}.customer_history
PARTITION (load_date='${hivevar:run_date}')
SELECT 
    customer_id,
    customer_name,
    email,
    total_transactions,
    total_spend,
    avg_transaction_amount,
    last_transaction_date,
    CURRENT_TIMESTAMP() as updated_timestamp
FROM enriched_${hivevar:env}.customers;


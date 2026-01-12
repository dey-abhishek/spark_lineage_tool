-- Complex Hive SQL with dynamic partitions
-- Tests: Dynamic partitions, multiple inserts, complex transformations

SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;
SET hive.exec.max.dynamic.partitions=1000;

-- Create target table if not exists
CREATE TABLE IF NOT EXISTS analytics.sales_summary (
    product_id STRING,
    product_name STRING,
    total_sales DECIMAL(10,2),
    total_quantity INT,
    avg_price DECIMAL(10,2)
)
PARTITIONED BY (year INT, month INT, region STRING)
STORED AS PARQUET;

-- Insert with dynamic partitioning
INSERT OVERWRITE TABLE analytics.sales_summary
PARTITION (year, month, region)
SELECT 
    s.product_id,
    p.product_name,
    SUM(s.amount) as total_sales,
    SUM(s.quantity) as total_quantity,
    AVG(s.unit_price) as avg_price,
    YEAR(s.sale_date) as year,
    MONTH(s.sale_date) as month,
    s.region
FROM prod.sales s
JOIN prod.products p ON s.product_id = p.product_id
WHERE s.sale_date >= '2024-01-01'
GROUP BY 
    s.product_id,
    p.product_name,
    YEAR(s.sale_date),
    MONTH(s.sale_date),
    s.region;


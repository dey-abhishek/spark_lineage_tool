-- Product Performance Analysis
-- Uses hivevar substitution with schema-qualified table names passed from shell

-- Variables are set via beeline --hivevar from shell script:
-- source_products, source_orders, target_table

INSERT OVERWRITE TABLE ${hivevar:target_table}
SELECT 
    p.product_id,
    p.product_name,
    p.category,
    COUNT(DISTINCT o.order_id) as times_ordered,
    SUM(o.order_amount) as total_revenue,
    AVG(o.order_amount) as avg_order_value
FROM ${hivevar:source_products} p
LEFT JOIN ${hivevar:source_orders} o ON p.product_id = o.product_id
WHERE o.order_date >= DATE_SUB(CURRENT_DATE, 30)
GROUP BY p.product_id, p.product_name, p.category
HAVING COUNT(DISTINCT o.order_id) > 0
ORDER BY total_revenue DESC;


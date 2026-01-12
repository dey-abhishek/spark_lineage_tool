-- Complex CTEs and window functions
-- Tests: CTEs, window functions, subqueries

WITH customer_orders AS (
    SELECT 
        user_id,
        transaction_id,
        amount,
        transaction_date,
        ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY transaction_date DESC) as rn,
        SUM(amount) OVER (PARTITION BY user_id) as total_spent
    FROM prod.transactions
    WHERE transaction_date >= '2024-01-01'
),
top_customers AS (
    SELECT 
        user_id,
        total_spent,
        RANK() OVER (ORDER BY total_spent DESC) as customer_rank
    FROM customer_orders
    WHERE rn = 1
      AND total_spent > 1000
),
customer_segments AS (
    SELECT 
        user_id,
        CASE 
            WHEN total_spent > 10000 THEN 'VIP'
            WHEN total_spent > 5000 THEN 'Premium'
            WHEN total_spent > 1000 THEN 'Regular'
            ELSE 'Basic'
        END as segment
    FROM top_customers
)
INSERT OVERWRITE TABLE analytics.customer_segmentation
SELECT 
    tc.user_id,
    u.user_name,
    u.email,
    tc.total_spent,
    tc.customer_rank,
    cs.segment,
    CURRENT_DATE() as analysis_date
FROM top_customers tc
JOIN prod.users u ON tc.user_id = u.user_id
JOIN customer_segments cs ON tc.user_id = cs.user_id
WHERE tc.customer_rank <= 100;


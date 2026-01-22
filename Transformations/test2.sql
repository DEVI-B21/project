INSERT INTO testing_datasets.order_summary (
    customer_id,
    customer_name,
    total_amount
)
SELECT
    customers.customer_id,
    customers.customer_name,
    SUM(orders.amount)
FROM testing_datasets.orders
JOIN testing_datasets.customers
ON orders.customer_id = customers.customer_id
GROUP BY
    customers.customer_id,
    customers.customer_name;

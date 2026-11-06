-- Deterministic seed data. Debezium's initial snapshot emits these as "r" (read) events.

INSERT INTO customers (email, full_name, city)
SELECT
    'customer' || g || '@example.com',
    (ARRAY['Aarav', 'Diya', 'Vivaan', 'Ananya', 'Rohan', 'Meera', 'Kabir', 'Isha'])[1 + g % 8]
        || ' ' ||
    (ARRAY['Reddy', 'Sharma', 'Iyer', 'Nair', 'Patel', 'Khan'])[1 + g % 6],
    (ARRAY['Chennai', 'Bengaluru', 'Hyderabad', 'Mumbai', 'Delhi', 'Pune'])[1 + g % 6]
FROM generate_series(1, 50) AS g;

INSERT INTO products (sku, name, category, price_cents, stock)
SELECT
    'SKU-' || lpad(g::text, 4, '0'),
    'Product ' || g,
    (ARRAY['electronics', 'books', 'home', 'fitness', 'grocery'])[1 + g % 5],
    500 + (g * 397) % 9500,
    10 + (g * 7) % 90
FROM generate_series(1, 20) AS g;

INSERT INTO orders (customer_id, product_id, quantity, status, total_cents)
SELECT x.customer_id, p.id, x.quantity, x.status, x.quantity * p.price_cents
FROM (
    SELECT
        1 + g % 50 AS customer_id,
        1 + g % 20 AS product_id,
        1 + g % 4  AS quantity,
        (ARRAY['pending', 'paid', 'shipped', 'delivered', 'cancelled'])[1 + g % 5] AS status
    FROM generate_series(1, 100) AS g
) AS x
JOIN products p ON p.id = x.product_id;

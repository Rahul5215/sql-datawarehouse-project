/* =========================================================
   6. Ranking Analysis
========================================================= */
-- Top 10 customers by revenue
SELECT
    c.customer_key,
    c.first_name,
    c.last_name,
    SUM(s.sales_amount) AS total_revenue
FROM gold.dim_customers c
LEFT JOIN gold.fact_sales s
    ON s.customer_key = c.customer_key
GROUP BY c.customer_key, c.first_name, c.last_name
ORDER BY total_revenue DESC LIMIT 10;

-- Top 10 customers by revenue (using window function)
SELECT *
FROM (
    SELECT
        c.customer_key,
        c.first_name,
        c.last_name,
        SUM(s.sales_amount) AS total_revenue,
        ROW_NUMBER() OVER (ORDER BY SUM(s.sales_amount) DESC) AS ranking
    FROM gold.dim_customers c
    LEFT JOIN gold.fact_sales s
        ON s.customer_key = c.customer_key
    GROUP BY c.customer_key
) t
WHERE ranking <= 10;


-- Bottom 3 customers by orders
SELECT
    c.customer_key,
    c.first_name,
    c.last_name,
    COUNT(DISTINCT s.order_number) AS total_orders
FROM gold.dim_customers c
LEFT JOIN gold.fact_sales s
    ON s.customer_key = c.customer_key
GROUP BY c.customer_key
ORDER BY total_orders ASC LIMIT 3;

-- Bottom 3 customers by orders (using window function)
SELECT *
FROM (
    SELECT
        c.customer_key,
        c.first_name,
        c.last_name,
        COUNT(DISTINCT s.order_number) AS total_orders,
        ROW_NUMBER() OVER (ORDER BY COUNT(DISTINCT s.order_number) ASC) AS ranking
    FROM gold.dim_customers c
    LEFT JOIN gold.fact_sales s
        ON s.customer_key = c.customer_key
    GROUP BY c.customer_key
) t
WHERE ranking <= 3;


-- Top 5 products by revenue
SELECT
    p.product_name,
    SUM(s.sales_amount) AS total_revenue
FROM gold.dim_products p
JOIN gold.fact_sales s
    ON s.product_key = p.product_key
GROUP BY p.product_name
ORDER BY total_revenue DESC LIMIT 5;

-- Bottom 5 products by sales quantity
SELECT
    p.product_name,
    SUM(s.quantity) AS total_sales
FROM gold.dim_products p
JOIN gold.fact_sales s
    ON s.product_key = p.product_key
GROUP BY p.product_name
ORDER BY total_sales ASC LIMIT 5;

-- Bottom 5 products by sales (using window function)
SELECT *
FROM (
    SELECT
        p.product_name,
        SUM(s.quantity) AS total_sales,
        ROW_NUMBER() OVER (ORDER BY SUM(s.quantity) ASC) AS ranking
    FROM gold.dim_products p
    JOIN gold.fact_sales s
        ON s.product_key = p.product_key
    GROUP BY p.product_name
) t
WHERE ranking <= 5;

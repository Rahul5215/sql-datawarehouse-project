/* =========================================================
   5. Magnitude Analysis
========================================================= */
-- Magnitude of each business measure (absolute scale)
SELECT 'Total Sales' AS measure_name, SUM(sales_amount) AS measure_value FROM gold.fact_sales
UNION ALL
SELECT 'Total Quantity' AS measure_name, SUM(quantity) AS measure_value FROM gold.fact_sales
UNION ALL
SELECT 'Total Orders' AS measure_name, COUNT(DISTINCT order_number) AS measure_value FROM gold.fact_sales
UNION ALL
SELECT 'Total Customers (Ordered)' AS measure_name, COUNT(DISTINCT customer_key) AS measure_value FROM gold.fact_sales
UNION ALL
SELECT 'Total Products (Sold)' AS measure_name, COUNT(DISTINCT product_key) AS measure_value FROM gold.fact_sales;

-- Magnitude by product category (absolute + relative share)
SELECT
    p.category,
    SUM(s.sales_amount) AS total_revenue,
    ROUND(100.0 * SUM(s.sales_amount) / SUM(SUM(s.sales_amount)) OVER(), 2) AS revenue_share_pct
FROM gold.fact_sales s
JOIN gold.dim_products p
    ON s.product_key = p.product_key
GROUP BY p.category
ORDER BY total_revenue DESC;

-- Magnitude by customer (absolute + relative share)
SELECT
    c.customer_key,
    c.first_name,
    c.last_name,
    SUM(s.sales_amount) AS total_revenue,
    ROUND(100.0 * SUM(s.sales_amount) / SUM(SUM(s.sales_amount)) OVER(), 2) AS revenue_share_pct
FROM gold.fact_sales s
JOIN gold.dim_customers c
    ON s.customer_key = c.customer_key
GROUP BY c.customer_key, c.first_name, c.last_name
ORDER BY total_revenue DESC;

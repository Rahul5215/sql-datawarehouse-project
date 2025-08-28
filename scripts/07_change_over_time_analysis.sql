/* =========================================================
   7. Change Over Time Analysis
========================================================= */

-- Find the first and last order date and how many years of sales are available
SELECT
MIN(order_date) AS first_order_date,
MAX(order_date) AS last_order_date,
DATE_PART('year', AGE(MAX(order_date), MIN(order_date))) AS order_range_year
FROM gold.fact_sales;


-- Calculate yearly total sales, running total, and moving average price
SELECT
order_date,
total_sales,
SUM(total_sales) OVER (ORDER BY order_date) AS running_total_sales,
ROUND(AVG(avg_price) OVER (ORDER BY order_date),2) AS moving_average_price
FROM(
  SELECT 
  DATE_TRUNC('year', order_date)::date AS order_date,
  SUM(sales_amount) AS total_sales,
  AVG(price) AS avg_price
  FROM gold.fact_sales
  GROUP BY DATE_TRUNC('year', order_date)
) t;

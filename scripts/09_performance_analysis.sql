/*=====================================================================================
9. Performance Analysis
=====================================================================================*/

-- Customer lifespan
SELECT customer_key,
       MIN(order_date) AS first_order,
       MAX(order_date) AS last_order,
       (EXTRACT(YEAR FROM AGE(MAX(order_date), MIN(order_date))) * 12
        + EXTRACT(MONTH FROM AGE(MAX(order_date), MIN(order_date)))) AS lifespan_months
FROM gold.fact_sales
GROUP BY customer_key;

-- Average monthly sales per customer (added)
WITH lifespan AS (
  SELECT customer_key,
         MIN(order_date) AS first_order,
         MAX(order_date) AS last_order,
         (EXTRACT(YEAR FROM AGE(MAX(order_date), MIN(order_date))) * 12
          + EXTRACT(MONTH FROM AGE(MAX(order_date), MIN(order_date)))) AS lifespan_months,
         SUM(sales_amount) AS total_sales
  FROM gold.fact_sales
  GROUP BY customer_key
)
SELECT customer_key, total_sales, lifespan_months,
       ROUND(total_sales::numeric / NULLIF(lifespan_months,0),2) AS avg_monthly_sales
FROM lifespan;

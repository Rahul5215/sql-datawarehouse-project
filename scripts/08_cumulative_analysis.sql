/* =========================================================
   8. Cumulative Analysis
========================================================= */

-- Calculate the total sales per month and the running total of sales over years 

--For months:-
SELECT
order_date,
total_sales,
SUM(total_sales) OVER (PARTITION BY DATE_PART('year', order_date) ORDER BY order_date) AS running_total_sales,
ROUND(AVG(avg_price) OVER (PARTITION BY DATE_PART('year', order_date) ORDER BY order_date),2) AS moving_average_price
FROM(
SELECT 
DATE_TRUNC('month', order_date)::date AS order_date,
SUM(sales_amount) AS total_sales,
AVG(price) AS avg_price
FROM gold.fact_sales
WHERE order_date IS NOT NULL
GROUP BY DATE_TRUNC('month', order_date)::date
)t


--Calculate the total sales per month
--and the running total of sales over time
SELECT
order_date,
total_sales,
SUM(total_sales) OVER (ORDER BY order_date) AS running_total_sales,
ROUND(AVG(avg_price) OVER (ORDER BY order_date),2) AS moving_average_price
FROM(
SELECT 
DATE_TRUNC('month', order_date)::date AS order_date,
SUM(sales_amount) AS total_sales,
AVG(price) AS avg_price
FROM gold.fact_sales
WHERE order_date IS NOT NULL
GROUP BY DATE_TRUNC('month', order_date)::date
)t


--Calculate the total sales per year and the running total of sales over time
SELECT
order_date,
total_sales,
SUM(total_sales) OVER (ORDER BY order_date) AS running_total_sales,
ROUND(AVG(avg_price) OVER(ORDER BY order_date),2) AS moving_average_price
FROM(
SELECT 
DATE_TRUNC('year', order_date)::date AS order_date,
SUM(sales_amount) AS total_sales,
AVG(price) AS avg_price
FROM gold.fact_sales
WHERE order_date IS NOT NULL
GROUP BY DATE_TRUNC('year', order_date)::date
)t

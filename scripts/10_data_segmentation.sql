/* =========================================================
   10  Data Segmentation
========================================================= */

--Segment products into cost ranges and count how many products fall into each segment
WITH cost_range AS (
SELECT
product_key,
product_name,
cost,
CASE
   WHEN cost < 100 THEN 'Below 100'
   WHEN cost BETWEEN 100 AND 500 THEN '100-500'
   WHEN cost between 500 AND 1000 THEN '500-1000'
   ELSE 'Above 1000'
END cost_range
FROM gold.dim_products 
)

SELECT
cost_range,
COUNT(product_key) AS total_products
FROM cost_range
GROUP BY cost_range

/*Group customers into three segments based on their spending behaviour
  VIP: atleast 12 months of history and spending more than 5000
  Regular: atleast 12 months of history and spending 5000 or less
  New: lifespan less than 12 months
  And find the total number of customers of each group    */
WITH customer_spending AS(
SELECT
s.customer_key,
c.first_name,
c.last_name,
SUM(s.sales_amount) AS total_spending,
MIN(s.order_date) AS first_order_date,
MAX(s.order_date) AS last_order_date,
EXTRACT(YEAR FROM AGE(MAX(s.order_date), MIN(s.order_date)))*12 + EXTRACT(MONTH FROM AGE(MAX(s.order_date), MIN(s.order_date))) AS lifespan
FROM gold.fact_sales s
JOIN gold.dim_customers c
ON c.customer_key = s.customer_key
GROUP BY s.customer_key,first_name,c.last_name
order by customer_key
)

SELECT
customer_segment,
COUNT(customer_key)
FROM(
SELECT
customer_key,
first_name,
last_name,
total_spending,
lifespan,
CASE  
    WHEN lifespan >= 12 AND total_spending >5000 THEN 'VIP'
	WHEN lifespan >= 12 AND total_spending <=5000 THEN 'Regular'
    ELSE 'New'
END AS customer_segment
FROM customer_spending
)t
GROUP BY customer_segment

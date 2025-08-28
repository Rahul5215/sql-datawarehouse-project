/*
=============================================================
Product Report
=============================================================
Purpose:
 - This report consolidates key product metrics and behaviors.

Highlights:
 1. Gathers essential fields such as product name, category, subcategory, and cost.
 2. Segments products by revenue to identify High-Performers, Mid-Range, or Low-Performers.
 3. Aggregates product-level metrics:
    - Total orders
    - Total sales
    - Total quantity sold
    - Total customers (unique)
    - Lifespan (in months)
 4. Calculates valuable KPIs:
    - recency (months since last sale)
    - average order revenue (AOR)
    - average monthly revenue
=============================================================
*/


/*-----------------------------------------------------------------------
1) Base Query: Retrieves core columns from fact_sales and dim_product
------------------------------------------------------------------------*/
WITH base_query AS (
SELECT
s.order_number,
s.order_date,
s.sales_amount,
s.quantity,
s.customer_key,
p.product_key,
p.product_name,
p.category,
p.subcategory,
p.cost
FROM gold.fact_sales s
LEFT JOIN gold.dim_products p
ON p.product_key = s.product_key
)

, product_aggregations AS (
SELECT
product_key,
product_name,
category,
subcategory,
cost,
COUNT(order_number) AS total_orders,
MIN(order_date) AS first_order_date,
MAX(order_date) AS last_order_date,
EXTRACT(YEAR FROM AGE(MAX(order_date), MIN(order_date)))*12 + EXTRACT(MONTH FROM AGE(MAX(order_date), MIN(order_date))) AS lifespan,
SUM(sales_amount) AS total_sales,
SUM(quantity) AS total_quantity,
ROUND(AVG(sales_amount),2) AS avg_selling_price
FROM base_query
GROUP BY product_key,
         product_name,
         category,
         subcategory,
         cost
)

SELECT
product_key,
product_name,
category,
subcategory,
cost,
first_order_date,
last_order_date,
EXTRACT(YEAR FROM AGE(CURRENT_DATE, last_order_date))*12 + EXTRACT(MONTH FROM AGE(CURRENT_DATE, last_order_date)) AS recency_in_months,
CASE
    WHEN total_sales > 50000 THEN 'High-Performance'
	WHEN total_sales >= 10000 THEN 'Mid-Range'
	ELSE 'Low-Performance'
END AS product_segment,
lifespan,
total_orders
total_sales,
total_quantity,
avg_selling_price,
-- average order revenue (AOR)
CASE
    WHEN total_orders = 0 THEN 0
	ELSE total_sales / total_orders
END AS avg_order_revenue,
-- average monthly revenue
CASE
    WHEN lifespan = 0 THEN total_sales
	ELSE total_sales / lifespan
END AS avg_order_revenue
FROM product_aggregations

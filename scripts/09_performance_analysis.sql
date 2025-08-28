/*=====================================================================================
9. Performance Analysis
=====================================================================================*/

/*Analyze the yearly performance of the products by comparing their sales to 
  both the average sales performance of the product and the previous year's sales.*/
WITH yearly_product_sales AS(
SELECT
EXTRACT(YEAR FROM s.order_date) AS order_year,
p.product_name,
SUM(s.sales_amount) AS current_sales
FROM gold.fact_sales s
JOIN gold.dim_products p
ON s.product_key = p.product_key
WHERE s.order_date IS NOT NULL
GROUP BY order_year, p.product_name
)

SELECT
order_year,
product_name,
current_sales,
ROUND(AVG(current_sales) OVER(PARTITION BY product_name),2) AS avg_sales,
current_sales - ROUND(AVG(current_sales) OVER(PARTITION BY product_name),2) AS avg_diff,
CASE 
   WHEN current_sales - ROUND(AVG(current_sales) OVER(PARTITION BY product_name),2) > 0 THEN 'Above Average'
   WHEN current_sales - ROUND(AVG(current_sales) OVER(PARTITION BY product_name),2) < 0 THEN 'Below Average'
   ELSE 'Average'
END AS avg_change,
LAG(current_sales) OVER(PARTITION BY product_name ORDER BY order_year) AS py_sales,
current_sales - LAG(current_sales) OVER(PARTITION BY product_name ORDER BY order_year) AS diff_py_sales,
CASE 
   WHEN current_sales - LAG(current_sales) OVER(PARTITION BY product_name ORDER BY order_year) > 0 THEN 'Yes'
   WHEN current_sales - LAG(current_sales) OVER(PARTITION BY product_name ORDER BY order_year) < 0 THEN 'No'
   ELSE 'Even'
END AS growth_py
FROM yearly_product_sales
ORDER BY product_name,order_year



/*Analyze the monthly performance of the products by comaparing their sales to 
  both the average sales performance of the product and the previous year's sales.*/
WITH monthly_product_sales AS(
SELECT
EXTRACT(MONTH FROM s.order_date) AS order_month_num,
TO_CHAR(s.order_date,'month') AS order_month,
p.product_name,
SUM(s.sales_amount) AS current_sales
FROM gold.fact_sales s
JOIN gold.dim_products p
ON s.product_key = p.product_key
WHERE s.order_date IS NOT NULL
GROUP BY order_month_num,order_month, p.product_name
)

SELECT
order_month,
product_name,
current_sales,
ROUND(AVG(current_sales) OVER(PARTITION BY product_name),2) AS avg_sales,
current_sales - ROUND(AVG(current_sales) OVER(PARTITION BY product_name),2) AS avg_diff,
CASE 
   WHEN current_sales - ROUND(AVG(current_sales) OVER(PARTITION BY product_name),2) > 0 THEN 'Above Average'
   WHEN current_sales - ROUND(AVG(current_sales) OVER(PARTITION BY product_name),2) < 0 THEN 'Below Average'
   ELSE 'Average'
END AS avg_change,
LAG(current_sales) OVER(PARTITION BY product_name ORDER BY order_month_num) AS pm_sales,
current_sales - LAG(current_sales) OVER(PARTITION BY product_name ORDER BY order_month_num) AS diff_pm_sales,
CASE 
   WHEN current_sales - LAG(current_sales) OVER(PARTITION BY product_name ORDER BY order_month_num) > 0 THEN 'Yes'
   WHEN current_sales - LAG(current_sales) OVER(PARTITION BY product_name ORDER BY order_month_num) < 0 THEN 'No'
   ELSE 'Even'
END AS growth_pm
FROM monthly_product_sales
ORDER BY product_name,order_month_num

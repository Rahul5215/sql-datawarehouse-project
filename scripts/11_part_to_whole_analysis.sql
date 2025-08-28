/*==========================================================================
 11  Part-To-Whole Analysis
==========================================================================*/

--Which category contributes most to overall sales
WITH category_sales AS(
SELECT
p.category,
SUM(s.sales_amount) AS total_sales
FROM gold.fact_sales s
LEFT JOIN gold.dim_products p
ON p.product_key = s.product_key
GROUP BY p.category
)

SELECT
category,
total_sales,
SUM(total_sales) OVER() AS overall_sales,
CONCAT(ROUND(total_sales / SUM(total_sales) OVER() *100,2),'%') AS sales_contribution
FROM category_sales



--Which subcategory contributes most to overall sales
WITH subcategory_sales AS(
SELECT
p.category,
p.subcategory,
SUM(s.sales_amount) AS total_sales
FROM gold.fact_sales s
LEFT JOIN gold.dim_products p
ON p.product_key = s.product_key
GROUP BY p.category,p.subcategory
ORDER BY p.category
)

SELECT
category,
subcategory,
total_sales,
SUM(total_sales) OVER() AS overall_sales,
CONCAT(ROUND(total_sales / SUM(total_sales) OVER() *100,2),'%') AS sales_contribution




-- Overall cumulative sales
SELECT order_date, SUM(sales_amount) OVER (ORDER BY order_date) AS cumulative_sales
FROM gold.fact_sales;

-- YTD cumulative sales (added)
SELECT DATE_TRUNC('year', order_date)::date AS year,
       DATE_TRUNC('month', order_date)::date AS month,
       SUM(sales_amount) AS monthly_sales,
       SUM(SUM(sales_amount)) OVER (PARTITION BY DATE_TRUNC('year', order_date)
                                    ORDER BY DATE_TRUNC('month', order_date)) AS ytd_sales
FROM gold.fact_sales
GROUP BY 1,2
ORDER BY 1,2;

-- ========================================================
-- Quality Checks for Gold Layer
-- ========================================================

-- ========================================================
-- 1. gold.dim_customers
-- Purpose: Ensure customer dimension is clean, unique, and complete
-- ========================================================

-- Check for duplicate keys
SELECT customer_key, COUNT(*)
FROM gold.dim_customers
GROUP BY customer_key
HAVING COUNT(*) > 1;   -- Should return 0 rows

-- Check for missing values in important fields
SELECT *
FROM gold.dim_customers
WHERE customer_id IS NULL OR first_name IS NULL OR last_name IS NULL;


-- ========================================================
-- 2. gold.dim_products
-- Purpose: Ensure product dimension is consistent and reliable
-- ========================================================

-- Check for duplicate keys
SELECT product_key, COUNT(*)
FROM gold.dim_products
GROUP BY product_key
HAVING COUNT(*) > 1;   -- Should return 0 rows

-- Check for missing values in key product details
SELECT *
FROM gold.dim_products
WHERE product_id IS NULL OR product_name IS NULL OR category_id IS NULL;


-- ========================================================
-- 3. gold.fact_sales
-- Purpose: Validate fact table integrity and business logic
-- ========================================================

-- Check for NULL foreign keys (should not happen if joins worked correctly)
SELECT *
FROM gold.fact_sales
WHERE customer_key IS NULL OR product_key IS NULL;

-- Check for negative or zero sales
SELECT *
FROM gold.fact_sales
WHERE sales_amount <= 0;

-- Check for invalid dates (shipping date before order date)
SELECT *
FROM gold.fact_sales
WHERE shipping_date < order_date;

-- Ensure fact table is not empty
SELECT COUNT(*) AS sales_count FROM gold.fact_sales;

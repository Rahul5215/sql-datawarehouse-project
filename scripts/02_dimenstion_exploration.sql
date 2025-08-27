/* =========================================================
   2. Explore Dimensions
========================================================= */
-- Explore all countries our customers come from
SELECT DISTINCT country FROM gold.dim_customers;

-- Explore all product categories
SELECT DISTINCT category FROM gold.dim_products;

-- Explore categories with subcategories
SELECT DISTINCT category, subcategory
FROM gold.dim_products
ORDER BY category, subcategory;

-- Explore categories, subcategories with product name
SELECT DISTINCT category, subcategory, product_name
FROM gold.dim_products
ORDER BY category, subcategory, product_name;

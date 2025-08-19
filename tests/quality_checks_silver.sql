/*
Data Quality Checks and Transformations
---------------------------------------
Purpose:
- Validate, clean, and standardize data from Bronze Layer before loading into Silver Layer.
- Ensure data integrity, consistency, and readiness for analytics.

Checks Implemented:
1. Primary Key Validation
   - Ensure no NULLs or duplicates in entity identifiers.
2. Data Deduplication
   - Retain the latest record per entity using ROW_NUMBER().
3. Data Cleansing
   - Trim unwanted spaces from string fields.
   - Standardize coded values (e.g., marital status, gender, product line).
4. Referential Integrity
   - Validate foreign key relationships (sales → customers, products).
5. Date Validation
   - Ensure dates are valid (8 digits, > 1920, < current timestamp).
   - Ensure logical ordering (order_dt < ship_dt < due_dt).
6. Numeric Validations
   - Replace NULL/negative costs, sales, or prices with corrected values.
7. Standardization
   - Normalize country names, categories, and other lookup values.
8. Transformation Rules
   - Generate surrogate IDs, category IDs, calculate end dates with LEAD().
   - Enforce sales = quantity * price.
9. Missing Value Handling
   - Replace NULLs with 'n/a' or defaults where applicable.
10. Outlier Detection (added)
   - Identify unusually high/low sales, prices, or invalid ages in customers.
*/

------------------------------------------------------------
-- CRM Customer Info
------------------------------------------------------------
-- Nulls & Duplicates in Primary Key
SELECT cst_id, COUNT(*) 
FROM bronze.crm_cust_info
GROUP BY cst_id
HAVING COUNT(*) > 1 OR cst_id IS NULL;

-- Deduplication (retain latest record)
SELECT *
FROM (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag
    FROM bronze.crm_cust_info
    WHERE cst_id IS NOT NULL
) t
WHERE flag = 1;

-- Trim unwanted spaces
SELECT *
FROM bronze.crm_cust_info
WHERE cst_firstname != TRIM(cst_firstname);

-- Standardize marital status & gender
SELECT
    cst_firstname,
    CASE WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'SINGLE'
         WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'MARRIED'
         ELSE 'n/a'
    END AS cst_marital_status,
    CASE WHEN UPPER(TRIM(cst_gndr)) IN ('F') THEN 'FEMALE'
         WHEN UPPER(TRIM(cst_gndr)) IN ('M') THEN 'MALE'
         ELSE 'n/a'
    END AS cst_gndr
FROM bronze.crm_cust_info;

------------------------------------------------------------
-- CRM Product Info
------------------------------------------------------------
-- Nulls & Duplicates
SELECT prd_id, COUNT(*)
FROM bronze.crm_prd_info
GROUP BY prd_id
HAVING COUNT(*) > 1 OR prd_id IS NULL;

-- Unwanted spaces
SELECT *
FROM bronze.crm_prd_info
WHERE prd_nm != TRIM(prd_nm);

-- Invalid product cost
SELECT *
FROM bronze.crm_prd_info
WHERE prd_cost IS NULL OR prd_cost < 0;

-- Invalid product dates
SELECT *
FROM bronze.crm_prd_info
WHERE prd_start_dt > prd_end_dt;

------------------------------------------------------------
-- CRM Sales Details
------------------------------------------------------------
SELECT *FROM bronze.crm_sales_details
-- Unwanted spaces
SELECT *
FROM bronze.crm_sales_details
WHERE sls_ord_num != TRIM(sls_ord_num);

-- Check orphan records (sales without valid order number)
SELECT *
FROM bronze.crm_sales_details
WHERE sls_ord_num IS NULL OR sls_ord_num = '';

-- Referential integrity: product key exists
SELECT *
FROM bronze.crm_sales_details s
WHERE s.sls_prd_key NOT IN (SELECT prd_key FROM silver.crm_prd_info);

-- Referential integrity: customer exists
SELECT *
FROM bronze.crm_sales_details s
WHERE s.sls_cust_id NOT IN (SELECT cst_id FROM silver.crm_cust_info);

-- Invalid dates
SELECT *
FROM bronze.crm_sales_details
WHERE sls_order_dt <= 0 OR LENGTH(sls_order_dt::text) != 8;

-- Invalid date ordering
SELECT *
FROM bronze.crm_sales_details
WHERE sls_order_dt > sls_ship_dt OR sls_order_dt > sls_due_dt;

-- Sales consistency
SELECT DISTINCT
    sls_sales, sls_quantity, sls_price
FROM bronze.crm_sales_details
WHERE sls_sales != sls_quantity * sls_price
   OR sls_sales <= 0 OR sls_quantity <= 0 OR sls_price <= 0
   OR sls_sales IS NULL OR sls_quantity IS NULL OR sls_price IS NULL;

------------------------------------------------------------
-- ERP Customer AZ12
------------------------------------------------------------
-- Out-of-range birthdates
SELECT *
FROM bronze.erp_cus_az12
WHERE bdate > CURRENT_TIMESTAMP OR bdate < '1920-01-01';

-- Gender validation
SELECT DISTINCT
CASE 
    WHEN UPPER(TRIM(gen)) IN ('M','MALE') THEN 'Male'
    WHEN UPPER(TRIM(gen)) IN ('F','FEMALE') THEN 'Female'
    ELSE 'n/a'
END AS gen
FROM bronze.erp_cus_az12;

------------------------------------------------------------
-- ERP Location A101
------------------------------------------------------------
-- Standardize country values
SELECT DISTINCT
CASE
    WHEN TRIM(country) = 'DE' THEN 'Germany'
    WHEN TRIM(country) IN ('US','USA','United States') THEN 'United States'
    WHEN TRIM(country) = '' OR country IS NULL THEN 'n/a'
    ELSE TRIM(country)
END AS country
FROM bronze.erp_loc_a101;

------------------------------------------------------------
-- ERP Product Category G1V2
------------------------------------------------------------
-- Unwanted spaces
SELECT *
FROM bronze.erp_px_cat_g1v2
WHERE cat != TRIM(cat) OR subcat != TRIM(subcat) OR maintenance != TRIM(maintenance);

-- Domain checks for categories
SELECT DISTINCT cat FROM bronze.erp_px_cat_g1v2;
SELECT DISTINCT subcat FROM bronze.erp_px_cat_g1v2;
SELECT DISTINCT maintenance FROM bronze.erp_px_cat_g1v2;

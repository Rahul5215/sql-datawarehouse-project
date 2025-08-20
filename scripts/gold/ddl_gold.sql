-- ========================================================
-- Project: Data Warehouse Views (Star Schema)
-- Purpose:
--  This script creates views for the gold layer in the data warehouse.
--  The gold layer represents the final dimensions and fact tables (Star Schema)

--  Each view performs transformations and combines data from the silver layer
--  to produce a clean, enriched, and business-ready dataset.

--     - dim_customers : Customer details (personal + demographic + location)
--     - dim_products  : Product details (name, category, cost, etc.)
--     - fact_sales    : Sales transactions linked to customers & products
--
--   Usage:
--    These views transform operational data into analytics-ready tables 
--    that can be directly used for BI dashboards, reporting, and 
--    performance analysis.
-- ========================================================



-- ========================================================
-- Gold Layer: Dimension - Customers
-- ========================================================
DROP VIEW IF EXISTS gold.dim_customers;
CREATE VIEW gold.dim_customers AS
SELECT
    ROW_NUMBER() OVER(ORDER BY ci.cst_id) AS customer_key,
    ci.cst_id        AS customer_id,
    ci.cst_key       AS customer_number,
    ci.cst_firstname AS first_name,
    ci.cst_lastname  AS last_name,
    la.country       AS country,
    ci.cst_marital_status AS marital_status,
    CASE 
        WHEN ci.cst_gndr != 'n/a' THEN ci.cst_gndr 
        ELSE ca.gen
    END              AS gender,
    ca.bdate         AS birthdate,
    ci.cst_create_date AS create_date
FROM silver.crm_cust_info ci
LEFT JOIN silver.erp_cus_az12 ca
    ON ci.cst_key = ca.cid
LEFT JOIN silver.erp_loc_a101 la
    ON ci.cst_key = la.cid;


-- ========================================================
-- Gold Layer: Dimension - Products
-- ========================================================
DROP VIEW IF EXISTS gold.dim_products;
CREATE VIEW gold.dim_products AS
SELECT
    ROW_NUMBER() OVER(ORDER BY pi.prd_start_dt, pi.prd_key) AS product_key,
    pi.prd_id   AS product_id,
    pi.prd_key  AS product_number,
    pi.prd_nm   AS product_name,
    pi.cat_id   AS category_id,
    px.cat      AS category,
    px.subcat   AS subcategory,
    px.maintenance,
    pi.prd_cost AS product_cost,
    pi.prd_line AS product_line,
    pi.prd_start_dt AS start_date
FROM silver.crm_prd_info pi
LEFT JOIN silver.erp_px_cat_g1v2 px
    ON pi.cat_id = px.id
WHERE pi.prd_end_dt IS NULL;


-- ========================================================
-- Gold Layer: Fact - Sales
-- ========================================================
DROP VIEW IF EXISTS gold.fact_sales;
CREATE VIEW gold.fact_sales AS 
SELECT 
    sd.sls_ord_num AS order_number,
    gp.product_key,
    gc.customer_key,
    sd.sls_order_dt AS order_date,
    sd.sls_ship_dt  AS shipping_date,
    sd.sls_due_dt   AS due_date,
    sd.sls_sales    AS sales_amount,
    sd.sls_quantity AS quantity,
    sd.sls_price    AS price
FROM silver.crm_sales_details sd
LEFT JOIN gold.dim_customers gc
    ON sd.sls_cust_id = gc.customer_id
LEFT JOIN gold.dim_products gp
    ON sd.sls_prd_key = gp.product_number;


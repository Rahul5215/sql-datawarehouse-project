/*
Purpose:
--------
This stored procedure **silver.load_silver** is responsible for loading and transforming data 
from the Bronze Layer into the Silver Layer of the Data Warehouse.  

Key Features:
1. Truncates existing Silver tables before loading fresh data.
2. Applies data cleaning, standardization, and transformation rules such as:
   - Standardizing gender and marital status values.
   - Validating and formatting dates.
   - Ensuring sales and pricing consistency.
   - Normalizing product keys and category IDs.
   - Mapping country codes to full country names.
3. Tracks processing time for each table load (start, end, duration).
4. Provides detailed logs using `RAISE NOTICE` for monitoring the ETL flow.
5. Implements error handling with diagnostics for debugging in case of failures.

Tables Loaded:
- silver.crm_cust_info        (Customer information, deduplicated by latest record)
- silver.crm_prd_info         (Product details with cleaned keys and product lines)
- silver.crm_sales_details    (Sales transactions with validated dates and corrected sales amounts)
- silver.erp_loc_a101         (ERP customer locations with country name standardization)
- silver.erp_cus_az12         (ERP customer records with cleaned IDs, birthdates, and gender)
- silver.erp_px_cat_g1v2      (ERP product category mappings)

Note:
- This procedure ensures the Silver Layer always has **clean, consistent, and ready-to-use data** 
  for analytics and reporting.
- Execution time for each table is logged, as well as the overall job duration.
*/

CREATE OR REPLACE PROCEDURE silver.load_silver()
LANGUAGE plpgsql
AS $$
DECLARE
v_state  text;
v_msg    text;
v_detail text;
v_hint   text;
tbl_start_time TIMESTAMP;
tbl_end_time   TIMESTAMP;
tbl_duration   INTERVAL;
start_time     TIMESTAMP;
end_time       TIMESTAMP;
duration       INTERVAL;
BEGIN
    start_time := clock_timestamp();
    RAISE NOTICE'==============================================================';
	RAISE NOTICE'Loading Silver Layer';
	RAISE NOTICE'==============================================================';
	
	RAISE NOTICE'--------------------------------------------------------------';
	RAISE NOTICE'Loading CRM Tables';
	RAISE NOTICE'--------------------------------------------------------------';
	tbl_start_time := clock_timestamp();
	RAISE NOTICE '>> Truncating Table: silver.crm_cust_info';
	TRUNCATE TABLE silver.crm_cust_info;
	
	RAISE NOTICE '>> Inserting Data Into: silver.crm_cust_info';
	INSERT INTO silver.crm_cust_info (
	    cst_id,
	    cst_key,
	    cst_firstname,
	    cst_lastname,
	    cst_marital_status,
	    cst_gndr,
	    cst_create_date
	)
	SELECT
	    cst_id,
	    cst_key,
	    TRIM(cst_firstname),
	    TRIM(cst_lastname),
	    CASE 
	        WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'SINGLE'
	        WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'MARRIED'
	        ELSE 'n/a'
	    END AS cst_marital_status,
	    CASE 
	        WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'FEMALE'
	        WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'MALE'
	        ELSE 'n/a'
	    END AS cst_gndr,
	    cst_create_date 
	FROM (
	    SELECT
	        *,
	        ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag
	    FROM bronze.crm_cust_info
	    WHERE cst_id IS NOT NULL
	) t
	WHERE flag = 1;
	tbl_end_time := clock_timestamp();
	tbl_duration := tbl_end_time - tbl_start_time;
    RAISE NOTICE '...........................................................';
	RAISE NOTICE 'Loading Time For bronze.crm_cust_info : %', tbl_duration;
	RAISE NOTICE '...........................................................';
	
	-- ==============================================================
	-- CRM Product Info
	-- ==============================================================
	tbl_start_time := clock_timestamp();
	RAISE NOTICE '>> Truncating Table: silver.crm_prd_info';
	TRUNCATE TABLE silver.crm_prd_info;
	
	RAISE NOTICE '>> Inserting Data Into: silver.crm_prd_info';
	INSERT INTO silver.crm_prd_info (
	    prd_id,
	    prd_key,
	    cat_id,
	    prd_nm,
	    prd_cost,
	    prd_line,
	    prd_start_dt,
	    prd_end_dt
	)
	SELECT 
	    prd_id,
	    SUBSTRING(prd_key, 7, LENGTH(prd_key)) AS prd_key,                  -- Extracted product key
	    REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id,              -- Extracted category id
	    prd_nm,
	    COALESCE(prd_cost, 0) AS prd_cost,
	    CASE 
	        WHEN UPPER(TRIM(prd_line)) = 'R' THEN 'Road'
	        WHEN UPPER(TRIM(prd_line)) = 'M' THEN 'Mountain'
	        WHEN UPPER(TRIM(prd_line)) = 'S' THEN 'Other Sales'
	        WHEN UPPER(TRIM(prd_line)) = 'T' THEN 'Touring'
	        ELSE 'n/a'
	    END AS prd_line,
	    prd_start_dt,
	    LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt) - 1 AS prd_end_dt
	FROM bronze.crm_prd_info;
	tbl_end_time := clock_timestamp();
	duration := tbl_end_time - tbl_start_time;
	RAISE NOTICE '...........................................................';
	RAISE NOTICE 'Loading Time For bronze.crm_prd_info : %', tbl_duration;
	RAISE NOTICE '...........................................................';
	
	-- ==============================================================
	-- CRM Sales Details
	-- ==============================================================
	tbl_start_time := clock_timestamp();
	RAISE NOTICE '>> Truncating Table: silver.crm_sales_details';
	TRUNCATE TABLE silver.crm_sales_details;
	
	RAISE NOTICE '>> Inserting Data Into: silver.crm_sales_details';
	INSERT INTO silver.crm_sales_details (
	    sls_ord_num,
	    sls_prd_key,
	    sls_cust_id,
	    sls_order_dt,
	    sls_ship_dt,
	    sls_due_dt,
	    sls_sales,
	    sls_quantity,
	    sls_price
	)
	SELECT 
	    sls_ord_num,
	    sls_prd_key,
	    sls_cust_id,
	    CASE 
	        WHEN sls_order_dt::text = '0' OR LENGTH(sls_order_dt::text) != 8 THEN NULL
	        ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE)
	    END AS sls_order_dt,
	    CASE
	        WHEN sls_ship_dt::text = '0' OR LENGTH(sls_ship_dt::text) != 8 THEN NULL
	        ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE)
	    END AS sls_ship_dt,
	    CASE 
	        WHEN sls_due_dt::text = '0' OR LENGTH(sls_due_dt::text) != 8 THEN NULL
	        ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE)
	    END AS sls_due_dt,
	    CASE 
	        WHEN sls_sales <= 0 
	             OR sls_sales IS NULL 
	             OR sls_sales != sls_quantity * ABS(sls_price) 
	        THEN sls_quantity * ABS(COALESCE(sls_price, 0))
	        ELSE sls_sales
	    END AS sls_sales,
	    sls_quantity,
	    CASE 
	        WHEN sls_price <= 0 OR sls_price IS NULL 
	        THEN sls_sales / COALESCE(sls_quantity, 0)
	        ELSE sls_price
	    END AS sls_price
	FROM bronze.crm_sales_details;
	tbl_end_time := clock_timestamp();
	tbl_duration := tbl_end_time - tbl_start_time;
    RAISE NOTICE '...........................................................';
	RAISE NOTICE 'Loading Time For bronze.crm_sales_details : %', tbl_duration;
	RAISE NOTICE '...........................................................';
	


	RAISE NOTICE'--------------------------------------------------------------';
	RAISE NOTICE'Loading ERP Tables';
	RAISE NOTICE'--------------------------------------------------------------';
	-- ==============================================================
	-- ERP Location A101
	-- ==============================================================
	tbl_start_time := clock_timestamp();
	RAISE NOTICE '>> Truncating Table: silver.erp_loc_a101';
	TRUNCATE TABLE silver.erp_loc_a101;
	
	RAISE NOTICE '>> Inserting Data Into: silver.erp_loc_a101';
	INSERT INTO silver.erp_loc_a101
	SELECT
	    REPLACE(cid, '-', '') AS cid,
	    CASE
	        WHEN TRIM(country) = 'DE' THEN 'Germany'
	        WHEN TRIM(country) IN ('US', 'USA', 'United States') THEN 'United States'
	        WHEN TRIM(country) = '' OR country IS NULL THEN 'n/a'
	        ELSE TRIM(country)
	    END AS country
	FROM bronze.erp_loc_a101;
	tbl_end_time := clock_timestamp();
	tbl_duration := tbl_end_time - tbl_start_time;
    RAISE NOTICE '...........................................................';
	RAISE NOTICE 'Loading Time For bronze.erp_loc_a101 : %', tbl_duration;
	RAISE NOTICE '...........................................................';
	
	
	-- ==============================================================
	-- ERP Customer AZ12
	-- ==============================================================
	tbl_start_time := clock_timestamp();
	RAISE NOTICE '>> Truncating Table: silver.erp_cus_az12';
	TRUNCATE TABLE silver.erp_cus_az12;
	
	RAISE NOTICE '>> Inserting Data Into: silver.erp_cus_az12';
	INSERT INTO silver.erp_cus_az12 (
	    cid,
	    bdate,
	    gen
	)
	SELECT
	    CASE 
	        WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LENGTH(cid))
	        ELSE cid
	    END AS cid,
	    CASE
	        WHEN bdate > CURRENT_TIMESTAMP THEN NULL
	        ELSE bdate
	    END AS bdate,
	    CASE 
	        WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
	        WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
	        ELSE 'n/a'
	    END AS gen
	FROM bronze.erp_cus_az12;
	tbl_end_time := clock_timestamp();
	tbl_duration := tbl_end_time - tbl_start_time;
	RAISE NOTICE '...........................................................';
	RAISE NOTICE 'Loading Time For bronze.erp_cus_az12 : %', tbl_duration;
	RAISE NOTICE '...........................................................';
	
	-- ==============================================================
	-- ERP PX Category G1V2
	-- ==============================================================
	tbl_start_time := clock_timestamp();
	RAISE NOTICE '>> Truncating Table: silver.erp_px_cat_g1v2';
	TRUNCATE TABLE silver.erp_px_cat_g1v2;
	
	RAISE NOTICE '>> Inserting Data Into: silver.erp_px_cat_g1v2';
	INSERT INTO silver.erp_px_cat_g1v2 (
	    id,
	    cat,
	    subcat,
	    maintenance
	)
	SELECT
	    id,
	    cat,
	    subcat,
	    maintenance
	FROM bronze.erp_px_cat_g1v2;
 	tbl_end_time := clock_timestamp();
	tbl_duration := tbl_end_time - tbl_start_time;
	RAISE NOTICE '...........................................................';
	RAISE NOTICE 'Loading Time For bronze.erp_px_cat_g1v2 : %', tbl_duration;
	RAISE NOTICE '...........................................................';
	
	end_time := clock_timestamp();
	duration := tbl_end_time - tbl_start_time;
	RAISE NOTICE '=================================================';
	RAISE NOTICE 'Total Loading Time: %', duration;
	RAISE NOTICE '=================================================';

    EXCEPTION WHEN OTHERS THEN
	  GET STACKED DIAGNOSTICS
	  v_state  = RETURNED_SQLSTATE,
	  v_msg    = MESSAGE_TEXT,
	  v_detail = PG_EXCEPTION_DETAIL,
	  v_hint   = PG_EXCEPTION_DETAIL;

	  RAISE NOTICE 'Error Code   : %', v_state;
      RAISE NOTICE 'Error Message: %', v_msg;
      RAISE NOTICE 'Detail       : %', COALESCE(v_detail, 'N/A');
      RAISE NOTICE 'Hint         : %', COALESCE(v_hint, 'N/A');
	
END;
$$;

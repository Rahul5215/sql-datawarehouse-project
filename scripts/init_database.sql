
/*
=======================================================================================================
CREATE DATABASE AND SCHEMA
=======================================================================================================

Purpose of this Script:

    This SQL script sets up the foundational structure of a Data Warehouse using the medallion architecture:
    
    ==>> Schemas:
    
    bronze – Stores raw, unprocessed data from source systems (CRM & ERP).
    
    silver – Intended for cleaned and enriched data (to be added later).
    
    gold – Intended for aggregated, analytics-ready data (to be added later).
    
    ==>> Bronze Tables:
    
    -Customer Info (bronze_crm_cust_info) – Stores raw customer data from CRM.
    
    -Product Info (bronze_crm_prd_info) – Stores raw product data from CRM.
    
    -Sales Details (bronze_crm_sales_details) – Stores raw transactional sales data.
    
    -ERP Customer (bronze_erp_cus_az12) – Stores ERP customer details.
    
    -ERP Location (bronze_erp_loc_a101) – Stores ERP customer location information.
    
    -ERP Product Category (bronze_erp_px_cat_g1v2) – Stores ERP product categorization data.

  Goal: To establish the bronze layer of the data warehouse, serving as the foundation for downstream transformations 
        in the silver and gold layers. This allows future ETL/ELT pipelines to clean, enrich, and aggregate data for 
        analysis and reporting.
*/








CREATE SCHEMA bronze;
CREATE SCHEMA silver;
CREATE SCHEMA gold;


CREATE TABLE bronze.bronze_crm_cust_info (
    cst_id INT,      
    cst_key VARCHAR(50),     
    cst_firstname VARCHAR(50),
    cst_lastname VARCHAR(50),
    cst_marital_status VARCHAR(50), 
    cst_gndr VARCHAR(20),
    cst_create_date DATE
);


CREATE TABLE bronze.bronze_crm_prd_info (
    prd_id INT,        
    prd_key VARCHAR(50),      
    prd_nm VARCHAR(50),      
    prd_cost INT,  
    prd_line VARCHAR(50),             
    prd_start_dt DATE,        
    prd_end_dt DATE                     
);
CREATE TABLE bronze.bronze_crm_sales_details (
    sls_ord_num VARCHAR(50),
    sls_prd_key VARCHAR(50),
    sls_cust_id INT,
    sls_order_dt INT,
    sls_ship_dt INT,
    sls_due_dt INT,
    sls_sales INT,
    sls_quantity INT,
    sls_price INT
);

CREATE TABLE bronze.bronze_erp_cus_az12 (
    cid VARCHAR(50),
    bdate DATE,
    gen VARCHAR(10)
);

CREATE TABLE bronze.bronze_erp_loc_a101 (
    cid VARCHAR(50),
    country VARCHAR(50)
);
CREATE TABLE bronze.bronze_erp_px_cat_g1v2 (
    id VARCHAR(50),
    cat VARCHAR(50),
    subcat VARCHAR(50),
    maintenance VARCHAR(100)
);


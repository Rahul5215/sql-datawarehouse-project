/*
Purpose:
--------
This script creates the **Silver Layer schema** tables for the Data Warehouse.  
It ensures old versions of the tables are dropped (if they exist) and new ones are created with the required structure.  
The tables capture customer, product, sales, ERP, and category-related information.  

Tables Created:
1. silver.crm_cust_info        - Stores customer master information.
2. silver.crm_prd_info         - Stores product details including category, cost, and lifecycle dates.
3. silver.crm_sales_details    - Stores sales order information such as order date, shipment, and pricing details.
4. silver.erp_cus_az12         - Stores ERP customer details (ID, birthdate, gender).
5. silver.erp_loc_a101         - Stores ERP customer location information.
6. silver.erp_px_cat_g1v2      - Stores ERP product category and subcategory mapping.
*/

DROP TABLE IF EXISTS silver.crm_cust_info; 
CREATE TABLE silver.crm_cust_info (
    cst_id INT,      
    cst_key VARCHAR(50),     
    cst_firstname VARCHAR(50),
    cst_lastname VARCHAR(50),
    cst_marital_status VARCHAR(50), 
    cst_gndr VARCHAR(20),
    cst_create_date DATE,
	dwh_create_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);


DROP TABLE IF EXISTS silver.crm_prd_info; 
CREATE TABLE silver.crm_prd_info (
    prd_id INT,        
    prd_key VARCHAR(50),
	cat_id VARCHAR(50),
    prd_nm VARCHAR(50),      
    prd_cost INT,  
    prd_line VARCHAR(50),             
    prd_start_dt DATE,        
    prd_end_dt DATE,
	dwh_create_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);


DROP TABLE IF EXISTS silver.crm_sales_details; 
CREATE TABLE silver.crm_sales_details (
    sls_ord_num VARCHAR(50),
    sls_prd_key VARCHAR(50),
    sls_cust_id INT,
    sls_order_dt DATE,
    sls_ship_dt DATE,
    sls_due_dt DATE,
    sls_sales INT,
    sls_quantity INT,
    sls_price INT,
	dwh_create_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP

);

DROP TABLE IF EXISTS silver.erp_cus_az12; 
CREATE TABLE silver.erp_cus_az12 (
    cid VARCHAR(50),
    bdate DATE,
    gen VARCHAR(10),
	dwh_create_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP

);

DROP TABLE IF EXISTS silver.erp_loc_a101; 
CREATE TABLE silver.erp_loc_a101 (
    cid VARCHAR(50),
    country VARCHAR(50),
	dwh_create_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP

);

DROP TABLE IF EXISTS silver.erp_px_cat_g1v2; 
CREATE TABLE silver.erp_px_cat_g1v2 (
    id VARCHAR(50),
    cat VARCHAR(50),
    subcat VARCHAR(50),
    maintenance VARCHAR(100),
	dwh_create_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP

);

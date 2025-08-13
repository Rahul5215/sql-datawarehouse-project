

/*
====================================================================================================================
Purpose of bronze_load_bronze() Procedure: This PL/pgSQL procedure automates the loading of raw data into the Bronze
                                           layer of a Data Warehouse following the medallion architecture.
====================================================================================================================

Key Functions:

1.Truncate & Load Tables: >> Clears existing data from all bronze tables before loading fresh CSV data.

                          >> Handles CRM and ERP source data separately.


2.Data Sources: >> CRM Tables: bronze_crm_cust_info, bronze_crm_prd_info, bronze_crm_sales_details

                >> ERP Tables: bronze_erp_cus_az12, bronze_erp_loc_a101, bronze_erp_px_cat_g1v2

                >> CSV files are read from local directories for each table.


3.Execution Tracking: >> Measures and logs load time for each table individually.
                      >> Logs total loading time for the entire procedure.

4.Error Handling: Captures and logs detailed error messages, hints, and diagnostics if any issues occur during the load process.

Goal:  To provide a reliable, repeatable process for ingesting raw source data into the bronze layer, forming the foundation
       for downstream silver (cleaned/enriched) and gold (analytics-ready) layers.
*/










CREATE OR REPLACE PROCEDURE bronze_load_bronze()
LANGUAGE plpgsql
AS $$
DECLARE
    v_state   text;
    v_msg     text;
    v_detail  text;
    v_hint    text;
	start_time TIMESTAMP;
    end_time TIMESTAMP;
    duration INTERVAL;
	tbl_start TIMESTAMP;
    tbl_end TIMESTAMP;
    tbl_duration INTERVAL;
	
BEGIN
	    start_time := clock_timestamp();
        RAISE NOTICE '=================================================';
        RAISE NOTICE 'Loading Bronze Layer';
        RAISE NOTICE '=================================================';

        RAISE NOTICE '-------------------------------------------------';
        RAISE NOTICE 'Loading CRM Tables';
        RAISE NOTICE '-------------------------------------------------';

        -- 01 - bronze_crm_cust_info
		tbl_start:=clock_timestamp();
        RAISE NOTICE '>> Truncating Table: bronze_crm_cust_info';
        TRUNCATE TABLE bronze.bronze_crm_cust_info;

        RAISE NOTICE '>> Inserting Data Into: bronze_crm_cust_info';
		
        EXECUTE $cmd$
        COPY bronze.bronze_crm_cust_info
        FROM 'A:/sql-data-warehouse-project/datasets/source_crm/cust_info.csv'
        DELIMITER ','
        CSV HEADER
        $cmd$;
		tbl_end:=clock_timestamp();
		tbl_duration:=tbl_end - tbl_start;

	    RAISE NOTICE '........................................................';
		RAISE NOTICE '>>Loading Time for bronze_crm_cust_info:%',tbl_duration;
		RAISE NOTICE '........................................................';

        -- 02 - bronze_crm_prd_info
		tbl_start:=clock_timestamp();
        RAISE NOTICE '>> Truncating Table: bronze_crm_prd_info';
        TRUNCATE TABLE bronze.bronze_crm_prd_info;

        RAISE NOTICE '>> Inserting Data Into: bronze_crm_prd_info';
        EXECUTE $cmd$
        COPY bronze.bronze_crm_prd_info
        FROM 'A:/sql-data-warehouse-project/datasets/source_crm/prd_info.csv'
        DELIMITER ','
        CSV HEADER
        $cmd$;

		tbl_end:=clock_timestamp();
		tbl_duration:=tbl_end - tbl_start;

		RAISE NOTICE '........................................................';
		RAISE NOTICE '>>Loading Time for bronze_crm_prd_info:%',tbl_duration;
		RAISE NOTICE '........................................................';

        -- 03 - bronze_crm_sales_details
		tbl_start:=clock_timestamp();
        RAISE NOTICE '>> Truncating Table: bronze_crm_sales_details';
        TRUNCATE TABLE bronze.bronze_crm_sales_details;

        RAISE NOTICE '>> Inserting Data Into: bronze_crm_sales_details';
        EXECUTE $cmd$
        COPY bronze.bronze_crm_sales_details
        FROM 'A:/sql-data-warehouse-project/datasets/source_crm/sales_details.csv'
        DELIMITER ','
        CSV HEADER
        $cmd$;

		tbl_end:=clock_timestamp();
		tbl_duration:=tbl_end - tbl_start;

		RAISE NOTICE '..........................................................';
		RAISE NOTICE '>>Loading Time for bronze_crm_sales_details:%',tbl_duration;
		RAISE NOTICE '..........................................................';



        RAISE NOTICE '-------------------------------------------------';
        RAISE NOTICE 'Loading ERP Tables';
        RAISE NOTICE '-------------------------------------------------';

        -- 04 - bronze_erp_cus_az12
		tbl_start:=clock_timestamp();
        RAISE NOTICE '>> Truncating Table: bronze_erp_cus_az12';
        TRUNCATE TABLE bronze.bronze_erp_cus_az12;

        RAISE NOTICE '>> Inserting Data Into: bronze_erp_cus_az12';
        EXECUTE $cmd$
        COPY bronze.bronze_erp_cus_az12
        FROM 'A:/sql-data-warehouse-project/datasets/source_erp/CUST_AZ12.csv'
        DELIMITER ','
        CSV HEADER
        $cmd$;

		tbl_end:=clock_timestamp();
		tbl_duration:=tbl_end - tbl_start;

		RAISE NOTICE '............................................................';
		RAISE NOTICE '>>Loading Time for bronze.bronze_erp_cus_az12:%',tbl_duration;
		RAISE NOTICE '............................................................';

        -- 05 - bronze_erp_loc_a101
		tbl_start:=clock_timestamp();
        RAISE NOTICE '>> Truncating Table: bronze_erp_loc_a101';
        TRUNCATE TABLE bronze.bronze_erp_loc_a101;

        RAISE NOTICE '>> Inserting Data Into: bronze_erp_loc_a101';
        EXECUTE $cmd$
        COPY bronze.bronze_erp_loc_a101
        FROM 'A:/sql-data-warehouse-project/datasets/source_erp/LOC_A101.csv'
        DELIMITER ','
        CSV HEADER
        $cmd$;

	    tbl_end:=clock_timestamp();
		tbl_duration:=tbl_end - tbl_start;

		RAISE NOTICE '............................................................';
		RAISE NOTICE '>>Loading Time for bronze.bronze_erp_loc_a101:%',tbl_duration;
		RAISE NOTICE '............................................................';

        -- 06 - bronze_erp_px_cat_g1v2
		tbl_start:=clock_timestamp();
        RAISE NOTICE '>> Truncating Table: bronze_erp_px_cat_g1v2';
        TRUNCATE TABLE bronze.bronze_erp_px_cat_g1v2;

        RAISE NOTICE '>> Inserting Data Into: bronze_erp_px_cat_g1v2';
        EXECUTE $cmd$
        COPY bronze.bronze_erp_px_cat_g1v2
        FROM 'A:/sql-data-warehouse-project/datasets/source_erp/PX_CAT_G1V2.csv'
        DELIMITER ','
        CSV HEADER
        $cmd$;

		tbl_end:=clock_timestamp();
		tbl_duration:=tbl_end - tbl_start;

		RAISE NOTICE '...............................................................';
		RAISE NOTICE '>>Loading Time for bronze.bronze_erp_px_cat_g1v2:%',tbl_duration;
		RAISE NOTICE '...............................................................';



		end_time := clock_timestamp();
	    duration := end_time - start_time;
	    RAISE NOTICE '=================================================';
	    RAISE NOTICE 'Total Loading Time: %', duration;
	    RAISE NOTICE '=================================================';

    EXCEPTION WHEN OTHERS THEN
        GET STACKED DIAGNOSTICS
            v_state = RETURNED_SQLSTATE,
            v_msg   = MESSAGE_TEXT,
            v_detail = PG_EXCEPTION_DETAIL,
            v_hint   = PG_EXCEPTION_HINT;

        RAISE NOTICE 'Error Code   : %', v_state;
        RAISE NOTICE 'Error Message: %', v_msg;
        RAISE NOTICE 'Detail       : %', COALESCE(v_detail, 'N/A');
        RAISE NOTICE 'Hint         : %', COALESCE(v_hint, 'N/A');
END;
$$;


CALL bronze_load_bronze();

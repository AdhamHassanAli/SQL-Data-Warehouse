CREATE OR REPLACE PROCEDURE bronze.load_bronze()
LANGUAGE plpgsql
AS $$
DECLARE
    start_time TIMESTAMP;
    end_time TIMESTAMP;
    batch_start_time TIMESTAMP;
    batch_end_time TIMESTAMP;
BEGIN
    batch_start_time := NOW();
    RAISE NOTICE '================================================';
    RAISE NOTICE 'Loading Bronze Layer';
    RAISE NOTICE '================================================';

    RAISE NOTICE '------------------------------------------------';
    RAISE NOTICE 'Loading CRM Tables';
    RAISE NOTICE '------------------------------------------------';

    -- crm_cust_info
    start_time := NOW();
    RAISE NOTICE '>> Truncating Table: bronze.crm_cust_info';
    TRUNCATE TABLE bronze.crm_cust_info;
    RAISE NOTICE '>> Loading Data Into: bronze.crm_cust_info';
    COPY bronze.crm_cust_info
    FROM '/csv/cust_info.csv'
    WITH (FORMAT csv, HEADER true);
    end_time := NOW();
    RAISE NOTICE '>> Load Duration: % seconds', EXTRACT(SECOND FROM (end_time - start_time));

    -- crm_prd_info
    start_time := NOW();
    RAISE NOTICE '>> Truncating Table: bronze.crm_prd_info';
    TRUNCATE TABLE bronze.crm_prd_info;
    RAISE NOTICE '>> Loading Data Into: bronze.crm_prd_info';
    COPY bronze.crm_prd_info
    FROM '/csv/prd_info.csv'
    WITH (FORMAT csv, HEADER true);
    end_time := NOW();
    RAISE NOTICE '>> Load Duration: % seconds', EXTRACT(SECOND FROM (end_time - start_time));

    -- crm_sales_details
    start_time := NOW();
    RAISE NOTICE '>> Truncating Table: bronze.crm_sales_details';
    TRUNCATE TABLE bronze.crm_sales_details;
    RAISE NOTICE '>> Loading Data Into: bronze.crm_sales_details';
    COPY bronze.crm_sales_details
    FROM '/csv/sales_details.csv'
    WITH (FORMAT csv, HEADER true);
    end_time := NOW();
    RAISE NOTICE '>> Load Duration: % seconds', EXTRACT(SECOND FROM (end_time - start_time));

    -- ERP TABLES
    RAISE NOTICE '------------------------------------------------';
    RAISE NOTICE 'Loading ERP Tables';
    RAISE NOTICE '------------------------------------------------';

    -- erp_loc_a101
    start_time := NOW();
    TRUNCATE TABLE bronze.erp_loc_a101;
    COPY bronze.erp_loc_a101
    FROM '/csv/loc_a101.csv'
    WITH (FORMAT csv, HEADER true);
    end_time := NOW();
    RAISE NOTICE '>> Load Duration: % seconds', EXTRACT(SECOND FROM (end_time - start_time));

    -- erp_cust_az12
    start_time := NOW();
    TRUNCATE TABLE bronze.erp_cust_az12;
    COPY bronze.erp_cust_az12
    FROM '/csv/cust_az12.csv'
    WITH (FORMAT csv, HEADER true);
    end_time := NOW();
    RAISE NOTICE '>> Load Duration: % seconds', EXTRACT(SECOND FROM (end_time - start_time));

    -- erp_px_cat_g1v2
    start_time := NOW();
    TRUNCATE TABLE bronze.erp_px_cat_g1v2;
    COPY bronze.erp_px_cat_g1v2
    FROM '/csv/px_cat_g1v2.csv'
    WITH (FORMAT csv, HEADER true);
    end_time := NOW();
    RAISE NOTICE '>> Load Duration: % seconds', EXTRACT(SECOND FROM (end_time - start_time));

    batch_end_time := NOW();
    RAISE NOTICE '==========================================';
    RAISE NOTICE 'Loading Bronze Layer is Completed';
    RAISE NOTICE 'Total Load Duration: % seconds', EXTRACT(SECOND FROM (batch_end_time - batch_start_time));
    RAISE NOTICE '==========================================';

EXCEPTION WHEN OTHERS THEN
    RAISE NOTICE '==========================================';
    RAISE NOTICE 'ERROR OCCURRED DURING LOADING BRONZE LAYER';
    RAISE NOTICE 'Error: %', SQLERRM;
    RAISE NOTICE '==========================================';
END;
$$;

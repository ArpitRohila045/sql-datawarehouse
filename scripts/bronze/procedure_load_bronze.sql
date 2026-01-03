/*
===============================================================================
Stored Procedure: Load Bronze Layer (Source -> Bronze)
===============================================================================
Script Purpose:
    This stored procedure loads data into the 'bronze' schema from external CSV files. 
    It performs the following actions:
    - Truncates the bronze tables before loading data.
    - Uses the `BULK INSERT` command to load data from csv Files to bronze tables.

Parameters:
    None. 
	  This stored procedure does not accept any parameters or return any values.

Usage Example:
    EXEC bronze.load_bronze;
===============================================================================
*/


CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN
    BEGIN TRY
            
        DECLARE @start_time DATETIME, @end_time DATETIME;
        PRINT '========================================================';
        PRINT 'LOADING BRONZE LAYER';
        PRINT '========================================================';

        PRINT '--------------------------------------------------------';
        PRINT 'LOADING FORM CRM';
        PRINT ''

        -- Delete the full table data
        PRINT '>>>TRUNCATING bronze.crm_cust_info'
        TRUNCATE TABLE bronze.crm_cust_info;
        -- Bulk inserting into crm_cust_info
        PRINT '+++LOADING INTO bronze.crm_cust_info FROM source_crm\cust_info.csv';

        SET @start_time = GETDATE();
        BULK INSERT bronze.crm_cust_info
        FROM 'C:\Users\hp\Desktop\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
        WITH (
            -- Defining the insert rules 
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK -- CREATE LOCK ON THE TABLE WHILE LOADING, IMPROVES PERFORMANCE 
        )
        SET @end_time = GETDATE();

        PRINT 'LOADED IN : ' + CAST(DATEDIFF(microsecond, @start_time, @end_time) AS NVARCHAR) + ' micro-seconds';  
        -- VALIDATING THE FULL LOAD 
        -- SELECT COUNT(*) FROM bronze.crm_cust_info

        PRINT ''
        SET @start_time = GETDATE();
        -- INSERTING INTO CRM_PROD_INFO
        PRINT '>>>TRUNCATIG bronze.crm_prd_info';
        TRUNCATE TABLE bronze.crm_prd_info;
        PRINT '+++LOADING INTO bronze.crm_prd_info FROM source_crm\prd_info.csv'
        BULK INSERT bronze.crm_prd_info
        FROM 'C:\Users\hp\Desktop\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
        WITH (
           FIRSTROW = 2,
           FIELDTERMINATOR = ',',
           TABLOCK
        )
        SET @end_time = GETDATE();

        PRINT 'LOADED IN : ' + CAST(DATEDIFF(microsecond, @start_time, @end_time) AS NVARCHAR) + ' micro-seconds';
        -- SELECT * FROM bronze.crm_prd_info


        -- inserting into bronze.crm_sales_details
        PRINT ''
        SET @start_time = GETDATE();
        PRINT '>>>TRUNCATING bronze.crm_sales_details';
        TRUNCATE TABLE bronze.crm_sales_details;
        PRINT '+++LOADING INOT bronze.crm_sales_details FROM source_crm\sales_details.csv'
        BULK INSERT bronze.crm_sales_details
        FROM 'C:\Users\hp\Desktop\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        )
        SET @end_time = GETDATE();
        PRINT 'LOADED IN : ' + CAST(DATEDIFF(microsecond, @start_time, @end_time) AS NVARCHAR) + ' micro-seconds';


        -- SELECT COUNT(*) FROM bronze.crm_sales_details

        PRINT 'LAODING SUCESSFULL SOURCE: CRM DONE!'
        PRINT ''
        PRINT '--------------------------------------------------------';
        PRINT 'LOADING FORM ERP';
        PRINT ''

        -- INSERTING INTO bronze.erp_loc_a101
        PRINT '>>>TRUNCATING bronze.erp_loc_a101';
        TRUNCATE TABLE bronze.erp_loc_a101;
        SET @start_time = GETDATE();
        PRINT '+++LOADING INTO bronze.erp_loc_a101 FROM source_erp\loc_a101.csv'
        BULK INSERT bronze.erp_loc_a101
        FROM 'C:\Users\hp\Desktop\sql-data-warehouse-project\datasets\source_erp\loc_a101.csv'
        WITH (
           FIRSTROW = 2,
           FIELDTERMINATOR = ',',
           TABLOCK
        )
        SET @end_time = GETDATE();
        PRINT 'LOADED IN : ' + CAST(DATEDIFF(microsecond, @start_time, @end_time) AS NVARCHAR) + ' micro-seconds';

        PRINT ''
        PRINT '>>>TRUNCATING bronze.erp_cust_az12';
        TRUNCATE TABLE bronze.erp_cust_az12;
        SET @start_time = GETDATE();
        PRINT '++LOADING INTO bronze.erp_cust_az12 FROM source_erp\cust_az12.csv';
        BULK INSERT bronze.erp_cust_az12
        FROM 'C:\Users\hp\Desktop\sql-data-warehouse-project\datasets\source_erp\cust_az12.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        )
        SET @end_time = GETDATE();
        PRINT 'LOADED IN : ' + CAST(DATEDIFF(microsecond, @start_time, @end_time) AS NVARCHAR) + ' micro-seconds';

        PRINT ''
        PRINT '>>>TRUNCATION bronze.erp_px_cat_g1v2';
        TRUNCATE TABLE bronze.erp_px_cat_g1v2;
        SET @start_time = GETDATE();
        PRINT '+++LOADING INTO bronze.erp_px_cat_g1v2 FROM source_erp\px_cat_g1v2.csv'
        BULK INSERT bronze.erp_px_cat_g1v2
        FROM 'C:\Users\hp\Desktop\sql-data-warehouse-project\datasets\source_erp\px_cat_g1v2.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        )
        SET @end_time = GETDATE();
        PRINT 'LOADED IN : ' + CAST(DATEDIFF(microsecond, @start_time, @end_time) AS NVARCHAR) + ' micro-seconds';

    END TRY

    BEGIN CATCH
        PRINT '======================================================='
        PRINT '>> AN ERROR OCCURED WHILE LOADING BRONZE LAYAER';
        PRINT 'ERROR MESSAGE : ' + ERROR_MESSAGE();
        PRINT 'ERROR NUMBER : ' + CAST(ERROR_NUMBER() AS NVARCHAR);
        PRINT 'OCCURED ON : ' + CAST(GETDATE() AS NVARCHAR);
        PRINT '======================================================='
    END CATCH
END

/*
===============================================================================
Stored Procedure: Load Bronze Layer (Source -> Bronze)
===============================================================================
Script Purpose:
    This stored procedure loads data into the 'bronze' schema from external CSV files. 
    It performs the following actions:
	- Drop the bronze tables if exist and recreate them'
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
	DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME
	BEGIN TRY
		SET @batch_start_time = GETDATE();
		PRINT'=============================================================';
		PRINT' Loading Bronze Layer';
		PRINT'=============================================================';

		PRINT'-------------------------------------------------------------';
		PRINT' Loading POS Tables';
		PRINT'-------------------------------------------------------------';

		SET @start_time = GETDATE();
		PRINT'>> Truncating Table: bronze.pos_transaction';
		TRUNCATE TABLE  bronze.pos_transaction;

		PRINT'>> Inserting Data Into Table: bronze.pos_transaction';
		BULK INSERT bronze.pos_transaction
		FROM 'C:\DATA PROJECTS\Python\Quantium\transaction_data.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT'>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT'>> ------------';

		SET @start_time = GETDATE();
		PRINT'>> Truncating Table: bronze.pos_purchase_behavior';
		TRUNCATE TABLE bronze.pos_purchase_behavior

		PRINT'>> Inserting Data Into Table: bronze.pos_purchase_behavior'
		BULK INSERT bronze.pos_purchase_behavior
		FROM 'C:\DATA PROJECTS\Python\Quantium\purchase_behaviour.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		PRINT'>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT'>> ------------';
		SET @batch_end_time = GETDATE();
		PRINT'=============================================================';
		PRINT'Loading Bronze Layer is Completed';
		PRINT'	- Total Load Duration: ' +  CAST (DATEDIFF(second, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' seconds';
		PRINT'=============================================================';

	END TRY
	BEGIN CATCH
		PRINT'=============================================================';
		PRINT'ERROR OCCURED DURING LOADING BRONZE LAYER';
		PRINT'Error Message' + ERROR_MESSAGE();
		PRINT'Error Message' + CAST(ERROR_NUMBER() AS NVARCHAR);
		PRINT'Error Message' + CAST(ERROR_STATE() AS NVARCHAR);
		PRINT'=============================================================';
	END CATCH
END

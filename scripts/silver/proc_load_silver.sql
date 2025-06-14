/*
===========================================================================================
Stored Procedure: Load Silver Layer (Bronze --> Silver)
===========================================================================================
Script Purpose:
  This stored procedure performs the ETL (Extract, Transform, Load) process to populate the 'silver'
  schema tables from the bronze schema.
Action performed:
  - Truncates silver tables
  - Inserts transformed and cleansed data from the bronze into silver tables.

Parameters:
  None.
  This stored procedure does not accept any parameters or return any values.

Usage example:
  EXEC silver.load_silver;
===========================================================================================
*/
CREATE OR ALTER PROCEDURE silver.load_silver AS
BEGIN
	DECLARE @StartTime DATETIME, @EndTime DATETIME, @Batch_Start_Time DATETIME, @Batch_End_Time DATETIME
	BEGIN TRY
		Set @Batch_Start_Time = GETDATE();
		PRINT '=====================================================================';
		PRINT 'Loading Procedure';
		PRINT '=====================================================================';

		PRINT '------------------------------------------------------------------------';
		PRINT 'Loading CRM Tables';
		PRINT '------------------------------------------------------------------------';

		Set @StartTime = GETDATE();
		PRINT '>> Truncating Table: [silver].[crm_cust_info]';
		TRUNCATE TABLE [silver].[crm_cust_info]
		PRINT '>> Inserting Data Into: [silver].[crm_cust_info]';
		INSERT INTO [silver].[crm_cust_info](
			cst_id,
			cst_key,
			cst_firstname,
			cst_lastname,
			cst_marital_status,
			cst_gndr,
			cst_create_date)

		SELECT 
			cst_id,
			cst_key,
			TRIM(cst_firstname) as cst_firstname,
			TRIM(cst_lastname) as cst_lastname,
			CASE	
				WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
				WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
				ELSE 'N/A' END AS cst_marital_status, --Normalize marital status values to readable format
			CASE	
				WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
				WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
			ELSE 'N/A' END AS cst_gndr,--Normalize gender values to readable format
			cst_create_date
		FROM (

			SELECT 
				*,
				ROW_NUMBER() OVER(PARTITION BY cst_id ORDER BY cst_create_date desc) as rank
			FROM [bronze].[crm_cust_info]
			WHERE cst_id IS NOT NULL
		)t WHERE rank = 1;--select the most recent customers
		SET @EndTime = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @StartTime, @EndTime) AS NVARCHAR) + ' seconds';
		PRINT '>> ---------------------------------------------------------------'


		Set @StartTime = GETDATE();
		PRINT '>> Truncating Table: [silver].[crm_prd_info]';
		TRUNCATE TABLE [silver].[crm_prd_info]
		PRINT '>> Inserting Data Into: [silver].[crm_prd_info]';
		INSERT INTO [silver].[crm_prd_info](
			prd_id,
			cat_id,
			prd_key,
			prd_nm,
			prd_cost,
			prd_line,
			prd_start_dt,
			prd_end_dt
		)
		SELECT
			prd_id,
			REPLACE(SUBSTRING(prd_key, 1,5),'-','_') as cat_id,--Extract category ID
			SUBSTRING(prd_key, 7,LEN(prd_key)) AS prd_key,-- extract product key
			prd_nm,
			ISNULL(prd_cost,0) AS prd_cost,-- handle missing infomation
			CASE UPPER(TRIM(prd_line))
				WHEN 'M' THEN 'Mountain'
				WHEN 'R' THEN 'Road'
				WHEN 'S' THEN 'Other Sales'
				WHEN 'T' THEN 'Touring'
				ELSE 'N/A' END AS prd_line, -- map product line codes to descriptive values
			CAST(prd_start_dt AS DATE) AS prd_start_dt,
			CAST(LEAD(prd_start_dt) OVER(PARTITION BY prd_key ORDER BY prd_start_dt) -1 AS DATE) AS prd_end_dt -- calculate end date as one day before the next start date
		FROM [bronze].[crm_prd_info];
		SET @EndTime = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @StartTime, @EndTime) AS NVARCHAR) + ' seconds';
		PRINT '>> ---------------------------------------------------------------'


		Set @StartTime = GETDATE();
		PRINT '>> Truncating Table: [silver].[crm_sales_details]';
		TRUNCATE TABLE [silver].[crm_sales_details]
		PRINT '>> Inserting Data Into: [silver].[crm_sales_details]';
		INSERT INTO [silver].[crm_sales_details](
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
				WHEN sls_order_dt = 0 OR LEN(sls_order_dt) != 8 THEN NULL
				ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE)
				END AS sls_order_dt,
			CASE	
				WHEN sls_ship_dt = 0 OR LEN(sls_ship_dt) != 8 THEN NULL
				ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE)
				END AS sls_ship_dt,
			CASE	
				WHEN sls_due_dt = 0 OR LEN(sls_due_dt) != 8 THEN NULL
				ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE)
				END AS sls_due_dt,
			CASE WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price) THEN sls_quantity * ABS(sls_price)
				ELSE sls_sales END AS sls_sales,-- recalculating sales if original value is missing or incorrect
			sls_quantity,
			CASE WHEN sls_price IS NULL OR sls_price <= 0 THEN sls_sales/NULLIF(sls_quantity,0)
				ELSE sls_price END AS sls_price -- Derive price if original value is invalid
		FROM bronze.crm_sales_details;
		SET @EndTime = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @StartTime, @EndTime) AS NVARCHAR) + ' seconds';
		PRINT '>> ---------------------------------------------------------------'

		PRINT '------------------------------------------------------------------------';
		PRINT 'Loading ERP Tables';
		PRINT '------------------------------------------------------------------------';

		Set @StartTime = GETDATE();
		PRINT '>> Truncating Table: [silver].[erp_cust_az12]';
		TRUNCATE TABLE [silver].[erp_cust_az12]
		PRINT '>> Inserting Data Into: [silver].[erp_cust_az12]';
		INSERT INTO [silver].[erp_cust_az12](
			cid,
			bdate,
			gen
		)
		SELECT 
			CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid,4, LEN(cid))-- remove 'NAS' prefix if present
				ELSE cid END as cid,
			CASE WHEN bdate > GETDATE() THEN NULL	
				ELSE bdate END AS bdate,-- set future birthdates to NULL
			CASE WHEN UPPER(TRIM(gen)) IN ('F','FEMALE') THEN 'Female'
				WHEN UPPER(TRIM(gen)) IN ('M','MALE') THEN 'Male'
				ELSE 'N/A' END AS gen -- Normalize fender values and handle unknown cases
		FROM [bronze].[erp_cust_az12];
		SET @EndTime = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @StartTime, @EndTime) AS NVARCHAR) + ' seconds';
		PRINT '>> ---------------------------------------------------------------'

		Set @StartTime = GETDATE();
		PRINT '>> Truncating Table: [silver].[erp_loc_a101]';
		TRUNCATE TABLE [silver].[erp_loc_a101]
		PRINT '>> Inserting Data Into: [silver].[erp_loc_a101]';
		INSERT INTO [silver].[erp_loc_a101](
			cid,
			cntry)
		SELECT DISTINCT
			REPLACE(cid, '-','') AS cid,
			CASE 
				WHEN TRIM(cntry) IN ('USA','US','United States') THEN 'United States'
				WHEN TRIM(cntry) = 'DE'  THEN 'Germany'
				WHEN TRIM(cntry) ='' OR TRIM(cntry) IS NULL THEN 'N/A'
				ELSE cntry END AS cntry -- normalize missing data or invalid data
		FROM [bronze].[erp_loc_a101];
		SET @EndTime = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @StartTime, @EndTime) AS NVARCHAR) + ' seconds';
		PRINT '>> ---------------------------------------------------------------'

		Set @StartTime = GETDATE();
		PRINT '>> Truncating Table: [silver].[erp_px_cat_g1v2]';
		TRUNCATE TABLE [silver].[erp_px_cat_g1v2]
		PRINT '>> Inserting Data Into: [silver].[erp_px_cat_g1v2]';
		INSERT INTO [silver].[erp_px_cat_g1v2](
			id,
			cat,
			subcat,
			maintenance)
		SELECT 
			id,
			cat,
			subcat,
			maintenance
		FROM [bronze].[erp_px_cat_g1v2];
		SET @EndTime = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @StartTime, @EndTime) AS NVARCHAR) + ' seconds';
		PRINT '>> ---------------------------------------------------------------'

		SET @Batch_End_Time = GETDATE();
		PRINT '===================================================================='
		PRINT 'Loading Silver Layer is Completed'
		PRINT ' - Total Load Duration: '+ CAST(DATEDIFF(second, @Batch_Start_Time, @Batch_End_Time) AS NVARCHAR) + ' seconds';
		PRINT '===================================================================='
		END TRY
		BEGIN CATCH
			PRINT '==========================================================================';
			PRINT 'ERROR OCCURED DURING LOADING BRONZE LAYER';
			PRINT 'Error Message'+ ERROR_MESSAGE();
			PRINT 'Error Message'+ CAST(ERROR_NUMBER() AS NVARCHAR);
			PRINT 'Error Message'+ CAST(ERROR_STATE() AS NVARCHAR);
			PRINT '==========================================================================';
		END CATCH

END

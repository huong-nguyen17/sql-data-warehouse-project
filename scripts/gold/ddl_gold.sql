/*
======================================================================
DDL Script: Create Gold Views
=======================================================================
Script Purpose:
This script creates views for the gold layer- the data warehouse
The gold layer represents the final dimension and fact tables (star schema)

Each view performs transformations and combines data from the silver layer
to produce a clean, enriched, and business-ready dataset.

Usage:
  - these views can be queried directly for analytics and reporting.
*/

-- no duplicates, no invalid data
-- naming convention
-- may arrange the columns to look more convenient

-- create customer dimension view
CREATE VIEW gold.dim_customers AS (
	SELECT 
		ROW_NUMBER() OVER(ORDER BY cst_id) AS customer_key,-- surrogate key: system-generated unique identifier assigned to each record in a table
		ci.cst_id as customer_id,
		ci.cst_key as customer_number,
		ci.cst_firstname as firstname,
		ci.cst_lastname as lastname,
		la.cntry as country,
		ci.cst_marital_status as marital_status,
		CASE	
			WHEN ci.cst_gndr != 'n/a' THEN ci.cst_gndr -- crm is the master for gender info
			ELSE COALESCE(ca.gen, 'N/A') END AS gender, -- if there is no match result in ca table, replace 'n/a' value
		ca.bdate as birthdate,
		ci.cst_create_date as create_date
	FROM [silver].[crm_cust_info] AS ci
	LEFT JOIN [silver].[erp_cust_az12] AS ca
	ON ci.cst_key= ca.cid
	LEFT JOIN [silver].[erp_loc_a101] AS la
	ON ci.cst_key=la.cid
)

-- create product dimension view
-- only consider current products where prd_end_dt is NULL
-- decide if this view can be dim or fact??

CREATE VIEW gold.dim_product AS(
	SELECT 
		ROW_NUMBER() OVER(ORDER BY pn.prd_start_dt, pn.prd_key ) as product_key,
		pn.prd_id as product_id,
		pn.prd_key as product_number,
		pn.prd_nm as product_name,
		pn.cat_id as category_id,
		pc.cat as category,
		pc.subcat as subcategory,
		pc.maintenance,
		pn.prd_cost as cost,
		pn.prd_line as product_line,
		pn.prd_start_dt as start_date
	FROM [silver].[crm_prd_info] pn
	LEFT JOIN	[silver].[erp_px_cat_g1v2] pc
	ON pn.cat_id=pc.id
	WHERE pn.prd_end_dt IS NULL -- filter out all historical data
)

-- create view for sale
-- data looking 
CREATE VIEW gold.fact_sales AS (
	SELECT
		sd.sls_ord_num as order_number,
		pr.product_key,
		cu.customer_id,
		sd.sls_order_dt as order_date,
		sd.sls_ship_dt as shipping_date,
		sd.sls_due_dt as due_date,
		sd.sls_sales as sale_amount,
		sd.sls_quantity as quantity,
		sd.sls_price
	FROM [silver].[crm_sales_details] AS sd
	LEFT JOIN [gold].[dim_product] AS pr
	ON sd.sls_prd_key = pr.product_number
	LEFT JOIN [gold].[dim_customers] AS cu
	ON	sd.sls_cust_id = cu.customer_id
)

SELECT *
FROM bronze.crm_cust_info
LIMIT 1000;
-- customer information

SELECT *
FROM bronze.crm_prd_info
LIMIT 1000;
-- product information

SELECT *
FROM bronze.crm_sales_details
LIMIT 1000;
-- transaction records on sales and orders
-- mergeable on sls_cust_id with crm_cust_info's cst_id
-- mergeable on sls_prd_key with crm_prd_info's prd_key

SELECT *
FROM bronze.erp_cust_az12
LIMIT 1000;
-- birthdate and gender info on customers
-- mergeable on cid with crm_cust_info's cst_key: key contains overlapping characters with cid

SELECT *
FROM bronze.erp_loc_a101
LIMIT 1000;
-- location data on customers
-- mergeable on cid with crm_cust_info's cst_key, but we will need to remove any hyphens first

SELECT *
FROM bronze.erp_px_cat_g1v2
LIMIT 1000;
-- categorization data on items
-- mergeable on id with crm_prd_info's prd_key: prd_key contains the various id values as part of the key

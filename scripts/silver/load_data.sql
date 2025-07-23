-- copying over all our tables from the bronze schema into silver
DROP TABLE IF EXISTS silver.crm_cust_info;
CREATE TABLE silver.crm_cust_info AS
SELECT * FROM bronze.crm_cust_info;


DROP TABLE IF EXISTS silver.crm_prd_info;
CREATE TABLE silver.crm_prd_info AS
SELECT * FROM bronze.crm_prd_info;


DROP TABLE IF EXISTS silver.crm_sales_details;
CREATE TABLE silver.crm_sales_details AS
SELECT * FROM bronze.crm_sales_details;


DROP TABLE IF EXISTS silver.erp_cust_az12;
CREATE TABLE silver.erp_cust_az12 AS
SELECT * FROM bronze.erp_cust_az12;


DROP TABLE IF EXISTS silver.erp_loc_a101;
CREATE TABLE silver.erp_loc_a101 AS
SELECT * FROM bronze.erp_loc_a101;

DROP TABLE IF EXISTS silver.erp_px_cat_g1v2;
CREATE TABLE silver.erp_px_cat_g1v2 AS
SELECT * FROM bronze.erp_px_cat_g1v2;

select * from silver.crm_cust_info
-- verify that the data was loaded properly

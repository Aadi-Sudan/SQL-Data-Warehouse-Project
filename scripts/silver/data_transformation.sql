-- removing any existing entries from our silver layer and inserting cleaned data

TRUNCATE TABLE silver.crm_cust_info;

insert into silver.crm_cust_info (
	cst_id,
	cst_key,
	cst_firstname,
	cst_lastname,
	cst_marital_status,
	cst_gndr,
	cst_create_date
)
-- what we have done: trimmed all varchar datatypes, made marital_status and gender clearer, and removed any null and duplicate id's by focusing only on latest creation date
select cst_id, TRIM(cst_key), 
TRIM(cst_firstname) as cst_firstname, TRIM(cst_lastname) as cst_lastname, 
case when UPPER(TRIM(cst_marital_status)) = 'M' then 'Married'
	 when UPPER(TRIM(cst_marital_status)) = 'S' then 'Single' 
	 else 'N/A'
end cst_marital_status, 
case when UPPER(TRIM(cst_gndr)) = 'F' then 'Female'
	 when UPPER(TRIM(cst_gndr)) = 'M' then 'Male' 
	 else 'N/A'
end cst_gndr,
cst_create_date
from (
select *,
row_number() over (partition by cst_id order by cst_create_date desc) as flag_last
from bronze.crm_cust_info) where cst_id is not null and flag_last = 1

TRUNCATE TABLE silver.crm_prd_info;
ALTER TABLE silver.crm_prd_info ADD COLUMN cat_id VARCHAR(50);
insert into silver.crm_prd_info(
	prd_id,
	cat_id,
	prd_key,
	prd_nm,
	prd_cost,
	prd_line,
	prd_start_dt,
	prd_end_dt
)
-- what we have done: derived cat_id and prd_key columns by transforming the existing prd_key column, handled missing information in prd_cost, normalized prd_line, and enriched prd_end_dt to properly represent the end of a product period
select prd_id, 
replace(SUBSTRING(prd_key, 1, 5), '-', '_') as cat_id,
SUBSTRING(prd_key, 7, LENGTH(prd_key)) as prd_key,
prd_nm,
COALESCE(prd_cost, 0) as prd_cost,
case when UPPER(TRIM(prd_line)) = 'M' then 'Mountain'
	 when UPPER(TRIM(prd_line)) = 'R' then 'Road' 
	 when UPPER(TRIM(prd_line)) = 'S' then 'Other Sales'
	 when UPPER(TRIM(prd_line)) = 'T' then 'Touring'
	 else 'N/A'
end prd_line,
prd_start_dt,
LEAD(prd_start_dt) over (partition by prd_key order by prd_start_dt)-1 as prd_end_dt
from bronze.crm_prd_info

TRUNCATE TABLE silver.crm_sales_details;
ALTER TABLE silver.crm_sales_details
ALTER COLUMN sls_order_dt TYPE DATE USING TO_DATE(sls_order_dt::TEXT, 'YYYYMMDD');

ALTER TABLE silver.crm_sales_details
ALTER COLUMN sls_ship_dt TYPE DATE USING TO_DATE(sls_ship_dt::TEXT, 'YYYYMMDD');

ALTER TABLE silver.crm_sales_details
ALTER COLUMN sls_due_dt TYPE DATE USING TO_DATE(sls_due_dt::TEXT, 'YYYYMMDD');

insert into silver.crm_sales_details (
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
-- what we have done: changed dates from int to date datatypes, ensured that quantity, price, and sales data line up with one another
select sls_ord_num,
sls_prd_key,
sls_cust_id,
case when sls_order_dt = 0 or length(cast(sls_order_dt as VARCHAR)) != 8 then NULL
	 else cast(cast(sls_order_dt as VARCHAR) as DATE)
end as sls_order_dt,
case when sls_ship_dt = 0 or length(cast(sls_ship_dt as VARCHAR)) != 8 then NULL
	 else cast(cast(sls_ship_dt as VARCHAR) as DATE)
end as sls_ship_dt,
case when sls_due_dt = 0 or length(cast(sls_due_dt as VARCHAR)) != 8 then NULL
	 else cast(cast(sls_due_dt as VARCHAR) as DATE)
end as sls_due_dt,
case when sls_sales is null or sls_sales <= 0 or sls_sales != sls_quantity * ABS(sls_price)
	 then sls_quantity * ABS(sls_price)
	 else sls_sales
end as sls_sales,
sls_quantity,
case when sls_price is null or sls_price = 0 then sls_sales / nullif(sls_quantity, 0)
	 else sls_price
end as sls_price
from bronze.crm_sales_details

truncate table silver.erp_cust_az12;
insert into silver.erp_cust_az12(
	cid,
	bdate,
	gen
)
-- what we did: ensure cid matches the cst_key from cst_cust_info to make for easy merging, cleaned extreme dates, standardized gender
select
case when cid like 'NAS%' then SUBSTRING(cid, 4, LENGTH(cid))
	 else cid
end as cid,
case when bdate > CURRENT_DATE then null
	 else bdate
end as bdate,
case when UPPER(TRIM(gen)) in ('F', 'FEMALE') then 'Female'
	 when UPPER(TRIM(gen)) in ('M', 'MALE') then 'Male'
	 else 'N/A'
end as gen
from bronze.erp_cust_az12

truncate table silver.erp_loc_a101;
insert into silver.erp_loc_a101 (
	cid,
	cntry
)
-- what we did: cleaned cid to match with cst_id from cst_cust_info to ensure easy merging, standardized country
select replace(cid, '-', '') as cid,
case when TRIM(cntry) = 'DE' then 'Germany'
	 when TRIM(cntry) in ('US', 'USA') then 'United States'
	 when TRIM(cntry) = '' or cntry is null then 'N/A'
	 else TRIM(cntry)
end as cntry
from bronze.erp_loc_a101

truncate table silver.erp_px_cat_g1v2;
insert into silver.erp_px_cat_g1v2(
	id,
	cat,
	subcat,
	maintenance
)
-- what we did: nothing! This table has excellent data quality and does not require any cleaning!
select id,
cat,
subcat,
maintenance
from bronze.erp_px_cat_g1v2 

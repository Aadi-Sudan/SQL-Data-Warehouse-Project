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
insert into silver.crm_prd_info(
	prd_id,
	prd_key,
	prd_nm,
	prd_cost,
	prd_line,
	prd_start_dt,
	prd_end_dt
)

select prd_id, 

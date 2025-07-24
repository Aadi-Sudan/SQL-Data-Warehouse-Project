-- code for creating our views from our cleaned and joined tables

create view gold.dim_customers AS
select 
ROW_NUMBER() over (order by cst_id) as customer_key,
cci.cst_id as customer_id, 
cci.cst_key as customer_number,
cci.cst_firstname as first_name,
cci.cst_lastname as last_name,
ela.cntry as country,
cci.cst_marital_status as marital_status,
case when cci.cst_gndr != 'N/A' then cci.cst_gndr
	 else coalesce(eca.gen, 'n/a')
end as gender,
eca.bdate as birthdate,
cci.cst_create_date as create_date
from silver.crm_cust_info cci
left join silver.erp_cust_az12 eca
on cci.cst_key = eca.cid
left join silver.erp_loc_a101 ela
on cci.cst_key = ela.cid

create view gold.dim_products AS
select
ROW_NUMBER() over (order by cpi.prd_start_dt, cpi.prd_id) as product_key,
cpi.prd_id as product_id,
cpi.prd_key as product_number,
cpi.prd_nm as product_name,
cpi.cat_id as category_id,
epx.cat as category,
epx.subcat as subcategory,
epx.maintenance as maintenance,
cpi.prd_cost as cost,
cpi.prd_line as line,
cpi.prd_start_dt as start_date
from silver.crm_prd_info cpi
left join silver.erp_px_cat_g1v2 epx
on cpi.cat_id = epx.id
where cpi.prd_end_dt is null

create view gold.fact_sales AS
select
sd.sls_ord_num as order_number,
pr.product_key,
cu.customer_key,
sd.sls_order_dt as order_date,
sd.sls_ship_dt as ship_date,
sd.sls_due_dt as due_date,
sd.sls_sales as sales_amount,
sd.sls_quantity as quantity,
sd. sls_price as price
from silver.crm_sales_details sd
left join gold.dim_products pr
on sd.sls_prd_key = pr.product_number
left join gold.dim_customers cu
on sd.sls_cust_id = cu.customer_id

select * from gold.fact_sales fs
left join  gold.dim_customers dc on dc.customer_key = fs.customer_key
left join gold.dim_products dp on dp.product_key = fs.product_key

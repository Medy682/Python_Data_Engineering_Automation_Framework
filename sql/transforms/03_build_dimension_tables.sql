USE DATABASE customer_analytics;
USE SCHEMA Analytics;


--dimension customer table
CREATE OR REPLACE TABLE Analytics.dim_customer AS 
select 
id as customer_id,
name as customer_name,
country
from staging.customers_clean;

--dimension products table 
create or replace table Analytics.dim_products as 
select
id as product_id,
name as product_name,
category
from staging.products_clean;

--dimension date table
create or replace table Analytics.dim_date as 
select 
sales_date,
year(sales_date) as year,
month(sales_date) as month,
day( sales_date) as day
from staging.sales_clean;


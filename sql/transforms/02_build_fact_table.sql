
USE DATABASE customer_analytics;
USE SCHEMA Analytics;

CREATE OR REPLACE TABLE Analytics.fact_sales as
select 
customer_id,
product_id,
quantity,
sales_date,
total_amount
from staging.sales_clean;
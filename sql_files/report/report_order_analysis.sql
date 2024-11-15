create or replace table your_dataset.report_order_analysis as
select
fo.* except(seller_id, product_id, customer_id)
, dp.*
, du.customer_id
, du.customer_city
, du.customer_state
, du.geolocation_city customer_geo_city
, du.geolocation_zip_code_prefix customer_zip_code_prefix
, ds.seller_id
, ds.seller_city
, ds.seller_state
, ds.geolocation_city seller_geo_city
, ds.geolocation_zip_code_prefix seller_zip_code_prefix
from `your_dataset.fact_orders` fo 
left join `your_dataset.dim_products` dp on fo.product_id = dp.product_id
left join `your_dataset.dim_users` du on du.customer_id = fo.customer_id
left join `your_dataset.dim_sellers` ds on ds.seller_id = fo.seller_id
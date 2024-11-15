create or replace table your_dataset.dim_users as
with c as (
  select
  *
  , row_number() over(partition by customer_id order by ingestion_date desc) rn
  from `your_dataset.customers_dataset` c
)
, g as (
  select
  *
  , row_number() over(partition by geolocation_zip_code_prefix order by ingestion_date desc) rn
  from `your_dataset.geolocation_dataset` g
)
select
c.customer_id
, c.customer_unique_id
, c.customer_city
, c.customer_state
, g.geolocation_city
, g.geolocation_lat
, g.geolocation_lng
, g.geolocation_zip_code_prefix
from c
left join g on c.customer_zip_code_prefix = g.geolocation_zip_code_prefix
where c.rn = 1
and g.rn = 1
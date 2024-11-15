create or replace table your_dataset.dim_sellers as
with s as (
  select
  *
  , row_number() over(partition by seller_id order by ingestion_date desc) rn
  from `your_dataset.sellers_dataset` c
)
, g as (
  select
  *
  , row_number() over(partition by geolocation_zip_code_prefix order by ingestion_date desc) rn
  from `your_dataset.geolocation_dataset` g
)
select
s.seller_id
, s.seller_city
, s.seller_state
, g.geolocation_city
, g.geolocation_lat
, g.geolocation_lng
, g.geolocation_zip_code_prefix
from s
left join g on s.seller_zip_code_prefix = g.geolocation_zip_code_prefix
where s.rn = 1
and g.rn = 1
create or replace table your_dataset.dim_products as
with p as (
  select
  *
  , row_number() over(partition by product_id order by ingestion_date desc) rn
  from `your_dataset.products_dataset` c
)
, pcn as (
  select
  *
  , row_number() over(partition by product_category_name order by ingestion_date desc) rn
  from `your_dataset.product_category_name_translation` g
)
select
p.* except(ingestion_date)
, pcn.product_category_name_english
from p
left join pcn on p.product_category_name = pcn.product_category_name
where p.rn = 1
and pcn.rn = 1
create or replace table your_dataset.fact_orders as
with o as (
  select
  *
  , row_number() over(partition by order_id order by ingestion_date desc) rn
  from `your_dataset.orders_dataset`
)
, oi as (
  select
  *
  , row_number() over(partition by order_id, order_item_id order by ingestion_date desc) rn
  from `your_dataset.order_items_dataset`
)
, op as (
  select
  *
  , row_number() over(partition by order_id order by ingestion_date desc) rn
  from `your_dataset.order_payments_dataset`
)
, orev as (
  select
  *
  , row_number() over(partition by review_id order by ingestion_date desc) rn
  from `your_dataset.order_reviews_dataset`
)
select
o.order_id
, o.customer_id
, o.order_status
, o.order_purchase_timestamp
, o.order_approved_at
, o.order_delivered_carrier_date
, o.order_delivered_customer_date
, o.order_estimated_delivery_date
, oi.order_item_id
, oi.seller_id
, oi.product_id
, oi.price
, oi.freight_value
, oi.shipping_limit_date
, op.payment_type
, op.payment_installments
, op.payment_value
, orev.review_score
, orev.review_comment_title
, orev.review_comment_message
, orev.review_creation_date
, orev.review_answer_timestamp
from o
left join oi on o.order_id = oi.order_id
left join op on op.order_id = o.order_id
left join orev on orev.order_id = o.order_id
where o.rn = 1
and oi.rn = 1
and op.rn = 1
and orev.rn = 1
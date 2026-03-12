{{
  config(
    materialized='table'
  )
}}

with orders as (
  select * from {{ ref('stg_orders') }}
),

payments_agg as (
  select
    order_id,
    sum(amount) as total_amount
  from {{ ref('stg_payments') }}
  group by order_id
),

final as (
  select
    orders.id as order_id,
    orders.user_id as customer_id,
    orders.order_date,
    orders.status,
    coalesce(payments_agg.total_amount, 0) as amount
  from orders
  left join payments_agg on orders.id = payments_agg.order_id
)

select * from final

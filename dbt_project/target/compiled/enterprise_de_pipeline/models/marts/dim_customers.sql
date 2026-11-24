with orders as (
    select * from ANALYTICS_DB.MARTS.fct_orders
)

select
    customer_id,
    count(distinct order_id) as total_orders,
    sum(case when order_status != 'CANCELLED' then order_amount else 0 end) as lifetime_value,
    min(event_timestamp) as first_order_at,
    max(event_timestamp) as last_order_at
from orders
group by customer_id
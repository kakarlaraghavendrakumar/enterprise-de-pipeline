
  create or replace   view ANALYTICS_DB.STAGING.stg_orders
  
   as (
    with source as (
    select * from ANALYTICS_DB.RAW.raw_orders
),

cleaned as (
    select
        order_id,
        customer_id,
        product_id,
        upper(trim(order_status)) as order_status,
        cast(amount as number(10, 2)) as order_amount,
        event_timestamp,
        ingestion_date
    from source
    where order_id is not null
)

select * from cleaned
  );


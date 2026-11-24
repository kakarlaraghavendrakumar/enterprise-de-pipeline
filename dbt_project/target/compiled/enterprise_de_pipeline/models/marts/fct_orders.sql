

select
    order_id,
    customer_id,
    product_id,
    order_status,
    order_amount,
    event_timestamp,
    ingestion_date,
    current_timestamp() as dbt_updated_at
from ANALYTICS_DB.STAGING.stg_orders


    where ingestion_date >= (select coalesce(max(ingestion_date), '1900-01-01') from ANALYTICS_DB.MARTS.fct_orders)

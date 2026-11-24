{{
    config(
        materialized='incremental',
        unique_key='order_id',
        incremental_strategy='merge'
    )
}}

select
    order_id,
    customer_id,
    product_id,
    order_status,
    order_amount,
    event_timestamp,
    ingestion_date,
    current_timestamp() as dbt_updated_at
from {{ ref('stg_orders') }}

{% if is_incremental() %}
    where ingestion_date >= (select coalesce(max(ingestion_date), '1900-01-01') from {{ this }})
{% endif %}
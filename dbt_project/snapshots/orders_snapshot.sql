{% snapshot orders_snapshot %}

{{
    config(
      target_schema='SNAPSHOTS',
      unique_key='order_id',
      strategy='timestamp',
      updated_at='event_timestamp'
    )
}}

select * from {{ ref('stg_orders') }}

{% endsnapshot %}
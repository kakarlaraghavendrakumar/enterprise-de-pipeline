
      begin;
    merge into "ANALYTICS_DB"."SNAPSHOTS"."ORDERS_SNAPSHOT" as DBT_INTERNAL_DEST
    using "ANALYTICS_DB"."SNAPSHOTS"."ORDERS_SNAPSHOT__dbt_tmp" as DBT_INTERNAL_SOURCE
    on DBT_INTERNAL_SOURCE.dbt_scd_id = DBT_INTERNAL_DEST.dbt_scd_id

    when matched
     
       and DBT_INTERNAL_DEST.dbt_valid_to is null
     
     and DBT_INTERNAL_SOURCE.dbt_change_type in ('update', 'delete')
        then update
        set dbt_valid_to = DBT_INTERNAL_SOURCE.dbt_valid_to

    when not matched
     and DBT_INTERNAL_SOURCE.dbt_change_type = 'insert'
        then insert ("ORDER_ID", "CUSTOMER_ID", "PRODUCT_ID", "ORDER_STATUS", "ORDER_AMOUNT", "EVENT_TIMESTAMP", "INGESTION_DATE", "DBT_UPDATED_AT", "DBT_VALID_FROM", "DBT_VALID_TO", "DBT_SCD_ID")
        values ("ORDER_ID", "CUSTOMER_ID", "PRODUCT_ID", "ORDER_STATUS", "ORDER_AMOUNT", "EVENT_TIMESTAMP", "INGESTION_DATE", "DBT_UPDATED_AT", "DBT_VALID_FROM", "DBT_VALID_TO", "DBT_SCD_ID")

;
    commit;
  
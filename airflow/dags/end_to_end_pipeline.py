import os
from datetime import datetime, timedelta
import snowflake.connector
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

default_args = {
    "owner": "data_engineering",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}


def load_parquet_to_snowflake(execution_date: str) -> None:
    """
    Idempotently loads a single day's Parquet partition into Snowflake
    using PUT (to internal stage) + COPY INTO.
    """
    local_parquet_glob = f"/opt/airflow/data/bronze/orders/dt={execution_date}/*.parquet"

    conn = snowflake.connector.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        password=os.environ["SNOWFLAKE_PASSWORD"],
        role=os.environ["SNOWFLAKE_ROLE"],
        warehouse=os.environ["SNOWFLAKE_WAREHOUSE"],
        database=os.environ["SNOWFLAKE_DATABASE"],
        schema=os.environ["SNOWFLAKE_SCHEMA"],
    )

    try:
        with conn.cursor() as cur:
            # 1. Upload local Parquet files to Snowflake internal stage
            cur.execute(
                f"PUT file://{local_parquet_glob} @RAW_ORDERS_STAGE/{execution_date}/ "
                "AUTO_COMPRESS=FALSE OVERWRITE=TRUE"
            )

            # 2. Idempotency: remove any existing rows for this partition before reloading
            cur.execute(
                "DELETE FROM RAW_ORDERS WHERE INGESTION_DATE = %s",
                (execution_date,),
            )

            # 3. Bulk load from stage into the raw table
            cur.execute(
                f"""
                COPY INTO RAW_ORDERS
                FROM @RAW_ORDERS_STAGE/{execution_date}/
                FILE_FORMAT = (TYPE = 'PARQUET')
                MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
                FORCE = TRUE
                """
            )
    finally:
        conn.close()


with DAG(
    dag_id="end_to_end_de_pipeline",
    default_args=default_args,
    description="Ingest -> PySpark Clean -> Snowflake Load -> dbt Transform & Test",
    schedule="@daily",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["system-design", "pyspark", "snowflake", "dbt"],
) as dag:

    # 1. Simulate raw source data (with duplicates/nulls) for the execution date
    ingest_raw_data = BashOperator(
        task_id="ingest_raw_data",
        bash_command="python /opt/airflow/src/ingestion/data_generator.py --date {{ ds }}",
    )

    # 2. Clean, deduplicate, route bad rows to quarantine, and write Parquet
    spark_bronze_to_stage = BashOperator(
        task_id="spark_bronze_to_stage",
        bash_command=(
            "spark-submit --master local[*] "
            "/opt/airflow/src/spark_jobs/bronze_to_stage.py --date {{ ds }}"
        ),
    )

    # 3. Load the cleaned Parquet partition into Snowflake
    load_to_snowflake = PythonOperator(
        task_id="load_to_snowflake",
        python_callable=load_parquet_to_snowflake,
        op_kwargs={"execution_date": "{{ ds }}"},
    )

    # 4. Run dbt models (Silver -> Gold) and snapshots (SCD Type 2)
    dbt_transform = BashOperator(
        task_id="dbt_transform",
        bash_command=(
            "cd /opt/airflow/dbt_project && "
            "dbt run --vars '{\"run_date\": \"{{ ds }}\"}' && "
            "dbt snapshot"
        ),
    )

    # 5. Enforce data quality gates on Gold tables
    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command="cd /opt/airflow/dbt_project && dbt test",
    )

    ingest_raw_data >> spark_bronze_to_stage >> load_to_snowflake >> dbt_transform >> dbt_test
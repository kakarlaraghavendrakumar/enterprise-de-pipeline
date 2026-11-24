import argparse
from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DecimalType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

VALID_STATUSES = ["PLACED", "SHIPPED", "DELIVERED", "CANCELLED"]

RAW_SCHEMA = StructType(
    [
        StructField("order_id", StringType(), True),
        StructField("customer_id", StringType(), True),
        StructField("product_id", StringType(), True),
        StructField("order_status", StringType(), True),
        StructField("amount", DecimalType(10, 2), True),
        StructField("event_timestamp", TimestampType(), True),
    ]
)


def process_orders(run_date: str) -> None:
    spark = (
        SparkSession.builder.appName(f"orders_bronze_to_stage_{run_date}")
        .config("spark.sql.parquet.writeLegacyFormat", "true")
        .getOrCreate()
    )

    raw_path = f"/opt/airflow/data/raw/orders/dt={run_date}/orders.jsonl"
    bronze_path = f"/opt/airflow/data/bronze/orders/dt={run_date}"
    quarantine_path = f"/opt/airflow/data/quarantine/orders/dt={run_date}"

    raw_df = spark.read.schema(RAW_SCHEMA).json(raw_path)

    # 1. Tag records failing data quality rules
    checked_df = raw_df.withColumn(
        "dq_error",
        F.when(F.col("order_id").isNull(), F.lit("NULL_ORDER_ID"))
        .when(F.col("customer_id").isNull(), F.lit("NULL_CUSTOMER_ID"))
        .when(F.col("amount").isNull() | (F.col("amount") <= 0), F.lit("INVALID_AMOUNT"))
        .when(~F.col("order_status").isin(VALID_STATUSES), F.lit("INVALID_STATUS"))
        .when(F.col("event_timestamp").isNull(), F.lit("NULL_TIMESTAMP"))
        .otherwise(F.lit(None)),
    )

    # 2. Route invalid rows to Dead Letter Queue (Quarantine)
    quarantine_df = checked_df.filter(F.col("dq_error").isNotNull()).withColumn(
        "quarantined_at", F.current_timestamp()
    )
    quarantine_count = quarantine_df.count()
    if quarantine_count > 0:
        quarantine_df.write.mode("overwrite").parquet(quarantine_path)

    # 3. Deduplicate valid rows: keep the latest event per order_id
    valid_df = checked_df.filter(F.col("dq_error").isNull()).drop("dq_error")

    window_spec = Window.partitionBy("order_id").orderBy(F.col("event_timestamp").desc())
    deduped_df = (
        valid_df.withColumn("rn", F.row_number().over(window_spec))
        .filter(F.col("rn") == 1)
        .drop("rn")
        .withColumn("ingestion_date", F.to_date(F.lit(run_date)))
    )

    # 4. Write clean Parquet partition for Snowflake COPY INTO
    clean_count = deduped_df.count()
    deduped_df.coalesce(1).write.mode("overwrite").parquet(bronze_path)

    print(
        f"[{run_date}] Summary -> Raw: {raw_df.count()} | "
        f"Clean (Deduped): {clean_count} | Quarantined: {quarantine_count}"
    )
    spark.stop()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", required=True, help="Execution date (YYYY-MM-DD)")
    args = parser.parse_args()
    process_orders(args.date)


if __name__ == "__main__":
    main()
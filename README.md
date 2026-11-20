### Featured Projects
- **[Enterprise End-to-End Data Engineering Pipeline](https://github.com/<your-username>/enterprise-de-pipeline)** — Containerized Medallion architecture (Bronze → Silver → Gold) using **Docker, Apache Airflow, PySpark, Snowflake, and dbt Core** with Dead Letter Queue (DLQ) quarantine, SCD Type 2 snapshots, and automated data quality gates.


# Enterprise End-to-End Data Engineering Pipeline

A production-style, containerized **Medallion Architecture (Bronze → Silver → Gold)** data pipeline built with **Docker, Apache Airflow, PySpark, Snowflake, and dbt Core**.

Designed to demonstrate modern data engineering system design: idempotent batch ingestion, schema enforcement, Dead Letter Queue (DLQ) quarantine for malformed records, incremental Star Schema modeling, SCD Type 2 history tracking, and automated data quality gates.

---

## Architecture

```mermaid
flowchart LR
    A[Python + Faker<br/>Raw Order Generator] -->|JSONL<br/>Valid + Dirty + Dups| B[PySpark Job<br/>Schema & Dedup]
    B -->|Invalid Rows| C[(Quarantine / DLQ<br/>Parquet)]
    B -->|Clean Partitioned<br/>Parquet| D[Snowflake Stage<br/>PUT + COPY INTO]
    D --> E[(Snowflake RAW<br/>RAW_ORDERS)]
    E --> F[dbt Staging / Silver<br/>stg_orders View]
    F --> G[(dbt Snapshot<br/>SCD Type 2)]
    F --> H[dbt Marts / Gold<br/>fct_orders + dim_customers]
    H --> I[dbt Test<br/>Automated DQ Gates]

    subgraph Orchestration [Apache Airflow in Docker Compose]
        A
        B
        D
        F
        I
    end

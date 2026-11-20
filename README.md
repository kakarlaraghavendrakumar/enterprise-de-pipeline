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


1. Prerequisites
Docker Desktop (with WSL 2 on Windows)

A Snowflake account

2. Configure Snowflake
Run the following setup in a Snowsight SQL Worksheet as ACCOUNTADMIN:
CREATE ROLE IF NOT EXISTS TRANSFORMER_ROLE;
GRANT ROLE TRANSFORMER_ROLE TO ROLE SYSADMIN;

CREATE WAREHOUSE IF NOT EXISTS COMPUTE_WH
  WAREHOUSE_SIZE = 'XSMALL' AUTO_SUSPEND = 60 AUTO_RESUME = TRUE;

CREATE DATABASE IF NOT EXISTS ANALYTICS_DB;
CREATE SCHEMA IF NOT EXISTS ANALYTICS_DB.RAW;
CREATE SCHEMA IF NOT EXISTS ANALYTICS_DB.STAGING;
CREATE SCHEMA IF NOT EXISTS ANALYTICS_DB.MARTS;
CREATE SCHEMA IF NOT EXISTS ANALYTICS_DB.SNAPSHOTS;

GRANT USAGE, OPERATE ON WAREHOUSE COMPUTE_WH TO ROLE TRANSFORMER_ROLE;
GRANT USAGE ON DATABASE ANALYTICS_DB TO ROLE TRANSFORMER_ROLE;
GRANT ALL PRIVILEGES ON ALL SCHEMAS IN DATABASE ANALYTICS_DB TO ROLE TRANSFORMER_ROLE;
GRANT ALL PRIVILEGES ON FUTURE TABLES IN DATABASE ANALYTICS_DB TO ROLE TRANSFORMER_ROLE;
GRANT ALL PRIVILEGES ON FUTURE VIEWS IN DATABASE ANALYTICS_DB TO ROLE TRANSFORMER_ROLE;

-- Grant role to your Snowflake user
GRANT ROLE TRANSFORMER_ROLE TO USER <YOUR_SNOWFLAKE_USER>;

USE ROLE TRANSFORMER_ROLE;
USE WAREHOUSE COMPUTE_WH;

CREATE STAGE IF NOT EXISTS ANALYTICS_DB.RAW.RAW_ORDERS_STAGE
  FILE_FORMAT = (TYPE = 'PARQUET');

CREATE TABLE IF NOT EXISTS ANALYTICS_DB.RAW.RAW_ORDERS (
    ORDER_ID VARCHAR,
    CUSTOMER_ID VARCHAR,
    PRODUCT_ID VARCHAR,
    ORDER_STATUS VARCHAR,
    AMOUNT NUMBER(10, 2),
    EVENT_TIMESTAMP TIMESTAMP_NTZ,
    INGESTION_DATE DATE
);


3. Configure Environment Variables
Create a .env file in the project root (never commit this file):

SNOWFLAKE_ACCOUNT=your_org-your_account
SNOWFLAKE_USER=your_snowflake_user
SNOWFLAKE_PASSWORD=your_snowflake_password
SNOWFLAKE_ROLE=TRANSFORMER_ROLE
SNOWFLAKE_WAREHOUSE=COMPUTE_WH
SNOWFLAKE_DATABASE=ANALYTICS_DB
SNOWFLAKE_SCHEMA=RAW

4. Launch the Pipeline

docker compose up --build -d


5. Trigger the DAG
Open the Airflow UI at http://localhost:8080 (default login: admin / admin).

Unpause end_to_end_de_pipeline and click Trigger DAG.

Verify the Gold layer in Snowflake:
SELECT COUNT(*) FROM ANALYTICS_DB.MARTS.FCT_ORDERS;
SELECT * FROM ANALYTICS_DB.MARTS.DIM_CUSTOMERS LIMIT 10;
SELECT * FROM ANALYTICS_DB.SNAPSHOTS.ORDERS_SNAPSHOT LIMIT 10;

# E-Commerce Batch ETL Pipeline

A daily batch ETL pipeline that ingests raw e-commerce order data, transforms it
with PySpark into a dimensional model, and loads curated marts into a warehouse —
orchestrated end-to-end with Apache Airflow.

## Architecture

```
 ┌────────────────────┐    ┌──────────────────────┐    ┌───────────────────────┐
 │  Raw CSV extract    │ -> │  PySpark transforms   │ -> │  Warehouse (SQLite     │
 │  (customers,         │    │  - clean & dedupe      │    │  locally / Snowflake   │
 │   products, orders,  │    │  - build fact_orders   │    │  in prod) + Parquet    │
 │   order_items)       │    │  - daily_revenue_mart  │    │  data lake             │
 └────────────────────┘    │  - top_products_mart  │    └───────────────────────┘
                              └──────────────────────┘
                       Orchestrated daily by an Airflow DAG
```

**Why this design:** the extract/transform/load steps are plain Python functions
operating on Spark DataFrames, independent of Airflow. That keeps the business
logic unit-testable (see `tests/`) and means swapping the local SQLite sink for
Snowflake/Redshift later is a one-file change in `src/load.py`.

## Tech Stack
PySpark · Apache Airflow · SQL (dimensional modeling) · SQLite (local warehouse stand-in for Snowflake) · pytest · Docker

## Project Structure
```
ecommerce-batch-etl/
├── src/
│   ├── generate_sample_data.py   # synthetic data generator (Faker)
│   ├── extract.py                # reads raw CSVs into Spark DataFrames
│   ├── transform.py              # cleaning + dimensional modeling logic
│   ├── load.py                   # writes Parquet + loads warehouse tables
│   └── pipeline.py                # wires extract -> transform -> load
├── dags/
│   └── ecommerce_etl_dag.py      # Airflow DAG definition
├── tests/
│   └── test_transform.py          # PySpark unit tests
├── docker-compose.yml             # local Airflow (LocalExecutor + Postgres)
└── requirements.txt
```

## Running locally (without Airflow)

```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt

# 1. Generate synthetic raw data
python src/generate_sample_data.py --num-customers 500 --num-orders 5000

# 2. Run the pipeline
python src/pipeline.py

# 3. Run tests
pytest tests/
```

This produces `data/processed/*.parquet` (fact/dim tables) and `data/warehouse.db`
(daily_revenue_mart, top_products_mart, dim_customer) you can query directly:

```bash
sqlite3 data/warehouse.db "select * from daily_revenue_mart limit 5;"
```

## Running with Airflow (Docker)

```bash
docker compose up airflow-init
docker compose up
```

Then open http://localhost:8080 (admin/admin) and trigger the `ecommerce_batch_etl` DAG.

## Migrating to a real cloud warehouse

`src/load.py` isolates the warehouse write. To target Snowflake instead of SQLite,
swap `write_to_warehouse` for a call using the `snowflake-connector-python` /
`spark-snowflake` connector with the same DataFrame inputs — no other module changes.

## Possible Extensions
- Add Great Expectations for richer data quality checks
- Partition Parquet output by `order_day` for incremental loads
- Add a `dbt` layer on top of the warehouse for SQL-based transformations

"""
Airflow DAG: orchestrates the daily e-commerce ETL.

    generate_sample_data (simulates upstream drop)
        -> run_etl_pipeline (extract -> transform -> load)
        -> data_quality_check

Place this file in your Airflow $AIRFLOW_HOME/dags directory, or run via
the provided docker-compose.yml which mounts it automatically.
"""
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

default_args = {
    "owner": "karthik",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

PROJECT_ROOT = "/opt/airflow/project"


def check_row_counts():
    import sqlite3

    conn = sqlite3.connect(f"{PROJECT_ROOT}/data/warehouse.db")
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM daily_revenue_mart")
    count = cur.fetchone()[0]
    conn.close()
    if count == 0:
        raise ValueError("Data quality check failed: daily_revenue_mart is empty")
    print(f"Data quality check passed: {count} rows in daily_revenue_mart")


with DAG(
    dag_id="ecommerce_batch_etl",
    description="Daily batch ETL for e-commerce orders -> dimensional warehouse",
    default_args=default_args,
    schedule_interval="@daily",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["etl", "spark", "portfolio"],
) as dag:

    generate_sample_data = BashOperator(
        task_id="generate_sample_data",
        bash_command=f"cd {PROJECT_ROOT} && python src/generate_sample_data.py --num-orders 2000",
    )

    run_etl_pipeline = BashOperator(
        task_id="run_etl_pipeline",
        bash_command=f"cd {PROJECT_ROOT} && python src/pipeline.py",
    )

    data_quality_check = PythonOperator(
        task_id="data_quality_check",
        python_callable=check_row_counts,
    )

    generate_sample_data >> run_etl_pipeline >> data_quality_check

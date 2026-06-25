"""
Pipeline entry point. Run end-to-end with:

    python src/pipeline.py --raw-dir data/raw --out-dir data/processed

This is the script Airflow's PythonOperator/BashOperator calls in
dags/ecommerce_etl_dag.py - keeping it as a standalone script means it
can be run and tested outside of Airflow too.
"""
import argparse

from extract import extract_all, get_spark_session
from load import write_parquet, write_to_warehouse
from transform import (
    build_daily_revenue_mart,
    build_fact_orders,
    build_top_products_mart,
    clean_customers,
    clean_products,
)


def run_pipeline(raw_dir: str, out_dir: str, db_path: str) -> None:
    spark = get_spark_session()
    raw = extract_all(spark, raw_dir)

    customers = clean_customers(raw["customers"])
    products = clean_products(raw["products"])
    fact_orders = build_fact_orders(raw["orders"], raw["order_items"])
    daily_revenue = build_daily_revenue_mart(fact_orders)
    top_products = build_top_products_mart(fact_orders, products)

    write_parquet(fact_orders, f"{out_dir}/fact_orders")
    write_parquet(customers, f"{out_dir}/dim_customer")
    write_parquet(products, f"{out_dir}/dim_product")

    write_to_warehouse(daily_revenue, "daily_revenue_mart", db_path)
    write_to_warehouse(top_products, "top_products_mart", db_path)
    write_to_warehouse(customers, "dim_customer", db_path)

    print("Pipeline run complete.")
    spark.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--raw-dir", default="data/raw")
    parser.add_argument("--out-dir", default="data/processed")
    parser.add_argument("--db-path", default="data/warehouse.db")
    args = parser.parse_args()
    run_pipeline(args.raw_dir, args.out_dir, args.db_path)

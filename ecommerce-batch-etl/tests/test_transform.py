"""
Unit tests for the transform layer using a local SparkSession.
Run with: pytest tests/
"""
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

import pytest
from pyspark.sql import SparkSession

from transform import build_daily_revenue_mart, build_fact_orders, clean_customers


@pytest.fixture(scope="module")
def spark():
    spark = SparkSession.builder.master("local[2]").appName("test").getOrCreate()
    yield spark
    spark.stop()


def test_clean_customers_dedupes_and_lowercases_email(spark):
    df = spark.createDataFrame(
        [
            ("c1", "Jane", "Doe", "JANE@EXAMPLE.COM", "2024-01-01"),
            ("c1", "Jane", "Doe", "JANE@EXAMPLE.COM", "2024-01-01"),
        ],
        ["customer_id", "first_name", "last_name", "email", "signup_date"],
    )
    result = clean_customers(df)
    assert result.count() == 1
    assert result.collect()[0]["email"] == "jane@example.com"


def test_build_fact_orders_excludes_non_completed_orders(spark):
    orders = spark.createDataFrame(
        [
            ("o1", "c1", "2026-01-01 10:00:00", "completed"),
            ("o2", "c1", "2026-01-01 10:00:00", "cancelled"),
        ],
        ["order_id", "customer_id", "order_date", "order_status"],
    )
    items = spark.createDataFrame(
        [
            ("i1", "o1", "p1", 2, 10.0),
            ("i2", "o2", "p1", 1, 10.0),
        ],
        ["order_item_id", "order_id", "product_id", "quantity", "unit_price"],
    )
    fact = build_fact_orders(orders, items)
    assert fact.count() == 1
    assert fact.collect()[0]["line_total"] == 20.0


def test_daily_revenue_mart_aggregates_correctly(spark):
    fact = spark.createDataFrame(
        [
            ("i1", "o1", "c1", "p1", "2026-01-01 09:00:00", 2, 10.0, 20.0),
            ("i2", "o2", "c1", "p1", "2026-01-01 15:00:00", 1, 10.0, 10.0),
        ],
        ["order_item_id", "order_id", "customer_id", "product_id", "order_date", "quantity", "unit_price", "line_total"],
    )
    mart = build_daily_revenue_mart(fact)
    row = mart.collect()[0]
    assert row["num_orders"] == 2
    assert row["total_revenue"] == 30.0

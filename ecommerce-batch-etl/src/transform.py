"""
Transform layer: cleans raw data and builds a dimensional model
(fact_orders + dim_customer + dim_product) plus a daily revenue mart.

All functions take/return Spark DataFrames so they are independently
unit-testable (see tests/test_transform.py).
"""
from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def clean_customers(df: DataFrame) -> DataFrame:
    return (
        df.dropDuplicates(["customer_id"])
        .withColumn("email", F.lower(F.trim(F.col("email"))))
        .withColumn("signup_date", F.to_date("signup_date"))
        .filter(F.col("customer_id").isNotNull())
    )


def clean_products(df: DataFrame) -> DataFrame:
    return (
        df.dropDuplicates(["product_id"])
        .withColumn("unit_price", F.col("unit_price").cast("decimal(10,2)"))
        .filter(F.col("unit_price") > 0)
    )


def build_fact_orders(orders: DataFrame, order_items: DataFrame) -> DataFrame:
    """
    One row per order_item, enriched with order-level attributes and
    a computed line_total. This is the atomic-grain fact table.
    """
    orders_clean = orders.withColumn("order_date", F.to_timestamp("order_date")).filter(
        F.col("order_status") == "completed"
    )

    items_clean = order_items.withColumn(
        "line_total", F.col("quantity") * F.col("unit_price")
    )

    fact = orders_clean.join(items_clean, "order_id", "inner").select(
        "order_item_id",
        "order_id",
        "customer_id",
        "product_id",
        "order_date",
        "quantity",
        "unit_price",
        "line_total",
    )
    return fact


def build_daily_revenue_mart(fact_orders: DataFrame) -> DataFrame:
    """Aggregated mart used by the BI layer / dashboards."""
    return (
        fact_orders.withColumn("order_day", F.to_date("order_date"))
        .groupBy("order_day")
        .agg(
            F.countDistinct("order_id").alias("num_orders"),
            F.sum("line_total").alias("total_revenue"),
            F.round(F.avg("line_total"), 2).alias("avg_line_value"),
        )
        .orderBy("order_day")
    )


def build_top_products_mart(fact_orders: DataFrame, products: DataFrame, top_n: int = 10) -> DataFrame:
    return (
        fact_orders.groupBy("product_id")
        .agg(
            F.sum("quantity").alias("units_sold"),
            F.sum("line_total").alias("revenue"),
        )
        .join(products, "product_id")
        .orderBy(F.desc("revenue"))
        .limit(top_n)
    )

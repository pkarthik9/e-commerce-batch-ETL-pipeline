"""
Extract layer: reads raw CSV files into Spark DataFrames.
In production this would point at an S3/ADLS/GCS path or a JDBC source;
it is parameterized so swapping the source is a one-line change.
"""
from pyspark.sql import DataFrame, SparkSession


def get_spark_session(app_name: str = "ecommerce-etl") -> SparkSession:
    return (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate()
    )


def read_csv(spark: SparkSession, path: str, header: bool = True) -> DataFrame:
    return spark.read.option("header", header).option("inferSchema", True).csv(path)


def extract_all(spark: SparkSession, raw_dir: str) -> dict:
    return {
        "customers": read_csv(spark, f"{raw_dir}/customers.csv"),
        "products": read_csv(spark, f"{raw_dir}/products.csv"),
        "orders": read_csv(spark, f"{raw_dir}/orders.csv"),
        "order_items": read_csv(spark, f"{raw_dir}/order_items.csv"),
    }

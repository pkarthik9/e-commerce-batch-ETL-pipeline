"""
Load layer: writes curated DataFrames to the warehouse.

For local development this writes Parquet to disk and loads into SQLite
(a free stand-in for Snowflake) via pandas/sqlalchemy. Swapping in real
Snowflake only requires changing `write_to_warehouse` to use the
snowflake-connector / spark-snowflake connector instead - the rest of the
pipeline (extract/transform) is unchanged. This separation is intentional
and mirrors how you would actually migrate this project to a cloud target.
"""
from pyspark.sql import DataFrame
from sqlalchemy import create_engine


def write_parquet(df: DataFrame, path: str, mode: str = "overwrite") -> None:
    df.write.mode(mode).parquet(path)


def write_to_warehouse(df: DataFrame, table_name: str, db_path: str = "data/warehouse.db") -> None:
    """Collects to driver and loads into SQLite. Fine for portfolio-scale data;
    for real workloads this would be a Spark JDBC write to Snowflake/Redshift."""
    pdf = df.toPandas()
    engine = create_engine(f"sqlite:///{db_path}")
    pdf.to_sql(table_name, engine, if_exists="replace", index=False)
    print(f"Loaded {len(pdf)} rows into {table_name} ({db_path})")

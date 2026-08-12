"""Reference solutions for 11_read_write.py — peek after a real try."""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col


def ex1_parquet_roundtrip(
    spark: SparkSession, orders: DataFrame, path: str
) -> DataFrame:
    orders.write.mode("overwrite").parquet(path)
    return spark.read.parquet(path)


def ex2_csv_header_roundtrip(
    spark: SparkSession, orders: DataFrame, path: str
) -> DataFrame:
    orders.write.mode("overwrite").option("header", True).csv(path)
    return (
        spark.read.option("header", True).option("inferSchema", True).csv(path)
    )


def ex3_write_open_parquet(
    spark: SparkSession, orders: DataFrame, path: str
) -> DataFrame:
    (
        orders.filter(col("status") == "open")
        .write.mode("overwrite")
        .parquet(path)
    )
    return spark.read.parquet(path)


def ex4_partition_by_region(
    spark: SparkSession, orders: DataFrame, path: str
) -> DataFrame:
    orders.write.mode("overwrite").partitionBy("region").parquet(path)
    return (
        spark.read.parquet(path)
        .filter(col("region") == "east")
        .select("order_id", "region", "amount", "status")
    )


def ex5_append_doubles(
    spark: SparkSession, orders: DataFrame, path: str
) -> DataFrame:
    orders.write.mode("overwrite").parquet(path)
    orders.write.mode("append").parquet(path)
    return spark.read.parquet(path)


def ex6_overwrite_replaces(
    spark: SparkSession, orders: DataFrame, path: str
) -> DataFrame:
    (
        orders.filter(col("status") == "open")
        .write.mode("overwrite")
        .parquet(path)
    )
    (
        orders.filter(col("status") == "closed")
        .write.mode("overwrite")
        .parquet(path)
    )
    return spark.read.parquet(path)


def ex7_filter_write_select(
    spark: SparkSession, orders: DataFrame, path: str
) -> DataFrame:
    (
        orders.filter(col("amount") >= 100)
        .write.mode("overwrite")
        .parquet(path)
    )
    return spark.read.parquet(path).select("order_id", "amount")


def ex8_csv_then_filter_open(
    spark: SparkSession, orders: DataFrame, path: str
) -> DataFrame:
    orders.write.mode("overwrite").option("header", True).csv(path)
    return (
        spark.read.option("header", True)
        .option("inferSchema", True)
        .csv(path)
        .filter(col("status") == "open")
    )

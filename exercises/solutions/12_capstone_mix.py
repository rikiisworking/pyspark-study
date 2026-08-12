"""Reference solutions for 12_capstone_mix.py — peek after a real try."""

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql.functions import col, coalesce, lit, row_number, sum


def ex1_fill_join_sum_by_name(
    orders: DataFrame, customers: DataFrame
) -> DataFrame:
    return (
        orders.na.fill({"amount": 0})
        .join(customers, "cust_id", "inner")
        .groupBy("name")
        .agg(sum("amount").alias("total"))
    )


def ex2_drop_join_select(
    orders: DataFrame, customers: DataFrame
) -> DataFrame:
    return (
        orders.na.drop(subset=["amount", "status"])
        .join(customers, "cust_id", "inner")
        .select("order_id", "name", "amount", "status")
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


def ex4_partition_by_region_east(
    spark: SparkSession, orders: DataFrame, customers: DataFrame, path: str
) -> DataFrame:
    (
        orders.na.fill({"amount": 0})
        .join(customers, "cust_id", "inner")
        .write.mode("overwrite")
        .partitionBy("region")
        .parquet(path)
    )
    return (
        spark.read.parquet(path)
        .filter(col("region") == "east")
        .select("order_id", "name", "region", "amount")
    )


def ex5_overwrite_open_then_closed(
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


def ex6_csv_full_then_filter_open(
    spark: SparkSession, orders: DataFrame, path: str
) -> DataFrame:
    orders.write.mode("overwrite").option("header", True).csv(path)
    return (
        spark.read.option("header", True)
        .option("inferSchema", True)
        .csv(path)
        .filter(col("status") == "open")
    )


def ex7_top_open_per_name(
    orders: DataFrame, customers: DataFrame
) -> DataFrame:
    w = Window.partitionBy("name").orderBy(
        col("amount").desc(), col("order_id")
    )
    return (
        orders.filter(
            (col("status") == "open") & col("amount").isNotNull()
        )
        .join(customers, "cust_id", "inner")
        .withColumn("rn", row_number().over(w))
        .filter(col("rn") == 1)
        .select("order_id", "name", "amount")
    )


def ex8_open_fill_sum_by_region(
    orders: DataFrame, customers: DataFrame
) -> DataFrame:
    return (
        orders.na.fill({"amount": 0})
        .filter(col("status") == "open")
        .join(customers, "cust_id", "inner")
        .withColumn("region", coalesce(col("region"), lit("unknown")))
        .groupBy("region")
        .agg(sum("amount").alias("total"))
    )

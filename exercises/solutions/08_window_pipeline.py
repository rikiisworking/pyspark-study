"""Reference solutions for 08_window_pipeline.py — peek after a real try."""

from pyspark.sql import DataFrame, Window
from pyspark.sql.functions import col, dense_rank, lag, rank, row_number, sum


def ex1_open_rn_by_region(
    orders: DataFrame, customers: DataFrame
) -> DataFrame:
    w = Window.partitionBy("region").orderBy(col("amount").desc(), col("order_id"))
    return (
        orders.filter(col("status") == "open")
        .join(customers, "cust_id", "inner")
        .withColumn("rn", row_number().over(w))
        .select("order_id", "name", "region", "amount", "rn")
    )


def ex2_top_open_per_region(
    orders: DataFrame, customers: DataFrame
) -> DataFrame:
    w = Window.partitionBy("region").orderBy(col("amount").desc(), col("order_id"))
    return (
        orders.filter(col("status") == "open")
        .join(customers, "cust_id", "inner")
        .withColumn("rn", row_number().over(w))
        .filter(col("rn") == 1)
        .select("order_id", "name", "region", "amount")
    )


def ex3_lag_amount_by_name(
    orders: DataFrame, customers: DataFrame
) -> DataFrame:
    w = Window.partitionBy("name").orderBy("day")
    return (
        orders.join(customers, "cust_id", "inner")
        .withColumn("prev", lag("amount").over(w))
        .select("order_id", "name", "day", "amount", "prev")
    )


def ex4_open_running_by_region(
    orders: DataFrame, customers: DataFrame
) -> DataFrame:
    w = Window.partitionBy("region").orderBy("day")
    return (
        orders.filter(col("status") == "open")
        .join(customers, "cust_id", "inner")
        .withColumn("running", sum("amount").over(w))
        .select("order_id", "name", "region", "day", "amount", "running")
    )


def ex5_top_amount_per_tier(
    orders: DataFrame, customers: DataFrame
) -> DataFrame:
    w = Window.partitionBy("tier").orderBy(col("amount").desc())
    return (
        orders.join(customers, "cust_id", "inner")
        .withColumn("r", rank().over(w))
        .filter(col("r") == 1)
        .select("order_id", "name", "tier", "amount")
    )


def ex6_high_dense_rank(
    orders: DataFrame, customers: DataFrame
) -> DataFrame:
    w = Window.partitionBy("region").orderBy(col("amount").desc())
    return (
        orders.filter(col("amount") >= 100)
        .join(customers, "cust_id", "inner")
        .withColumn("d", dense_rank().over(w))
        .select("order_id", "name", "region", "amount", "d")
    )


def ex7_open_delta_by_name(
    orders: DataFrame, customers: DataFrame
) -> DataFrame:
    w = Window.partitionBy("name").orderBy("day")
    return (
        orders.filter(col("status") == "open")
        .join(customers, "cust_id", "inner")
        .withColumn("prev", lag("amount").over(w))
        .withColumn("delta", col("amount") - col("prev"))
        .select("order_id", "name", "day", "amount", "prev", "delta")
    )


def ex8_top2_open_per_region(
    orders: DataFrame, customers: DataFrame
) -> DataFrame:
    w = Window.partitionBy("region").orderBy(col("amount").desc(), col("order_id"))
    return (
        orders.filter(col("status") == "open")
        .join(customers, "cust_id", "inner")
        .withColumn("rn", row_number().over(w))
        .filter(col("rn") <= 2)
        .select("order_id", "name", "region", "amount", "rn")
    )

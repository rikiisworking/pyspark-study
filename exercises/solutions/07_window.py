"""Reference solutions for 07_window.py — peek after a real try."""

from pyspark.sql import DataFrame, Window
from pyspark.sql.functions import col, dense_rank, lag, lead, rank, row_number, sum


def ex1_row_number_by_amount(orders: DataFrame) -> DataFrame:
    w = Window.partitionBy("region").orderBy(col("amount").desc(), col("id"))
    return orders.withColumn("rn", row_number().over(w)).select(
        "id", "region", "amount", "rn"
    )


def ex2_rank_by_amount(orders: DataFrame) -> DataFrame:
    w = Window.partitionBy("region").orderBy(col("amount").desc())
    return orders.withColumn("r", rank().over(w)).select(
        "id", "region", "amount", "r"
    )


def ex3_dense_rank_by_amount(orders: DataFrame) -> DataFrame:
    w = Window.partitionBy("region").orderBy(col("amount").desc())
    return orders.withColumn("d", dense_rank().over(w)).select(
        "id", "region", "amount", "d"
    )


def ex4_lag_prev_amount(orders: DataFrame) -> DataFrame:
    w = Window.partitionBy("region").orderBy("day")
    return orders.withColumn("prev", lag("amount").over(w)).select(
        "id", "region", "day", "amount", "prev"
    )


def ex5_lead_next_amount(orders: DataFrame) -> DataFrame:
    w = Window.partitionBy("region").orderBy("day")
    return orders.withColumn("nxt", lead("amount").over(w)).select(
        "id", "region", "day", "amount", "nxt"
    )


def ex6_region_total(orders: DataFrame) -> DataFrame:
    w = Window.partitionBy("region")
    return orders.withColumn("reg_total", sum("amount").over(w)).select(
        "id", "region", "amount", "reg_total"
    )


def ex7_running_total(orders: DataFrame) -> DataFrame:
    w = Window.partitionBy("region").orderBy("day")
    return orders.withColumn("running", sum("amount").over(w)).select(
        "id", "region", "day", "amount", "running"
    )


def ex8_top_amount_per_region(orders: DataFrame) -> DataFrame:
    w = Window.partitionBy("region").orderBy(col("amount").desc(), col("id"))
    return (
        orders.withColumn("rn", row_number().over(w))
        .filter(col("rn") == 1)
        .select("id", "region", "amount")
    )

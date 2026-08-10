"""Reference solutions for 09_nulls.py — peek only after a real try."""

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, coalesce, lit, when


def ex1_null_amounts(orders: DataFrame) -> DataFrame:
    return orders.filter(col("amount").isNull())


def ex2_not_null_amounts(orders: DataFrame) -> DataFrame:
    return orders.filter(col("amount").isNotNull())


def ex3_status_open(orders: DataFrame) -> DataFrame:
    return orders.filter(col("status") == "open")


def ex4_drop_amount_or_status_null(orders: DataFrame) -> DataFrame:
    return orders.na.drop(subset=["amount", "status"])


def ex5_fill_amount_zero(orders: DataFrame) -> DataFrame:
    return orders.na.fill({"amount": 0})


def ex6_coalesce_region(orders: DataFrame) -> DataFrame:
    return orders.withColumn(
        "region_filled",
        coalesce(col("region"), lit("unknown")),
    ).select("id", "region", "region_filled")


def ex7_amount_flag(orders: DataFrame) -> DataFrame:
    return orders.select(
        "id",
        "amount",
        when(col("amount").isNull(), "missing")
        .otherwise("present")
        .alias("flag"),
    )


def ex8_open_or_null_status(orders: DataFrame) -> DataFrame:
    return orders.filter(
        col("status").isNull() | (col("status") == "open")
    )

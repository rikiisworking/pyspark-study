"""Reference solutions for 10_nulls_pipeline.py — peek after a real try."""

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, coalesce, lit, sum, when


def ex1_null_amounts(orders: DataFrame) -> DataFrame:
    return orders.filter(col("amount").isNull()).select(
        "order_id", "cust_id", "amount", "status"
    )


def ex2_fill_then_open(orders: DataFrame) -> DataFrame:
    return (
        orders.na.fill({"amount": 0})
        .filter(col("status") == "open")
        .select("order_id", "cust_id", "amount", "status")
    )


def ex3_drop_then_inner(
    orders: DataFrame, customers: DataFrame
) -> DataFrame:
    return (
        orders.na.drop(subset=["amount", "status"])
        .join(customers, "cust_id", "inner")
        .select("order_id", "name", "amount", "status")
    )


def ex4_left_coalesce_region(
    orders: DataFrame, customers: DataFrame
) -> DataFrame:
    return (
        orders.join(customers, "cust_id", "left")
        .withColumn("region", coalesce(col("region"), lit("unknown")))
        .select("order_id", "name", "region", "amount")
    )


def ex5_open_or_null_status_amount_ok(orders: DataFrame) -> DataFrame:
    return orders.filter(
        (col("status").isNull() | (col("status") == "open"))
        & col("amount").isNotNull()
    ).select("order_id", "status", "amount")


def ex6_fill_join_sum_by_name(
    orders: DataFrame, customers: DataFrame
) -> DataFrame:
    return (
        orders.na.fill({"amount": 0})
        .join(customers, "cust_id", "inner")
        .groupBy("name")
        .agg(sum("amount").alias("total"))
    )


def ex7_flag_missing_amount(
    orders: DataFrame, customers: DataFrame
) -> DataFrame:
    return (
        orders.join(customers, "cust_id", "inner")
        .select(
            "order_id",
            "name",
            when(col("amount").isNull(), "missing")
            .otherwise("ok")
            .alias("flag"),
        )
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

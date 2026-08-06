"""Reference solutions for 05_groupby_agg.py — peek after a real try."""

from pyspark.sql import DataFrame
from pyspark.sql.functions import avg, col, count, max, sum


def ex1_count_by_region(orders: DataFrame) -> DataFrame:
    return orders.groupBy("region").count()


def ex2_sum_amount_default_name(orders: DataFrame) -> DataFrame:
    return orders.groupBy("region").sum("amount")


def ex3_avg_amount_aliased(orders: DataFrame) -> DataFrame:
    return orders.groupBy("region").agg(avg("amount").alias("avg_amount"))


def ex4_multi_agg(orders: DataFrame) -> DataFrame:
    return orders.groupBy("region").agg(
        count("*").alias("n"),
        sum("amount").alias("total"),
        max("amount").alias("hi"),
    )


def ex5_region_status_counts(orders: DataFrame) -> DataFrame:
    return orders.groupBy("region", "status").count()


def ex6_open_total_by_region(orders: DataFrame) -> DataFrame:
    return (
        orders.filter(col("status") == "open")
        .groupBy("region")
        .agg(sum("amount").alias("open_total"))
    )


def ex7_big_region_totals(orders: DataFrame) -> DataFrame:
    return (
        orders.groupBy("region")
        .agg(sum("amount").alias("total"))
        .filter(col("total") > 280)
    )


def ex8_spend_by_customer(
    sales: DataFrame, customers: DataFrame
) -> DataFrame:
    return (
        sales.join(customers, "cust_id", "inner")
        .groupBy("name")
        .agg(sum("amount").alias("spend"), count("*").alias("orders"))
    )

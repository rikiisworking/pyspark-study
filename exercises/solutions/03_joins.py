"""Reference solutions for 03_joins.py — peek only after a real try."""

from pyspark.sql import DataFrame
from pyspark.sql.functions import col


def ex1_inner_on_key(orders: DataFrame, customers: DataFrame) -> DataFrame:
    return orders.join(customers, "cust_id", "inner").select(
        "order_id", "name", "amount"
    )


def ex2_left_orders(orders: DataFrame, customers: DataFrame) -> DataFrame:
    return orders.join(customers, "cust_id", "left").select(
        "order_id", "name", "amount"
    )


def ex3_right_customers(orders: DataFrame, customers: DataFrame) -> DataFrame:
    return orders.join(customers, "cust_id", "right").select("name", "order_id")


def ex4_full_outer(orders: DataFrame, customers: DataFrame) -> DataFrame:
    return orders.join(customers, "cust_id", "full").select("order_id", "name")


def ex5_left_semi(orders: DataFrame, customers: DataFrame) -> DataFrame:
    # join key often lands first — select to pin column order
    return orders.join(customers, "cust_id", "left_semi").select(
        "order_id", "cust_id", "amount"
    )


def ex6_left_anti(orders: DataFrame, customers: DataFrame) -> DataFrame:
    return orders.join(customers, "cust_id", "left_anti").select(
        "order_id", "cust_id", "amount"
    )


def ex7_inner_east_only(orders: DataFrame, customers: DataFrame) -> DataFrame:
    return (
        orders.join(customers, "cust_id", "inner")
        .filter(col("region") == "east")
        .select("order_id", "name", "region", "amount")
    )


def ex8_column_join_select(orders: DataFrame, customers: DataFrame) -> DataFrame:
    o = orders.alias("o")
    c = customers.alias("c")
    return o.join(c, col("o.cust_id") == col("c.cust_id"), "inner").select(
        col("o.order_id").alias("order_id"),
        col("c.name").alias("name"),
        col("o.amount").alias("amount"),
    )

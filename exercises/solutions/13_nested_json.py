"""Reference solutions for 13_nested_json.py — peek after a real try."""

from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col,
    explode,
    explode_outer,
    from_json,
    get_json_object,
    sum,
)

JSON_SCHEMA = "user STRUCT<id:INT, name:STRING>, amount INT, tags ARRAY<STRING>"


def ex1_struct_field_select(orders: DataFrame) -> DataFrame:
    return orders.select(
        "order_id", col("user.name").alias("name"), "amount"
    )


def ex2_filter_nested_user(orders: DataFrame) -> DataFrame:
    return (
        orders.filter(col("user.id") == 10)
        .select("order_id", col("user.name").alias("name"), "amount")
    )


def ex3_from_json_select(events: DataFrame) -> DataFrame:
    return (
        events.withColumn("p", from_json(col("raw"), JSON_SCHEMA))
        .select(
            "event_id",
            col("p.user.id").alias("user_id"),
            col("p.amount").alias("amount"),
        )
    )


def ex4_get_json_object_user_id(events: DataFrame) -> DataFrame:
    return events.select(
        "event_id",
        get_json_object(col("raw"), "$.user.id").cast("int").alias("user_id"),
    )


def ex5_explode_tags(events: DataFrame) -> DataFrame:
    return (
        events.withColumn("p", from_json(col("raw"), JSON_SCHEMA))
        .select("event_id", explode(col("p.tags")).alias("tag"))
    )


def ex6_explode_outer_tags(events: DataFrame) -> DataFrame:
    return (
        events.withColumn("p", from_json(col("raw"), JSON_SCHEMA))
        .select("event_id", explode_outer(col("p.tags")).alias("tag"))
    )


def ex7_json_filter_amount(events: DataFrame) -> DataFrame:
    return (
        events.withColumn("p", from_json(col("raw"), JSON_SCHEMA))
        .filter(col("p.amount") >= 100)
        .select(
            "event_id",
            col("p.user.id").alias("user_id"),
            col("p.amount").alias("amount"),
        )
    )


def ex8_json_sum_by_user(events: DataFrame) -> DataFrame:
    return (
        events.withColumn("p", from_json(col("raw"), JSON_SCHEMA))
        .groupBy(col("p.user.id").alias("user_id"))
        .agg(sum("p.amount").alias("total"))
    )

"""Reference solutions for 14_nested_pipeline.py — peek after a real try."""

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql.functions import (
    coalesce,
    col,
    explode,
    explode_outer,
    from_json,
    lit,
    row_number,
    sum,
)

JSON_SCHEMA = "user_id INT, amount INT, status STRING, tags ARRAY<STRING>"


def ex1_parse_project(events: DataFrame) -> DataFrame:
    return (
        events.withColumn("p", from_json(col("raw"), JSON_SCHEMA))
        .select(
            "event_id",
            col("p.user_id").alias("user_id"),
            col("p.amount").alias("amount"),
            col("p.status").alias("status"),
        )
    )


def ex2_open_inner_name(events: DataFrame, customers: DataFrame) -> DataFrame:
    return (
        events.withColumn("p", from_json(col("raw"), JSON_SCHEMA))
        .filter(col("p.status") == "open")
        .join(customers, col("p.user_id") == col("cust_id"), "inner")
        .select("event_id", "name", col("p.amount").alias("amount"))
    )


def ex3_explode_tag_x(events: DataFrame) -> DataFrame:
    return (
        events.withColumn("p", from_json(col("raw"), JSON_SCHEMA))
        .select(
            "event_id",
            col("p.user_id").alias("user_id"),
            explode(col("p.tags")).alias("tag"),
        )
        .filter(col("tag") == "x")
    )


def ex4_outer_tag_left_name(
    events: DataFrame, customers: DataFrame
) -> DataFrame:
    return (
        events.withColumn("p", from_json(col("raw"), JSON_SCHEMA))
        .select(
            "event_id",
            col("p.user_id").alias("user_id"),
            explode_outer(col("p.tags")).alias("tag"),
        )
        .join(customers, col("user_id") == col("cust_id"), "left")
        .select("event_id", "name", "tag")
    )


def ex5_fill_open_sum_by_name(
    events: DataFrame, customers: DataFrame
) -> DataFrame:
    return (
        events.withColumn("p", from_json(col("raw"), JSON_SCHEMA))
        .select(
            "event_id",
            col("p.user_id").alias("user_id"),
            col("p.amount").alias("amount"),
            col("p.status").alias("status"),
        )
        .na.fill({"amount": 0})
        .filter(col("status") == "open")
        .join(customers, col("user_id") == col("cust_id"), "inner")
        .groupBy("name")
        .agg(sum("amount").alias("total"))
    )


def ex6_top_open_per_user(
    events: DataFrame, customers: DataFrame
) -> DataFrame:
    w = Window.partitionBy("user_id").orderBy(
        col("amount").desc(), col("event_id").asc()
    )
    return (
        events.withColumn("p", from_json(col("raw"), JSON_SCHEMA))
        .select(
            "event_id",
            col("p.user_id").alias("user_id"),
            col("p.amount").alias("amount"),
            col("p.status").alias("status"),
        )
        .filter((col("status") == "open") & col("amount").isNotNull())
        .withColumn("rn", row_number().over(w))
        .filter(col("rn") == 1)
        .join(customers, col("user_id") == col("cust_id"), "inner")
        .select("event_id", "name", "amount")
    )


def ex7_sum_by_region(
    events: DataFrame, customers: DataFrame
) -> DataFrame:
    return (
        events.withColumn("p", from_json(col("raw"), JSON_SCHEMA))
        .select(
            col("p.user_id").alias("user_id"),
            col("p.amount").alias("amount"),
        )
        .na.fill({"amount": 0})
        .join(customers, col("user_id") == col("cust_id"), "inner")
        .withColumn("region", coalesce(col("region"), lit("unknown")))
        .groupBy("region")
        .agg(sum("amount").alias("total"))
    )


def ex8_open_parquet_roundtrip(
    spark: SparkSession, events: DataFrame, path: str
) -> DataFrame:
    parsed = (
        events.withColumn("p", from_json(col("raw"), JSON_SCHEMA))
        .filter(col("p.status") == "open")
        .select(
            "event_id",
            col("p.user_id").alias("user_id"),
            col("p.amount").alias("amount"),
        )
    )
    parsed.write.mode("overwrite").parquet(path)
    return spark.read.parquet(path)

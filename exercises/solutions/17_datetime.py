"""Reference solutions for 17_datetime.py — peek only after a real try."""

from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    coalesce,
    col,
    date_format,
    date_trunc,
    datediff,
    month,
    sum,
    to_timestamp,
    try_to_date,
    year,
)


def ex1_try_iso_sold(orders: DataFrame) -> DataFrame:
    return orders.select(
        "order_id", try_to_date("sold_at", "yyyy-MM-dd").alias("sold")
    )


def ex2_coalesce_iso_eu(orders: DataFrame) -> DataFrame:
    return orders.select(
        "order_id",
        coalesce(
            try_to_date("sold_at", "yyyy-MM-dd"),
            try_to_date("sold_at", "dd/MM/yyyy"),
        ).alias("sold"),
    )


def ex3_parse_timestamp(orders: DataFrame) -> DataFrame:
    return orders.select(
        "order_id",
        to_timestamp("ts", "yyyy-MM-dd HH:mm:ss").alias("sold_ts"),
    )


def ex4_month_start(orders: DataFrame) -> DataFrame:
    d = try_to_date("sold_at", "yyyy-MM-dd")
    return orders.select(
        "order_id",
        date_format(date_trunc("month", d), "yyyy-MM-dd").alias("month_start"),
    )


def ex5_ship_lag_days(orders: DataFrame) -> DataFrame:
    return orders.select(
        "order_id",
        datediff(
            try_to_date("shipped_at", "yyyy-MM-dd"),
            try_to_date("sold_at", "yyyy-MM-dd"),
        ).alias("lag_days"),
    )


def ex6_year_month(orders: DataFrame) -> DataFrame:
    d = try_to_date("sold_at", "yyyy-MM-dd")
    return orders.select("order_id", year(d).alias("y"), month(d).alias("mo"))


def ex7_report_string(orders: DataFrame) -> DataFrame:
    return orders.select(
        "order_id",
        date_format(try_to_date("sold_at", "yyyy-MM-dd"), "dd-MM-yyyy").alias(
            "sold_str"
        ),
    )


def ex8_sum_by_month(orders: DataFrame) -> DataFrame:
    d = try_to_date("sold_at", "yyyy-MM-dd")
    return (
        orders.withColumn("sold", d)
        .filter(col("sold").isNotNull())
        .withColumn(
            "month_start",
            date_format(date_trunc("month", col("sold")), "yyyy-MM-dd"),
        )
        .groupBy("month_start")
        .agg(sum("amount").alias("total"))
        .select("month_start", "total")
    )

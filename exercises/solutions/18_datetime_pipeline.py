"""Reference solutions for 18_datetime_pipeline.py — peek only after a real try."""

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql.functions import (
    coalesce,
    col,
    date_format,
    date_trunc,
    datediff,
    lit,
    row_number,
    sum,
    try_to_date,
    try_to_timestamp,
)


def ex1_iso_open_after_jan10(orders: DataFrame) -> DataFrame:
    return (
        orders.withColumn("sold", try_to_date(col("sold_at"), "yyyy-MM-dd"))
        .filter((col("status") == "open") & (col("sold") > "2026-01-10"))
        .select("order_id", "sold")
    )


def ex2_iso_inner_name(orders: DataFrame, customers: DataFrame) -> DataFrame:
    return (
        orders.withColumn("sold", try_to_date(col("sold_at"), "yyyy-MM-dd"))
        .dropna(subset="sold")
        .join(customers, "cust_id")
        .select("order_id", "name", "sold")
    )


def ex3_coalesce_then_east(
    orders: DataFrame, customers: DataFrame
) -> DataFrame:
    return (
        orders.withColumn(
            "sold",
            coalesce(
                try_to_date(col("sold_at"), "yyyy-MM-dd"),
                try_to_date(col("sold_at"), "dd/MM/yyyy"),
            ),
        )
        .join(customers, "cust_id")
        .filter(col("region") == "east")
        .select("order_id", "name", "sold")
    )


def ex4_lag_gt_two(orders: DataFrame) -> DataFrame:
    return (
        orders.withColumn("shipped", try_to_date(col("shipped_at"), "yyyy-MM-dd"))
        .withColumn("sold", try_to_date(col("sold_at"), "yyyy-MM-dd"))
        .withColumn("lag_days", datediff(col("shipped"), col("sold")))
        .filter(col("lag_days") > 2)
        .select("order_id", "lag_days")
    )


def ex5_ts_not_sold_at(orders: DataFrame) -> DataFrame:
    return (
        orders.withColumn(
            "sold_ts",
            try_to_timestamp(col("ts"), lit("yyyy-MM-dd HH:mm:ss")),
        )
        .dropna(subset="sold_ts")
        .select("order_id", "sold_ts")
    )


def ex6_window_top_per_month(orders: DataFrame) -> DataFrame:
    w = Window.partitionBy("month_start").orderBy(
        col("amount").desc(), col("order_id")
    )
    return (
        orders.withColumn("sold", try_to_date(col("sold_at"), "yyyy-MM-dd"))
        .dropna(subset=["sold", "amount"])
        .withColumn(
            "month_start",
            date_format(date_trunc("month", col("sold")), "yyyy-MM-dd"),
        )
        .withColumn("rn", row_number().over(w))
        .filter(col("rn") == 1)
        .select("order_id", "month_start", "amount")
    )


def ex7_fill_sum_by_month(
    orders: DataFrame, customers: DataFrame
) -> DataFrame:
    return (
        orders.withColumn("sold", try_to_date(col("sold_at"), "yyyy-MM-dd"))
        .join(customers, "cust_id")
        .na.fill({"amount": 0})
        .dropna(subset="sold")
        .withColumn(
            "month_start",
            date_format(date_trunc("month", col("sold")), "yyyy-MM-dd"),
        )
        .groupBy("month_start")
        .agg(sum("amount").alias("total"))
        .select("month_start", "total")
    )


def ex8_iso_parquet(
    spark: SparkSession, orders: DataFrame, path: str
) -> DataFrame:
    (
        orders.withColumn("sold", try_to_date(col("sold_at"), "yyyy-MM-dd"))
        .select("order_id", "sold")
        .write.mode("overwrite")
        .parquet(path)
    )
    return spark.read.parquet(path)

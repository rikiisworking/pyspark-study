"""
Lesson 0020 drills — datetime + pipeline mix (0001–0019).

No new API. Interleave ISO parse / try_to_timestamp / date_trunc /
datediff with filter, join, fill, groupBy, window, parquet.
Run:
  ../.venv/bin/python 18_datetime_pipeline.py

Data (built in checker):

  orders: order_id INT, cust_id INT, sold_at STRING, shipped_at STRING,
          ts STRING, amount INT, status STRING
    (1, 10, "2026-01-15", "2026-01-20", "2026-01-15 14:30:00", 100,  "open")
    (2, 20, "2026-01-31", "2026-02-02", "2026-01-31 09:00:00",  50,  "open")
    (3, 10, "2026-02-01", "2026-02-01", "2026-02-01 00:00:00", 200,  "closed")
    (4, 30, "15/01/2026", "2026-01-16", None,                   80,  "open")   # EU
    (5, 99, None,         "2026-01-10", None,                   40,  "open")   # orphan
    (6, 20, "2026-01-10", "2026-01-12", "2026-01-10 08:00:00", None, "open")   # null amount
    (7, 30, "2026-02-15", "2026-02-18", "2026-02-15 12:00:00", 150,  "open")

  customers: cust_id INT, name STRING, region STRING
    (10, "Alice", "east")
    (20, "Bob",   None)
    (30, "Carol", "east")
    (40, "Dan",   "north")     # no orders

Lesson: lessons/0020-datetime-pipeline-mix.html
Solutions (after real try): exercises/solutions/18_datetime_pipeline.py
"""

from __future__ import annotations

import shutil
import tempfile

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql.functions import (  # noqa: F401 — use what you need
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

# ---------------------------------------------------------------------------
# YOUR CODE — replace `raise NotImplementedError`
# ---------------------------------------------------------------------------


def ex1_iso_open_after_jan10(orders: DataFrame) -> DataFrame:
    """
    ISO-parse sold_at as sold. Keep status == "open" and sold > 2026-01-10.
    Columns: order_id, sold
    """
    return (
        orders
            .withColumn("sold", try_to_date(col("sold_at"), "yyyy-MM-dd"))
            .filter(
                (col("status") == "open") & 
                (col("sold") > try_to_date(lit("2026-01-10"), "yyyy-MM-dd")))
            .select("order_id", "sold")
    )


def ex2_iso_inner_name(orders: DataFrame, customers: DataFrame) -> DataFrame:
    """
    ISO-parse sold_at as sold. Drop null sold. Inner join customers on cust_id.
    Columns: order_id, name, sold
    """
    return (
        orders
            .withColumn("sold", try_to_date(col("sold_at"), "yyyy-MM-dd"))
            .dropna(subset="sold")
            .join(customers, "cust_id")
            .select("order_id", "name", "sold")
    )


def ex3_coalesce_then_east(
    orders: DataFrame, customers: DataFrame
) -> DataFrame:
    """
    sold = coalesce(ISO try_to_date, EU dd/MM/yyyy try_to_date).
    Inner join customers. Keep region == "east".
    Columns: order_id, name, sold
    """
    return (
        orders
            .withColumn(
                "sold",
                coalesce(
                     try_to_date(col("sold_at"), "yyyy-MM-dd"),
                     try_to_date(col("sold_at"), "dd/MM/yyyy")
                )
            )
            .join(customers, "cust_id")
            .filter(col("region")=="east")
            .select("order_id", "name", "sold")
    )

def ex4_lag_gt_two(orders: DataFrame) -> DataFrame:
    """
    ISO-parse shipped_at and sold_at. lag_days = datediff(shipped, sold).
    Keep lag_days > 2.
    Columns: order_id, lag_days
    """
    return (
        orders
            .withColumn(
                "shipped_at", try_to_date(col("shipped_at"), "yyyy-MM-dd")
            )
            .withColumn(
                "sold_at", try_to_date(col("sold_at"), "yyyy-MM-dd")
            )
            .withColumn(
                "lag_days", datediff(col("shipped_at"), col("sold_at"))
            )
            .filter(col("lag_days") > 2)
            .select("order_id", "lag_days")
    )

def ex5_ts_not_sold_at(orders: DataFrame) -> DataFrame:
    """
    try_to_timestamp ts (not sold_at) with yyyy-MM-dd HH:mm:ss.
    Drop null sold_ts.
    Columns: order_id, sold_ts
    """
    return (
        orders
            .withColumn(
                "sold_ts", try_to_timestamp(col("ts"), lit("yyyy-MM-dd HH:mm:ss"))
            )
            .dropna(subset="sold_ts")
            .select("order_id", "sold_ts")
    )


def ex6_window_top_per_month(orders: DataFrame) -> DataFrame:
    """
    ISO-parse sold_at. Drop null sold and null amount.
    month_start = date_format(date_trunc month, yyyy-MM-dd).
    Window: top amount per month_start (ties → smaller order_id).
    Columns: order_id, month_start, amount
    """
    w = Window.partitionBy("month_start").orderBy("order_id")
    return (
        orders
            .withColumn(
                "sold_at", try_to_date(col("sold_at"), "yyyy-MM-dd")
            )
            .dropna(subset=("sold_at", "amount"))
            .withColumn(
                "month_start",
                date_format(date_trunc("month", col("sold_at")), "yyyy-MM-dd")
            )
            .withColumn(
                "rn",
                row_number().over(w)
            )
            .filter(col("rn")==1)
            .select("order_id", "month_start", "amount")

    )

def ex7_fill_sum_by_month(
    orders: DataFrame, customers: DataFrame
) -> DataFrame:
    """
    ISO-parse sold_at. Inner join customers. Fill amount 0. Drop null sold.
    Month bucket yyyy-MM-dd, sum amount.
    Columns: month_start, total
    """
    return (
        orders
            .withColumn("sold", try_to_date(col("sold_at"), "yyyy-MM-dd"))
            .join(customers, "cust_id")
            .fillna({"amount":0})
            .dropna(subset="sold")
            .withColumn(
                "month_start",
                date_format(date_trunc("month", col("sold")), "yyyy-MM-dd")
            )
            .groupBy("month_start")
            .agg(
                sum(col("amount")).alias("total")
            )
            .select("month_start", "total")
    )

def ex8_iso_parquet(spark: SparkSession, orders: DataFrame, path: str) -> DataFrame:
    """
    ISO-parse sold_at as sold. Select order_id, sold.
    Write overwrite parquet to path, read back.
    Columns: order_id, sold
    """

    (
        orders
            .withColumn("sold", try_to_date(col("sold_at"), "yyyy-MM-dd"))
            .write.mode("overwrite").parquet(path) 
    )

    return spark.read.parquet(path).select("order_id", "sold")


# ---------------------------------------------------------------------------
# Checker — do not edit below
# ---------------------------------------------------------------------------

from datetime import date, datetime  # noqa: E402

_ORDER_ROWS = [
    (1, 10, "2026-01-15", "2026-01-20", "2026-01-15 14:30:00", 100, "open"),
    (2, 20, "2026-01-31", "2026-02-02", "2026-01-31 09:00:00", 50, "open"),
    (3, 10, "2026-02-01", "2026-02-01", "2026-02-01 00:00:00", 200, "closed"),
    (4, 30, "15/01/2026", "2026-01-16", None, 80, "open"),
    (5, 99, None, "2026-01-10", None, 40, "open"),
    (6, 20, "2026-01-10", "2026-01-12", "2026-01-10 08:00:00", None, "open"),
    (7, 30, "2026-02-15", "2026-02-18", "2026-02-15 12:00:00", 150, "open"),
]
_ORDER_SCHEMA = (
    "order_id INT, cust_id INT, sold_at STRING, shipped_at STRING, "
    "ts STRING, amount INT, status STRING"
)

_CUSTOMER_ROWS = [
    (10, "Alice", "east"),
    (20, "Bob", None),
    (30, "Carol", "east"),
    (40, "Dan", "north"),
]
_CUSTOMER_SCHEMA = "cust_id INT, name STRING, region STRING"


def _orders(spark: SparkSession) -> DataFrame:
    return spark.createDataFrame(_ORDER_ROWS, _ORDER_SCHEMA)


def _customers(spark: SparkSession) -> DataFrame:
    return spark.createDataFrame(_CUSTOMER_ROWS, _CUSTOMER_SCHEMA)


def _run_path(spark: SparkSession, fn, expect, columns):
    d = tempfile.mkdtemp(prefix="pyspark-dt-mix-")
    try:
        got = fn(spark, _orders(spark), d)
        rows = got.collect()
        materialized = spark.createDataFrame(rows, got.schema)
        return materialized, expect, columns
    finally:
        shutil.rmtree(d, ignore_errors=True)


if __name__ == "__main__":
    from harness import run_suite

    run_suite(
        "18 datetime + pipeline mix",
        [
            (
                "ex1_iso_open_after_jan10",
                lambda s: (
                    ex1_iso_open_after_jan10(_orders(s)),
                    [
                        (1, date(2026, 1, 15)),
                        (2, date(2026, 1, 31)),
                        (7, date(2026, 2, 15)),
                    ],
                    ["order_id", "sold"],
                ),
            ),
            (
                "ex2_iso_inner_name",
                lambda s: (
                    ex2_iso_inner_name(_orders(s), _customers(s)),
                    [
                        (1, "Alice", date(2026, 1, 15)),
                        (2, "Bob", date(2026, 1, 31)),
                        (3, "Alice", date(2026, 2, 1)),
                        (6, "Bob", date(2026, 1, 10)),
                        (7, "Carol", date(2026, 2, 15)),
                    ],
                    ["order_id", "name", "sold"],
                ),
            ),
            (
                "ex3_coalesce_then_east",
                lambda s: (
                    ex3_coalesce_then_east(_orders(s), _customers(s)),
                    [
                        (1, "Alice", date(2026, 1, 15)),
                        (3, "Alice", date(2026, 2, 1)),
                        (4, "Carol", date(2026, 1, 15)),
                        (7, "Carol", date(2026, 2, 15)),
                    ],
                    ["order_id", "name", "sold"],
                ),
            ),
            (
                "ex4_lag_gt_two",
                lambda s: (
                    ex4_lag_gt_two(_orders(s)),
                    [
                        (1, 5),
                        (7, 3),
                    ],
                    ["order_id", "lag_days"],
                ),
            ),
            (
                "ex5_ts_not_sold_at",
                lambda s: (
                    ex5_ts_not_sold_at(_orders(s)),
                    [
                        (1, datetime(2026, 1, 15, 14, 30)),
                        (2, datetime(2026, 1, 31, 9, 0)),
                        (3, datetime(2026, 2, 1, 0, 0)),
                        (6, datetime(2026, 1, 10, 8, 0)),
                        (7, datetime(2026, 2, 15, 12, 0)),
                    ],
                    ["order_id", "sold_ts"],
                ),
            ),
            (
                "ex6_window_top_per_month",
                lambda s: (
                    ex6_window_top_per_month(_orders(s)),
                    [
                        (1, "2026-01-01", 100),
                        (3, "2026-02-01", 200),
                    ],
                    ["order_id", "month_start", "amount"],
                ),
            ),
            (
                "ex7_fill_sum_by_month",
                lambda s: (
                    ex7_fill_sum_by_month(_orders(s), _customers(s)),
                    [
                        ("2026-01-01", 150),
                        ("2026-02-01", 350),
                    ],
                    ["month_start", "total"],
                ),
            ),
            (
                "ex8_iso_parquet",
                lambda s: _run_path(
                    s,
                    ex8_iso_parquet,
                    [
                        (1, date(2026, 1, 15)),
                        (2, date(2026, 1, 31)),
                        (3, date(2026, 2, 1)),
                        (4, None),
                        (5, None),
                        (6, date(2026, 1, 10)),
                        (7, date(2026, 2, 15)),
                    ],
                    ["order_id", "sold"],
                ),
            ),
        ],
    )

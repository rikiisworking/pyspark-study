"""
Lesson 0018 drills — datetime (parse / trunc / diff / extract).

Spark 4: dirty strings → try_to_date (to_date raises).
Run:
  ../.venv/bin/python 17_datetime.py

Data (built in checker):

  orders: order_id INT, sold_at STRING, shipped_at STRING, ts STRING, amount INT
    (1, "2026-01-15", "2026-01-20", "2026-01-15 14:30:00", 100)
    (2, "2026-01-31", "2026-02-02", "2026-01-31 09:00:00",  50)
    (3, "2026-02-01", "2026-02-01", "2026-02-01 00:00:00", 200)
    (4, "15/01/2026", "2026-01-16", None,                   80)  # EU
    (5, None,         "2026-01-10", None,                   40)

Lesson: lessons/0018-datetime.html
Solutions (after real try): exercises/solutions/17_datetime.py
"""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql.functions import (  # noqa: F401 — use what you need
    coalesce,
    col,
    date_format,
    date_trunc,
    datediff,
    lit,
    month,
    sum,
    to_date,
    to_timestamp,
    try_to_date,
    try_to_timestamp,
    year,
)

# ---------------------------------------------------------------------------
# YOUR CODE — replace `raise NotImplementedError`
# ---------------------------------------------------------------------------


def ex1_try_iso_sold(orders: DataFrame) -> DataFrame:
    """
    try_to_date sold_at as yyyy-MM-dd.
    Columns: order_id, sold
    """
    return (
        orders
            .withColumn("sold", try_to_date(col("sold_at"), "yyyy-MM-dd"))
            .select("order_id", "sold")
    )


def ex2_coalesce_iso_eu(orders: DataFrame) -> DataFrame:
    """
    sold = coalesce(ISO try_to_date, EU dd/MM/yyyy try_to_date).
    Columns: order_id, sold
    """
    return (
        orders
            .withColumn("sold", coalesce(
                try_to_date(col("sold_at"), "dd/MM/yyyy"),
                try_to_date(col("sold_at"), "yyyy-MM-dd")
                ))
            .select("order_id", "sold")
    )


def ex3_parse_timestamp(orders: DataFrame) -> DataFrame:
    """
    to_timestamp ts with yyyy-MM-dd HH:mm:ss.
    Columns: order_id, sold_ts
    """
    return (
        orders
            .withColumn("sold_ts", try_to_timestamp(col("ts"), lit("yyyy-MM-dd HH:mm:ss")))
            .select("order_id", "sold_ts")
    )


def ex4_month_start(orders: DataFrame) -> DataFrame:
    """
    ISO-parse sold_at, date_trunc month, date_format yyyy-MM-dd.
    Columns: order_id, month_start
    """
    return (
        orders
            .withColumn("sold_at", try_to_date(col("sold_at"), "yyyy-MM-dd"))
            .withColumn(
                "month_start", 
                date_format(
                    date_trunc("month",col("sold_at")), 
                    "yyyy-MM-dd"))
            .select("order_id", "month_start")
    )


def ex5_ship_lag_days(orders: DataFrame) -> DataFrame:
    """
    ISO-parse shipped_at and sold_at. datediff(shipped, sold).
    Columns: order_id, lag_days
    """
    return (
        orders
            .withColumn("shipped_at", try_to_date(col("shipped_at"), "yyyy-MM-dd"))
            .withColumn("sold_at", try_to_date(col("sold_at"), "yyyy-MM-dd"))
            .withColumn("lag_days", datediff(col("shipped_at"), col("sold_at")))
            .select("order_id", "lag_days")
    )

def ex6_year_month(orders: DataFrame) -> DataFrame:
    """
    ISO-parse sold_at → year as y, month as mo.
    Columns: order_id, y, mo
    """
    return (
        orders
            .withColumn("sold_at", try_to_date(col("sold_at"), "yyyy-MM-dd"))
            .withColumn("y", year(col("sold_at")))
            .withColumn("mo", month(col("sold_at")))
            .select("order_id", "y", "mo")
    )


def ex7_report_string(orders: DataFrame) -> DataFrame:
    """
    ISO-parse sold_at, date_format dd-MM-yyyy.
    Columns: order_id, sold_str
    """
    return (
        orders
            .withColumn("sold_at", try_to_date(col("sold_at"), "yyyy-MM-dd"))
            .withColumn("sold_str", date_format(
                col("sold_at"), "dd-MM-yyyy"
            ))
            .select("order_id", "sold_str")
    )


def ex8_sum_by_month(orders: DataFrame) -> DataFrame:
    """
    ISO-parse sold_at, drop null sold, month bucket yyyy-MM-dd, sum amount.
    Columns: month_start, total
    """
    return (
        orders
            .withColumn("sold_at", try_to_date(col("sold_at"), "yyyy-MM-dd"))
            .dropna(subset="sold_at")
            .withColumn("month_start", date_format(
                date_trunc("month", col("sold_at")),
                "yyyy-MM-dd"
            ))
            .groupBy("month_start")
            .agg(
                sum("amount").alias("total")
            )
            .select("month_start", "total")
    )


# ---------------------------------------------------------------------------
# Checker — do not edit below
# ---------------------------------------------------------------------------

from datetime import date, datetime  # noqa: E402

_ROWS = [
    (1, "2026-01-15", "2026-01-20", "2026-01-15 14:30:00", 100),
    (2, "2026-01-31", "2026-02-02", "2026-01-31 09:00:00", 50),
    (3, "2026-02-01", "2026-02-01", "2026-02-01 00:00:00", 200),
    (4, "15/01/2026", "2026-01-16", None, 80),
    (5, None, "2026-01-10", None, 40),
]
_SCHEMA = (
    "order_id INT, sold_at STRING, shipped_at STRING, ts STRING, amount INT"
)


def _orders(spark) -> DataFrame:
    return spark.createDataFrame(_ROWS, _SCHEMA)


if __name__ == "__main__":
    from harness import run_suite

    run_suite(
        "17 datetime",
        [
            (
                "ex1_try_iso_sold",
                lambda s: (
                    ex1_try_iso_sold(_orders(s)),
                    [
                        (1, date(2026, 1, 15)),
                        (2, date(2026, 1, 31)),
                        (3, date(2026, 2, 1)),
                        (4, None),
                        (5, None),
                    ],
                    ["order_id", "sold"],
                ),
            ),
            (
                "ex2_coalesce_iso_eu",
                lambda s: (
                    ex2_coalesce_iso_eu(_orders(s)),
                    [
                        (1, date(2026, 1, 15)),
                        (2, date(2026, 1, 31)),
                        (3, date(2026, 2, 1)),
                        (4, date(2026, 1, 15)),
                        (5, None),
                    ],
                    ["order_id", "sold"],
                ),
            ),
            (
                "ex3_parse_timestamp",
                lambda s: (
                    ex3_parse_timestamp(_orders(s)),
                    [
                        (1, datetime(2026, 1, 15, 14, 30)),
                        (2, datetime(2026, 1, 31, 9, 0)),
                        (3, datetime(2026, 2, 1, 0, 0)),
                        (4, None),
                        (5, None),
                    ],
                    ["order_id", "sold_ts"],
                ),
            ),
            (
                "ex4_month_start",
                lambda s: (
                    ex4_month_start(_orders(s)),
                    [
                        (1, "2026-01-01"),
                        (2, "2026-01-01"),
                        (3, "2026-02-01"),
                        (4, None),
                        (5, None),
                    ],
                    ["order_id", "month_start"],
                ),
            ),
            (
                "ex5_ship_lag_days",
                lambda s: (
                    ex5_ship_lag_days(_orders(s)),
                    [
                        (1, 5),
                        (2, 2),
                        (3, 0),
                        (4, None),
                        (5, None),
                    ],
                    ["order_id", "lag_days"],
                ),
            ),
            (
                "ex6_year_month",
                lambda s: (
                    ex6_year_month(_orders(s)),
                    [
                        (1, 2026, 1),
                        (2, 2026, 1),
                        (3, 2026, 2),
                        (4, None, None),
                        (5, None, None),
                    ],
                    ["order_id", "y", "mo"],
                ),
            ),
            (
                "ex7_report_string",
                lambda s: (
                    ex7_report_string(_orders(s)),
                    [
                        (1, "15-01-2026"),
                        (2, "31-01-2026"),
                        (3, "01-02-2026"),
                        (4, None),
                        (5, None),
                    ],
                    ["order_id", "sold_str"],
                ),
            ),
            (
                "ex8_sum_by_month",
                lambda s: (
                    ex8_sum_by_month(_orders(s)),
                    [
                        ("2026-01-01", 150),
                        ("2026-02-01", 200),
                    ],
                    ["month_start", "total"],
                ),
            ),
        ],
    )

"""
Lesson 0007 drills — window functions.

Fill each function. Leave the signature alone.
Run from this directory:
  ../.venv/bin/python 07_window.py

Data (built in checker):

  orders: id INT, region STRING, amount INT, day INT
  rows:
    (1, "east", 100, 1)
    (2, "east", 200, 2)
    (3, "east", 200, 3)   # amount tie with id 2
    (4, "west", 150, 1)
    (5, "west",  80, 2)
    (6, "west", 300, 3)
    (7, "east",  50, 4)

Lesson knowledge: lessons/0007-window-functions.html
Stuck after a real try: exercises/solutions/07_window.py
"""

from __future__ import annotations

from pyspark.sql import DataFrame, Window
from pyspark.sql.functions import col, dense_rank, lag, lead, rank, row_number, sum

# ---------------------------------------------------------------------------
# YOUR CODE — replace `raise NotImplementedError`
# ---------------------------------------------------------------------------


def ex1_row_number_by_amount(orders: DataFrame) -> DataFrame:
    """
    Per region, order amount DESC then id ASC.
    rn = row_number().
    Columns: id, region, amount, rn
    """
    w = Window.partitionBy("region").orderBy(col("amount").desc(), "id")
    return (
        orders
            .withColumn("rn", row_number().over(w))
            .select("id", "region", "amount", "rn")
    )


def ex2_rank_by_amount(orders: DataFrame) -> DataFrame:
    """
    Per region, order amount DESC only (ties share rank).
    r = rank().
    Columns: id, region, amount, r
    """
    w = Window.partitionBy("region").orderBy(col("amount").desc())
    return (
        orders
            .withColumn("r", rank().over(w))
            .select("id","region","amount", "r")
    )


def ex3_dense_rank_by_amount(orders: DataFrame) -> DataFrame:
    """
    Per region, order amount DESC only.
    d = dense_rank().
    Columns: id, region, amount, d
    """
    w = Window.partitionBy("region").orderBy(col("amount").desc())
    return (
        orders
            .withColumn("d", dense_rank().over(w))
            .select("id", "region", "amount", "d")
    )


def ex4_lag_prev_amount(orders: DataFrame) -> DataFrame:
    """
    Per region, order by day.
    prev = lag(amount)  # null on first day
    Columns: id, region, day, amount, prev
    """
    w = Window.partitionBy("region").orderBy("day")
    return (
        orders
            .withColumn("prev", lag("amount").over(w))
            .select("id", "region", "day", "amount", "prev")
    )


def ex5_lead_next_amount(orders: DataFrame) -> DataFrame:
    """
    Per region, order by day.
    nxt = lead(amount)  # null on last day
    Columns: id, region, day, amount, nxt
    """
    w = Window.partitionBy("region").orderBy("day")
    return (
        orders
            .withColumn("nxt", lead("amount").over(w))
            .select("id", "region", "day", "amount", "nxt")
    )


def ex6_region_total(orders: DataFrame) -> DataFrame:
    """
    Per region (no orderBy): reg_total = sum(amount) over partition.
    All rows kept; same total repeated per region.
    Columns: id, region, amount, reg_total
    """
    w = Window.partitionBy("region")
    return (
        orders
            .withColumn("reg_total", sum("amount").over(w))
            .select("id", "region", "amount", "reg_total")
    )

def ex7_running_total(orders: DataFrame) -> DataFrame:
    """
    Per region, order by day: running = sum(amount) over window
    (default frame = running total).
    Columns: id, region, day, amount, running
    """
    w = Window.partitionBy("region").orderBy("day")
    return (
        orders
            .withColumn("running", sum("amount").over(w))
            .select("id", "region", "day", "amount", "running")
    )


def ex8_top_amount_per_region(orders: DataFrame) -> DataFrame:
    """
    Per region, order amount DESC then id ASC.
    Keep only rn == 1.
    Columns: id, region, amount
    """
    w = Window.partitionBy("region").orderBy(col("amount").desc(), col("id").asc())
    res = (orders
            .withColumn("rn", row_number().over(w))
            .filter(col("rn") == 1)
            .select("id", "region", "amount"))
    return res


# ---------------------------------------------------------------------------
# Checker — do not edit below
# ---------------------------------------------------------------------------

_ROWS = [
    (1, "east", 100, 1),
    (2, "east", 200, 2),
    (3, "east", 200, 3),
    (4, "west", 150, 1),
    (5, "west", 80, 2),
    (6, "west", 300, 3),
    (7, "east", 50, 4),
]
_SCHEMA = "id INT, region STRING, amount INT, day INT"


def _orders(spark):
    return spark.createDataFrame(_ROWS, _SCHEMA)


if __name__ == "__main__":
    from harness import run_suite

    run_suite(
        "07 window functions",
        [
            (
                "ex1_row_number_by_amount",
                lambda s: (
                    ex1_row_number_by_amount(_orders(s)),
                    [
                        (2, "east", 200, 1),
                        (3, "east", 200, 2),
                        (1, "east", 100, 3),
                        (7, "east", 50, 4),
                        (6, "west", 300, 1),
                        (4, "west", 150, 2),
                        (5, "west", 80, 3),
                    ],
                    ["id", "region", "amount", "rn"],
                ),
            ),
            (
                "ex2_rank_by_amount",
                lambda s: (
                    ex2_rank_by_amount(_orders(s)),
                    [
                        (2, "east", 200, 1),
                        (3, "east", 200, 1),
                        (1, "east", 100, 3),
                        (7, "east", 50, 4),
                        (6, "west", 300, 1),
                        (4, "west", 150, 2),
                        (5, "west", 80, 3),
                    ],
                    ["id", "region", "amount", "r"],
                ),
            ),
            (
                "ex3_dense_rank_by_amount",
                lambda s: (
                    ex3_dense_rank_by_amount(_orders(s)),
                    [
                        (2, "east", 200, 1),
                        (3, "east", 200, 1),
                        (1, "east", 100, 2),
                        (7, "east", 50, 3),
                        (6, "west", 300, 1),
                        (4, "west", 150, 2),
                        (5, "west", 80, 3),
                    ],
                    ["id", "region", "amount", "d"],
                ),
            ),
            (
                "ex4_lag_prev_amount",
                lambda s: (
                    ex4_lag_prev_amount(_orders(s)),
                    [
                        (1, "east", 1, 100, None),
                        (2, "east", 2, 200, 100),
                        (3, "east", 3, 200, 200),
                        (7, "east", 4, 50, 200),
                        (4, "west", 1, 150, None),
                        (5, "west", 2, 80, 150),
                        (6, "west", 3, 300, 80),
                    ],
                    ["id", "region", "day", "amount", "prev"],
                ),
            ),
            (
                "ex5_lead_next_amount",
                lambda s: (
                    ex5_lead_next_amount(_orders(s)),
                    [
                        (1, "east", 1, 100, 200),
                        (2, "east", 2, 200, 200),
                        (3, "east", 3, 200, 50),
                        (7, "east", 4, 50, None),
                        (4, "west", 1, 150, 80),
                        (5, "west", 2, 80, 300),
                        (6, "west", 3, 300, None),
                    ],
                    ["id", "region", "day", "amount", "nxt"],
                ),
            ),
            (
                "ex6_region_total",
                lambda s: (
                    ex6_region_total(_orders(s)),
                    [
                        (1, "east", 100, 550),
                        (2, "east", 200, 550),
                        (3, "east", 200, 550),
                        (7, "east", 50, 550),
                        (4, "west", 150, 530),
                        (5, "west", 80, 530),
                        (6, "west", 300, 530),
                    ],
                    ["id", "region", "amount", "reg_total"],
                ),
            ),
            (
                "ex7_running_total",
                lambda s: (
                    ex7_running_total(_orders(s)),
                    [
                        (1, "east", 1, 100, 100),
                        (2, "east", 2, 200, 300),
                        (3, "east", 3, 200, 500),
                        (7, "east", 4, 50, 550),
                        (4, "west", 1, 150, 150),
                        (5, "west", 2, 80, 230),
                        (6, "west", 3, 300, 530),
                    ],
                    ["id", "region", "day", "amount", "running"],
                ),
            ),
            (
                "ex8_top_amount_per_region",
                lambda s: (
                    ex8_top_amount_per_region(_orders(s)),
                    [
                        (2, "east", 200),
                        (6, "west", 300),
                    ],
                    ["id", "region", "amount"],
                ),
            ),
        ],
    )

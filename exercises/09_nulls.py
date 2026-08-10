"""
Lesson 0009 drills — nulls (isNull / drop / fill / coalesce).

Fill each function. Leave the signature alone.
Run from this directory:
  ../.venv/bin/python 09_nulls.py

Data (built in checker):

  orders: id INT, region STRING, amount INT, status STRING
  rows:
    (1, "east", 100,  "open")
    (2, "east", None, "open")
    (3, "west", 50,   None)
    (4, None,   None, "closed")
    (5, "west", 0,    "open")     # 0 is not null
    (6, "east", 200,  "closed")

Lesson knowledge: lessons/0009-nulls.html
Stuck after a real try: exercises/solutions/09_nulls.py
"""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, coalesce, lit,when  # noqa: F401 — use what you need

# ---------------------------------------------------------------------------
# YOUR CODE — replace `raise NotImplementedError`
# ---------------------------------------------------------------------------


def ex1_null_amounts(orders: DataFrame) -> DataFrame:
    """
    Keep rows where amount is null.
    Columns: id, region, amount, status
    """
    result = (
        orders
            .filter(col("amount").isNull())
            .select("id", "region", "amount", "status")
    )
    return result


def ex2_not_null_amounts(orders: DataFrame) -> DataFrame:
    """
    Keep rows where amount is not null (0 stays).
    Columns: id, region, amount, status
    """
    result = (
        orders
            .filter(col("amount").isNotNull())
            .select("id", "region", "amount", "status")
    )
    return result


def ex3_status_open(orders: DataFrame) -> DataFrame:
    """
    status == "open". Null status is dropped (three-valued logic).
    Columns: id, region, amount, status
    """
    result = (
        orders
            .filter(col("status")=="open")
            .select("id", "region", "amount", "status")
    )
    return result


def ex4_drop_amount_or_status_null(orders: DataFrame) -> DataFrame:
    """
    Drop rows if amount is null OR status is null.
    Columns: id, region, amount, status
    """
    result = (
        orders
            .filter((col("amount").isNotNull()) & (col("status").isNotNull()))
            .select("id", "region", "amount", "status")
    )
    return result


def ex5_fill_amount_zero(orders: DataFrame) -> DataFrame:
    """
    Fill null amount with 0. Other columns unchanged.
    Columns: id, region, amount, status
    """
    result = (
        orders.fillna({"amount":0}).select("id", "region", "amount", "status")
    )
    return result


def ex6_coalesce_region(orders: DataFrame) -> DataFrame:
    """
    region_filled = coalesce(region, "unknown").
    Columns: id, region, region_filled
    """
    result = (
        orders
            .withColumn("region_filled", coalesce(col("region"), lit("unknown")))
            .select("id", "region", "region_filled")
    )
    return result


def ex7_amount_flag(orders: DataFrame) -> DataFrame:
    """
    flag = "missing" if amount is null else "present".
    Columns: id, amount, flag
    """
    result = (
        orders
            .withColumn("flag", when(col("amount").isNull(), "missing").otherwise("present"))
            .select("id", "amount", "flag")
    )
    return result


def ex8_open_or_null_status(orders: DataFrame) -> DataFrame:
    """
    Keep status is null OR status == "open".
    Columns: id, region, amount, status
    """
    result = (
        orders
            .filter((col("status").isNull()) | (col("status")=="open"))
            .select("id", "region", "amount", "status")
    )
    return result

# ---------------------------------------------------------------------------
# Checker — do not edit below
# ---------------------------------------------------------------------------

_ROWS = [
    (1, "east", 100, "open"),
    (2, "east", None, "open"),
    (3, "west", 50, None),
    (4, None, None, "closed"),
    (5, "west", 0, "open"),
    (6, "east", 200, "closed"),
]
_SCHEMA = "id INT, region STRING, amount INT, status STRING"


def _orders(spark):
    return spark.createDataFrame(_ROWS, _SCHEMA)


if __name__ == "__main__":
    from harness import run_suite

    run_suite(
        "09 nulls",
        [
            (
                "ex1_null_amounts",
                lambda s: (
                    ex1_null_amounts(_orders(s)),
                    [
                        (2, "east", None, "open"),
                        (4, None, None, "closed"),
                    ],
                    ["id", "region", "amount", "status"],
                ),
            ),
            (
                "ex2_not_null_amounts",
                lambda s: (
                    ex2_not_null_amounts(_orders(s)),
                    [
                        (1, "east", 100, "open"),
                        (3, "west", 50, None),
                        (5, "west", 0, "open"),
                        (6, "east", 200, "closed"),
                    ],
                    ["id", "region", "amount", "status"],
                ),
            ),
            (
                "ex3_status_open",
                lambda s: (
                    ex3_status_open(_orders(s)),
                    [
                        (1, "east", 100, "open"),
                        (2, "east", None, "open"),
                        (5, "west", 0, "open"),
                    ],
                    ["id", "region", "amount", "status"],
                ),
            ),
            (
                "ex4_drop_amount_or_status_null",
                lambda s: (
                    ex4_drop_amount_or_status_null(_orders(s)),
                    [
                        (1, "east", 100, "open"),
                        (5, "west", 0, "open"),
                        (6, "east", 200, "closed"),
                    ],
                    ["id", "region", "amount", "status"],
                ),
            ),
            (
                "ex5_fill_amount_zero",
                lambda s: (
                    ex5_fill_amount_zero(_orders(s)),
                    [
                        (1, "east", 100, "open"),
                        (2, "east", 0, "open"),
                        (3, "west", 50, None),
                        (4, None, 0, "closed"),
                        (5, "west", 0, "open"),
                        (6, "east", 200, "closed"),
                    ],
                    ["id", "region", "amount", "status"],
                ),
            ),
            (
                "ex6_coalesce_region",
                lambda s: (
                    ex6_coalesce_region(_orders(s)),
                    [
                        (1, "east", "east"),
                        (2, "east", "east"),
                        (3, "west", "west"),
                        (4, None, "unknown"),
                        (5, "west", "west"),
                        (6, "east", "east"),
                    ],
                    ["id", "region", "region_filled"],
                ),
            ),
            (
                "ex7_amount_flag",
                lambda s: (
                    ex7_amount_flag(_orders(s)),
                    [
                        (1, 100, "present"),
                        (2, None, "missing"),
                        (3, 50, "present"),
                        (4, None, "missing"),
                        (5, 0, "present"),
                        (6, 200, "present"),
                    ],
                    ["id", "amount", "flag"],
                ),
            ),
            (
                "ex8_open_or_null_status",
                lambda s: (
                    ex8_open_or_null_status(_orders(s)),
                    [
                        (1, "east", 100, "open"),
                        (2, "east", None, "open"),
                        (3, "west", 50, None),
                        (5, "west", 0, "open"),
                    ],
                    ["id", "region", "amount", "status"],
                ),
            ),
        ],
    )

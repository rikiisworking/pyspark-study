"""
Lesson 0010 drills — nulls + pipeline mix (0001–0009).

No new API. Stack null handling with filter / join / groupBy / when.
Run:
  ../.venv/bin/python 10_nulls_pipeline.py

Data (built in checker):

  orders:     order_id INT, cust_id INT, amount INT, status STRING
  customers:  cust_id INT, name STRING, region STRING

  orders:
    (1, 10, 100,  "open")
    (2, 20, None, "open")
    (3, 10, 40,   None)
    (4, 99, 80,   "open")      # orphan cust_id
    (5, 20, 150,  "closed")
    (6, 30, 300,  "open")
    (7, 30, None, "closed")
    (8, 10, 200,  "closed")

  customers:
    (10, "Alice", "east")
    (20, "Bob",   None)        # null region
    (30, "Carol", "east")
    (40, "Dan",   "north")     # no orders

Lesson: lessons/0010-nulls-pipeline-mix.html
Solutions (after real try): exercises/solutions/10_nulls_pipeline.py
"""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import Window
from pyspark.sql.functions import col, coalesce, lit, sum, when  # noqa: F401

# ---------------------------------------------------------------------------
# YOUR CODE — replace `raise NotImplementedError`
# ---------------------------------------------------------------------------


def ex1_null_amounts(orders: DataFrame) -> DataFrame:
    """
    Keep rows where amount is null.
    Columns: order_id, cust_id, amount, status
    """
    return orders.filter(col("amount").isNull()).select(
        "order_id", "cust_id", "amount", "status"
    )


def ex2_fill_then_open(orders: DataFrame) -> DataFrame:
    """
    Fill null amounts with 0, then keep status == "open".
    Columns: order_id, cust_id, amount, status
    """
    return (
        orders.filter(col("status") == "open")
        .fillna({"amount": 0})
        .select("order_id", "cust_id", "amount", "status")
    )


def ex3_drop_then_inner(orders: DataFrame, customers: DataFrame) -> DataFrame:
    """
    Drop rows with null amount OR null status.
    Inner join customers.
    Columns: order_id, name, amount, status
    """
    return (
        orders.dropna(subset=["amount", "status"])
        .join(customers, "cust_id")
        .select("order_id", "name", "amount", "status")
    )


def ex4_left_coalesce_region(orders: DataFrame, customers: DataFrame) -> DataFrame:
    """
    Left join customers.
    Overwrite region with coalesce(region, "unknown").
    Columns: order_id, name, region, amount
    """
    return (
        orders.join(customers, "cust_id", "left")
        .withColumn("region", coalesce(col("region"), lit("unknown")))
        .select("order_id", "name", "region", "amount")
    )


def ex5_open_or_null_status_amount_ok(orders: DataFrame) -> DataFrame:
    """
    Keep rows where (status is null OR status == "open") AND amount is not null.
    Columns: order_id, status, amount
    """
    return orders.filter(
        ((col("status").isNull()) | (col("status") == "open"))
        & (col("amount").isNotNull())
    ).select("order_id", "status", "amount")


def ex6_fill_join_sum_by_name(orders: DataFrame, customers: DataFrame) -> DataFrame:
    """
    Fill null amounts with 0, inner join customers.
    Per name: total = sum(amount).
    Columns: name, total
    """
    return (
        orders.join(customers, "cust_id")
        .fillna({"amount": 0})
        .groupBy("name")
        .agg(sum("amount").alias("total"))
        .select("name", "total")
    )


def ex7_flag_missing_amount(orders: DataFrame, customers: DataFrame) -> DataFrame:
    """
    Inner join customers.
    flag = "missing" if amount is null else "ok".
    Columns: order_id, name, flag
    """
    return (
        orders.join(customers, "cust_id")
        .withColumn("flag", when(col("amount").isNull(), "missing").otherwise("ok"))
        .select("order_id", "name", "flag")
    )


def ex8_open_fill_sum_by_region(orders: DataFrame, customers: DataFrame) -> DataFrame:
    """
    Fill null amounts with 0, keep status == "open", inner join.
    region = coalesce(region, "unknown").
    Per region: total = sum(amount).
    Columns: region, total
    """
    return (
        orders.filter(col("status") == "open")
        .fillna({"amount": 0})
        .join(customers, "cust_id")
        .withColumn("region", coalesce(col("region"), lit("unknown")))
        .groupBy("region")
        .agg(sum("amount").alias("total"))
        .select("region", "total")
    )


# ---------------------------------------------------------------------------
# Checker — do not edit below
# ---------------------------------------------------------------------------

_ORDER_ROWS = [
    (1, 10, 100, "open"),
    (2, 20, None, "open"),
    (3, 10, 40, None),
    (4, 99, 80, "open"),
    (5, 20, 150, "closed"),
    (6, 30, 300, "open"),
    (7, 30, None, "closed"),
    (8, 10, 200, "closed"),
]
_ORDER_SCHEMA = "order_id INT, cust_id INT, amount INT, status STRING"

_CUST_ROWS = [
    (10, "Alice", "east"),
    (20, "Bob", None),
    (30, "Carol", "east"),
    (40, "Dan", "north"),
]
_CUST_SCHEMA = "cust_id INT, name STRING, region STRING"


def _orders(spark):
    return spark.createDataFrame(_ORDER_ROWS, _ORDER_SCHEMA)


def _customers(spark):
    return spark.createDataFrame(_CUST_ROWS, _CUST_SCHEMA)


if __name__ == "__main__":
    from harness import run_suite

    run_suite(
        "10 nulls + pipeline mix (0001–0009)",
        [
            (
                "ex1_null_amounts",
                lambda s: (
                    ex1_null_amounts(_orders(s)),
                    [
                        (2, 20, None, "open"),
                        (7, 30, None, "closed"),
                    ],
                    ["order_id", "cust_id", "amount", "status"],
                ),
            ),
            (
                "ex2_fill_then_open",
                lambda s: (
                    ex2_fill_then_open(_orders(s)),
                    [
                        (1, 10, 100, "open"),
                        (2, 20, 0, "open"),
                        (4, 99, 80, "open"),
                        (6, 30, 300, "open"),
                    ],
                    ["order_id", "cust_id", "amount", "status"],
                ),
            ),
            (
                "ex3_drop_then_inner",
                lambda s: (
                    ex3_drop_then_inner(_orders(s), _customers(s)),
                    [
                        (1, "Alice", 100, "open"),
                        (5, "Bob", 150, "closed"),
                        (6, "Carol", 300, "open"),
                        (8, "Alice", 200, "closed"),
                    ],
                    ["order_id", "name", "amount", "status"],
                ),
            ),
            (
                "ex4_left_coalesce_region",
                lambda s: (
                    ex4_left_coalesce_region(_orders(s), _customers(s)),
                    [
                        (1, "Alice", "east", 100),
                        (2, "Bob", "unknown", None),
                        (3, "Alice", "east", 40),
                        (4, None, "unknown", 80),
                        (5, "Bob", "unknown", 150),
                        (6, "Carol", "east", 300),
                        (7, "Carol", "east", None),
                        (8, "Alice", "east", 200),
                    ],
                    ["order_id", "name", "region", "amount"],
                ),
            ),
            (
                "ex5_open_or_null_status_amount_ok",
                lambda s: (
                    ex5_open_or_null_status_amount_ok(_orders(s)),
                    [
                        (1, "open", 100),
                        (3, None, 40),
                        (4, "open", 80),
                        (6, "open", 300),
                    ],
                    ["order_id", "status", "amount"],
                ),
            ),
            (
                "ex6_fill_join_sum_by_name",
                lambda s: (
                    ex6_fill_join_sum_by_name(_orders(s), _customers(s)),
                    [
                        ("Alice", 340),
                        ("Bob", 150),
                        ("Carol", 300),
                    ],
                    ["name", "total"],
                ),
            ),
            (
                "ex7_flag_missing_amount",
                lambda s: (
                    ex7_flag_missing_amount(_orders(s), _customers(s)),
                    [
                        (1, "Alice", "ok"),
                        (2, "Bob", "missing"),
                        (3, "Alice", "ok"),
                        (5, "Bob", "ok"),
                        (6, "Carol", "ok"),
                        (7, "Carol", "missing"),
                        (8, "Alice", "ok"),
                    ],
                    ["order_id", "name", "flag"],
                ),
            ),
            (
                "ex8_open_fill_sum_by_region",
                lambda s: (
                    ex8_open_fill_sum_by_region(_orders(s), _customers(s)),
                    [
                        ("east", 400),
                        ("unknown", 0),
                    ],
                    ["region", "total"],
                ),
            ),
        ],
    )

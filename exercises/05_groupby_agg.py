"""
Lesson 0005 drills — groupBy + agg.

Fill each function. Leave the signature alone.
Run from this directory:
  ../.venv/bin/python 05_groupby_agg.py

Data (built in checker):

  orders:  id INT, region STRING, amount INT, status STRING
  rows:
    (1, "east",  100, "open")
    (2, "west",  200, "open")
    (3, "east",   50, "closed")
    (4, "east",  150, "open")
    (5, "west",   80, "closed")
    (6, "north", 300, "open")

  sales / customers used only in ex8:
    sales:     order_id INT, cust_id INT, amount INT
    customers: cust_id INT, name STRING

Lesson knowledge: lessons/0005-groupby-agg.html
Stuck after a real try: exercises/solutions/05_groupby_agg.py
"""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql.functions import avg, col, count, max, min, sum

# ---------------------------------------------------------------------------
# YOUR CODE — replace `raise NotImplementedError`
# ---------------------------------------------------------------------------


def ex1_count_by_region(orders: DataFrame) -> DataFrame:
    """
    Count rows per region.
    Columns: region, count
    """
    return (
        orders.groupBy(col("region"))
        .agg(count("*").alias("count"))
        .select("region", "count")
    )


def ex2_sum_amount_default_name(orders: DataFrame) -> DataFrame:
    """
    Sum amount per region using .sum("amount") (default column name).
    Columns: region, sum(amount)
    """
    return orders.groupBy("region").agg(sum("amount")).select("region", "sum(amount)")


def ex3_avg_amount_aliased(orders: DataFrame) -> DataFrame:
    """
    Average amount per region.
    Columns: region, avg_amount
    """
    return (
        orders
            .groupBy("region")
            .agg(avg("amount").alias("avg_amount"))
            .select("region", "avg_amount")
    )

def ex4_multi_agg(orders: DataFrame) -> DataFrame:
    """
    Per region: n = count(*), total = sum(amount), hi = max(amount).
    Columns: region, n, total, hi
    """
    return (
        orders
            .groupBy("region")
            .agg(
                count("*").alias("n"),
                sum("amount").alias("total"),
                max("amount").alias("hi"))
            .select("region", "n", "total", "hi")
    )


def ex5_region_status_counts(orders: DataFrame) -> DataFrame:
    """
    Count rows per (region, status).
    Columns: region, status, count
    """
    return (
        orders
            .groupBy("region", "status")
            .agg(
                count("*").alias("count")
            )
            .select("region", "status", "count")
    )


def ex6_open_total_by_region(orders: DataFrame) -> DataFrame:
    """
    Keep status == "open", then sum amount per region as open_total.
    Columns: region, open_total
    """
    return (
        orders
            .filter(col("status") == "open")
            .groupBy("region")
            .agg(
                sum("amount").alias("open_total")
            )
            .select("region", "open_total")
    )


def ex7_big_region_totals(orders: DataFrame) -> DataFrame:
    """
    Sum amount per region as total, then keep total > 280.
    Columns: region, total
    """
    return (
        orders
            .groupBy("region")
            .agg(
                sum("amount").alias("total")
            )
            .filter(col("total") > 280)
            .select("region", "total")
    )


def ex8_spend_by_customer(sales: DataFrame, customers: DataFrame) -> DataFrame:
    """
    Inner join sales to customers on cust_id.
    Group by name: spend = sum(amount), orders = count(*).
    Columns: name, spend, orders
    """
    return (
        sales
            .join(customers, "cust_id")
            .groupBy("name")
            .agg(
                sum("amount").alias("spend"),
                count("*").alias("orders")
            )
            .select("name", "spend", "orders")
    )


# ---------------------------------------------------------------------------
# Checker — do not edit below
# ---------------------------------------------------------------------------

_ORDER_ROWS = [
    (1, "east", 100, "open"),
    (2, "west", 200, "open"),
    (3, "east", 50, "closed"),
    (4, "east", 150, "open"),
    (5, "west", 80, "closed"),
    (6, "north", 300, "open"),
]
_ORDER_SCHEMA = "id INT, region STRING, amount INT, status STRING"

_SALES_ROWS = [
    (1, 10, 100),
    (2, 20, 200),
    (3, 10, 50),
    (4, 10, 150),
]
_SALES_SCHEMA = "order_id INT, cust_id INT, amount INT"

_CUST_ROWS = [
    (10, "Alice"),
    (20, "Bob"),
    (30, "Carol"),
]
_CUST_SCHEMA = "cust_id INT, name STRING"


def _orders(spark):
    return spark.createDataFrame(_ORDER_ROWS, _ORDER_SCHEMA)


def _sales(spark):
    return spark.createDataFrame(_SALES_ROWS, _SALES_SCHEMA)


def _customers(spark):
    return spark.createDataFrame(_CUST_ROWS, _CUST_SCHEMA)


if __name__ == "__main__":
    from harness import run_suite

    run_suite(
        "05 groupBy + agg",
        [
            (
                "ex1_count_by_region",
                lambda s: (
                    ex1_count_by_region(_orders(s)),
                    [
                        ("east", 3),
                        ("west", 2),
                        ("north", 1),
                    ],
                    ["region", "count"],
                ),
            ),
            (
                "ex2_sum_amount_default_name",
                lambda s: (
                    ex2_sum_amount_default_name(_orders(s)),
                    [
                        ("east", 300),
                        ("west", 280),
                        ("north", 300),
                    ],
                    ["region", "sum(amount)"],
                ),
            ),
            (
                "ex3_avg_amount_aliased",
                lambda s: (
                    ex3_avg_amount_aliased(_orders(s)),
                    [
                        ("east", 100.0),
                        ("west", 140.0),
                        ("north", 300.0),
                    ],
                    ["region", "avg_amount"],
                ),
            ),
            (
                "ex4_multi_agg",
                lambda s: (
                    ex4_multi_agg(_orders(s)),
                    [
                        ("east", 3, 300, 150),
                        ("west", 2, 280, 200),
                        ("north", 1, 300, 300),
                    ],
                    ["region", "n", "total", "hi"],
                ),
            ),
            (
                "ex5_region_status_counts",
                lambda s: (
                    ex5_region_status_counts(_orders(s)),
                    [
                        ("east", "open", 2),
                        ("east", "closed", 1),
                        ("west", "open", 1),
                        ("west", "closed", 1),
                        ("north", "open", 1),
                    ],
                    ["region", "status", "count"],
                ),
            ),
            (
                "ex6_open_total_by_region",
                lambda s: (
                    ex6_open_total_by_region(_orders(s)),
                    [
                        ("east", 250),
                        ("west", 200),
                        ("north", 300),
                    ],
                    ["region", "open_total"],
                ),
            ),
            (
                "ex7_big_region_totals",
                lambda s: (
                    ex7_big_region_totals(_orders(s)),
                    [
                        ("east", 300),
                        ("north", 300),
                    ],
                    ["region", "total"],
                ),
            ),
            (
                "ex8_spend_by_customer",
                lambda s: (
                    ex8_spend_by_customer(_sales(s), _customers(s)),
                    [
                        ("Alice", 300, 3),
                        ("Bob", 200, 1),
                    ],
                    ["name", "spend", "orders"],
                ),
            ),
        ],
    )

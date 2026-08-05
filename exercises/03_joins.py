"""
Lesson 0003 drills — DataFrame joins.

Fill each function. Leave the signature alone.
Run from this directory:
  ../.venv/bin/python 03_joins.py

Two frames (built in checker):

  orders:     order_id INT, cust_id INT, amount INT
  customers:  cust_id INT, name STRING, region STRING

Lesson knowledge: lessons/0003-joins.html
Stuck after a real try: exercises/solutions/03_joins.py
"""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql.functions import col

# ---------------------------------------------------------------------------
# YOUR CODE — replace `raise NotImplementedError`
# ---------------------------------------------------------------------------


def ex1_inner_on_key(orders: DataFrame, customers: DataFrame) -> DataFrame:
    """
    Inner join on cust_id (shared key name).
    Columns: order_id, name, amount
    """
    raise NotImplementedError


def ex2_left_orders(orders: DataFrame, customers: DataFrame) -> DataFrame:
    """
    Left join: keep every order; customer fields null if no match.
    Columns: order_id, name, amount
    """
    raise NotImplementedError


def ex3_right_customers(orders: DataFrame, customers: DataFrame) -> DataFrame:
    """
    Right join: keep every customer; order fields null if no match.
    Columns: name, order_id
    """
    raise NotImplementedError


def ex4_full_outer(orders: DataFrame, customers: DataFrame) -> DataFrame:
    """
    Full outer on cust_id.
    Columns: order_id, name
    """
    raise NotImplementedError


def ex5_left_semi(orders: DataFrame, customers: DataFrame) -> DataFrame:
    """
    Left semi: orders that have a matching customer.
    Columns (order matters): order_id, cust_id, amount
    Left-side fields only. Use select(...) to pin order if needed.
    """
    raise NotImplementedError


def ex6_left_anti(orders: DataFrame, customers: DataFrame) -> DataFrame:
    """
    Left anti: orders with NO matching customer.
    Columns (order matters): order_id, cust_id, amount
    """
    raise NotImplementedError


def ex7_inner_east_only(orders: DataFrame, customers: DataFrame) -> DataFrame:
    """
    Inner join, then keep region == "east".
    Columns: order_id, name, region, amount
    """
    raise NotImplementedError


def ex8_column_join_select(orders: DataFrame, customers: DataFrame) -> DataFrame:
    """
    Inner join with Column condition (not string key):
      orders.cust_id == customers.cust_id
    Then select without ambiguous duplicate keys:
      order_id, name, amount
    Tip: alias frames or pick columns from one side only.
    """
    raise NotImplementedError


# ---------------------------------------------------------------------------
# Checker — do not edit below
# ---------------------------------------------------------------------------

_ORDER_ROWS = [
    (1, 10, 100),
    (2, 20, 200),
    (3, 10, 50),
    (4, 99, 80),  # orphan order
]
_ORDER_SCHEMA = "order_id INT, cust_id INT, amount INT"

_CUST_ROWS = [
    (10, "Alice", "east"),
    (20, "Bob", "west"),
    (30, "Carol", "east"),  # no orders
]
_CUST_SCHEMA = "cust_id INT, name STRING, region STRING"


def _orders(spark):
    return spark.createDataFrame(_ORDER_ROWS, _ORDER_SCHEMA)


def _customers(spark):
    return spark.createDataFrame(_CUST_ROWS, _CUST_SCHEMA)


if __name__ == "__main__":
    from harness import run_suite

    run_suite(
        "03 joins",
        [
            (
                "ex1_inner_on_key",
                lambda s: (
                    ex1_inner_on_key(_orders(s), _customers(s)),
                    [
                        (1, "Alice", 100),
                        (2, "Bob", 200),
                        (3, "Alice", 50),
                    ],
                    ["order_id", "name", "amount"],
                ),
            ),
            (
                "ex2_left_orders",
                lambda s: (
                    ex2_left_orders(_orders(s), _customers(s)),
                    [
                        (1, "Alice", 100),
                        (2, "Bob", 200),
                        (3, "Alice", 50),
                        (4, None, 80),
                    ],
                    ["order_id", "name", "amount"],
                ),
            ),
            (
                "ex3_right_customers",
                lambda s: (
                    ex3_right_customers(_orders(s), _customers(s)),
                    [
                        ("Alice", 1),
                        ("Alice", 3),
                        ("Bob", 2),
                        ("Carol", None),
                    ],
                    ["name", "order_id"],
                ),
            ),
            (
                "ex4_full_outer",
                lambda s: (
                    ex4_full_outer(_orders(s), _customers(s)),
                    [
                        (1, "Alice"),
                        (2, "Bob"),
                        (3, "Alice"),
                        (4, None),
                        (None, "Carol"),
                    ],
                    ["order_id", "name"],
                ),
            ),
            (
                "ex5_left_semi",
                lambda s: (
                    ex5_left_semi(_orders(s), _customers(s)),
                    [
                        (1, 10, 100),
                        (2, 20, 200),
                        (3, 10, 50),
                    ],
                    ["order_id", "cust_id", "amount"],
                ),
            ),
            (
                "ex6_left_anti",
                lambda s: (
                    ex6_left_anti(_orders(s), _customers(s)),
                    [(4, 99, 80)],
                    ["order_id", "cust_id", "amount"],
                ),
            ),
            (
                "ex7_inner_east_only",
                lambda s: (
                    ex7_inner_east_only(_orders(s), _customers(s)),
                    [
                        (1, "Alice", "east", 100),
                        (3, "Alice", "east", 50),
                    ],
                    ["order_id", "name", "region", "amount"],
                ),
            ),
            (
                "ex8_column_join_select",
                lambda s: (
                    ex8_column_join_select(_orders(s), _customers(s)),
                    [
                        (1, "Alice", 100),
                        (2, "Bob", 200),
                        (3, "Alice", 50),
                    ],
                    ["order_id", "name", "amount"],
                ),
            ),
        ],
    )

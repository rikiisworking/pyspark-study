"""
Integration drills — lessons 0001–0003 mixed.

Not one-API toys. Each function is a small pipeline:
  filter / select / withColumn / when / lit / cast / rename / drop / join

Data (built in checker):

  orders:     order_id INT, cust_id INT, amount INT, status STRING
  customers:  cust_id INT, name STRING, region STRING, tier STRING

  orders rows:
    (1, 10, 100, "open")
    (2, 20, 250, "open")
    (3, 10, 40,  "closed")
    (4, 99, 80,  "open")     # orphan cust_id
    (5, 20, 150, "closed")
    (6, 30, 300, "open")

  customers rows:
    (10, "Alice", "east",  "gold")
    (20, "Bob",   "west",  "silver")
    (30, "Carol", "east",  "gold")
    (40, "Dan",   "north", "bronze")  # no orders

Run:
  ../.venv/bin/python 04_pipeline_mix.py

Solutions (after real try): exercises/solutions/04_pipeline_mix.py
Cheat sheet: lessons/0004-pipeline-mix.html
"""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, lit, when

# ---------------------------------------------------------------------------
# YOUR CODE
# ---------------------------------------------------------------------------


def ex1_open_high_east(
    orders: DataFrame, customers: DataFrame
) -> DataFrame:
    """
    Open orders only (status == "open"), amount > 100.
    Inner-join customers. Keep region == "east".
    Columns: order_id, name, amount, region
    """
    raise NotImplementedError


def ex2_enrich_band_and_tax(
    orders: DataFrame, customers: DataFrame
) -> DataFrame:
    """
    Left-join customers onto orders (keep every order).
    Add:
      band  = "high" if amount >= 150 else "low"
      tax   = amount * 0.1
    Drop status and tier.
    Columns: order_id, cust_id, name, region, amount, band, tax
    """
    raise NotImplementedError


def ex3_gold_customers_order_stats_shape(
    orders: DataFrame, customers: DataFrame
) -> DataFrame:
    """
    Customers with tier == "gold" who have at least one order
    (left_semi against orders on cust_id).
    Add version = lit("v1").
    Columns: cust_id, name, region, version
    """
    raise NotImplementedError


def ex4_orphan_orders_flagged(
    orders: DataFrame, customers: DataFrame
) -> DataFrame:
    """
    Orders with NO matching customer (left_anti).
    Add flag = lit("orphan").
    Columns: order_id, cust_id, amount, flag
    """
    raise NotImplementedError


def ex5_closed_west_rename(
    orders: DataFrame, customers: DataFrame
) -> DataFrame:
    """
    Inner join. Keep status == "closed" and region == "west".
    Rename amount -> spend. Cast spend to double as spend_d
    (keep spend too).
    Columns: order_id, name, spend, spend_d
    """
    raise NotImplementedError


def ex6_full_roster(
    orders: DataFrame, customers: DataFrame
) -> DataFrame:
    """
    Full outer join on cust_id.
    Add has_order = when(order_id.isNotNull(), "yes").otherwise("no")
    Columns: cust_id, name, order_id, has_order
    Note: after string-key full join there is one cust_id.
    """
    raise NotImplementedError


def ex7_pipeline_chain(
    orders: DataFrame, customers: DataFrame
) -> DataFrame:
    """
    One chain (or clear steps — result matters):
      1. Inner join on cust_id
      2. Filter status == "open"
      3. withColumn fee = when(tier == "gold", lit(0)).otherwise(amount * 0.05)
      4. Drop status, tier, region
      5. Select: order_id, name, amount, fee
    """
    raise NotImplementedError


def ex8_column_join_project(
    orders: DataFrame, customers: DataFrame
) -> DataFrame:
    """
    Inner join with Column condition (not string key):
      orders.cust_id == customers.cust_id
    Keep amount >= 100.
    Project: order_id, customer_name (alias of name), amount
    Use aliases if you hit ambiguous columns.
    """
    raise NotImplementedError


# ---------------------------------------------------------------------------
# Checker — do not edit below
# ---------------------------------------------------------------------------

_ORDER_ROWS = [
    (1, 10, 100, "open"),
    (2, 20, 250, "open"),
    (3, 10, 40, "closed"),
    (4, 99, 80, "open"),
    (5, 20, 150, "closed"),
    (6, 30, 300, "open"),
]
_ORDER_SCHEMA = "order_id INT, cust_id INT, amount INT, status STRING"

_CUST_ROWS = [
    (10, "Alice", "east", "gold"),
    (20, "Bob", "west", "silver"),
    (30, "Carol", "east", "gold"),
    (40, "Dan", "north", "bronze"),
]
_CUST_SCHEMA = "cust_id INT, name STRING, region STRING, tier STRING"


def _orders(spark):
    return spark.createDataFrame(_ORDER_ROWS, _ORDER_SCHEMA)


def _customers(spark):
    return spark.createDataFrame(_CUST_ROWS, _CUST_SCHEMA)


if __name__ == "__main__":
    from harness import run_suite

    run_suite(
        "04 pipeline mix (0001–0003)",
        [
            (
                "ex1_open_high_east",
                lambda s: (
                    ex1_open_high_east(_orders(s), _customers(s)),
                    [
                        (6, "Carol", 300, "east"),
                    ],
                    ["order_id", "name", "amount", "region"],
                ),
            ),
            (
                "ex2_enrich_band_and_tax",
                lambda s: (
                    ex2_enrich_band_and_tax(_orders(s), _customers(s)),
                    [
                        (1, 10, "Alice", "east", 100, "low", 10.0),
                        (2, 20, "Bob", "west", 250, "high", 25.0),
                        (3, 10, "Alice", "east", 40, "low", 4.0),
                        (4, 99, None, None, 80, "low", 8.0),
                        (5, 20, "Bob", "west", 150, "high", 15.0),
                        (6, 30, "Carol", "east", 300, "high", 30.0),
                    ],
                    [
                        "order_id",
                        "cust_id",
                        "name",
                        "region",
                        "amount",
                        "band",
                        "tax",
                    ],
                ),
            ),
            (
                "ex3_gold_customers_order_stats_shape",
                lambda s: (
                    ex3_gold_customers_order_stats_shape(
                        _orders(s), _customers(s)
                    ),
                    [
                        (10, "Alice", "east", "v1"),
                        (30, "Carol", "east", "v1"),
                    ],
                    ["cust_id", "name", "region", "version"],
                ),
            ),
            (
                "ex4_orphan_orders_flagged",
                lambda s: (
                    ex4_orphan_orders_flagged(_orders(s), _customers(s)),
                    [(4, 99, 80, "orphan")],
                    ["order_id", "cust_id", "amount", "flag"],
                ),
            ),
            (
                "ex5_closed_west_rename",
                lambda s: (
                    ex5_closed_west_rename(_orders(s), _customers(s)),
                    [(5, "Bob", 150, 150.0)],
                    ["order_id", "name", "spend", "spend_d"],
                ),
            ),
            (
                "ex6_full_roster",
                lambda s: (
                    ex6_full_roster(_orders(s), _customers(s)),
                    [
                        (10, "Alice", 1, "yes"),
                        (10, "Alice", 3, "yes"),
                        (20, "Bob", 2, "yes"),
                        (20, "Bob", 5, "yes"),
                        (30, "Carol", 6, "yes"),
                        (40, "Dan", None, "no"),
                        (99, None, 4, "yes"),
                    ],
                    ["cust_id", "name", "order_id", "has_order"],
                ),
            ),
            (
                "ex7_pipeline_chain",
                lambda s: (
                    ex7_pipeline_chain(_orders(s), _customers(s)),
                    [
                        (1, "Alice", 100, 0.0),
                        (2, "Bob", 250, 12.5),
                        (6, "Carol", 300, 0.0),
                    ],
                    ["order_id", "name", "amount", "fee"],
                ),
            ),
            (
                "ex8_column_join_project",
                lambda s: (
                    ex8_column_join_project(_orders(s), _customers(s)),
                    [
                        (1, "Alice", 100),
                        (2, "Bob", 250),
                        (5, "Bob", 150),
                        (6, "Carol", 300),
                    ],
                    ["order_id", "customer_name", "amount"],
                ),
            ),
        ],
    )

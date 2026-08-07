"""
Lesson 0006 drills — full pipeline mix (0001–0005).

No new API. Stack filter / withColumn / join / groupBy / agg.
Run:
  ../.venv/bin/python 06_full_pipeline.py

Data (built in checker):

  orders:     order_id INT, cust_id INT, amount INT, status STRING
  customers:  cust_id INT, name STRING, region STRING, tier STRING

  orders:
    (1, 10, 100, "open")
    (2, 20, 250, "open")
    (3, 10,  40, "closed")
    (4, 99,  80, "open")     # orphan
    (5, 20, 150, "closed")
    (6, 30, 300, "open")
    (7, 30,  50, "open")

  customers:
    (10, "Alice", "east",  "gold")
    (20, "Bob",   "west",  "silver")
    (30, "Carol", "east",  "gold")
    (40, "Dan",   "north", "bronze")  # no orders

Lesson: lessons/0006-full-pipeline-mix.html
Solutions (after real try): exercises/solutions/06_full_pipeline.py
"""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, count, lit, sum, when

# ---------------------------------------------------------------------------
# YOUR CODE — replace `raise NotImplementedError`
# ---------------------------------------------------------------------------


def ex1_open_high_named(orders: DataFrame, customers: DataFrame) -> DataFrame:
    """
    status == "open" AND amount >= 100.
    Inner join customers.
    Columns: order_id, name, amount, region
    """
    return (
        orders
            .filter((col("status") == "open") & (col("amount") >= 100))
            .join(customers, "cust_id")
            .select("order_id", "name", "amount", "region")
    )


def ex2_band_and_tax(orders: DataFrame, customers: DataFrame) -> DataFrame:
    """
    Left join customers onto orders.
    Keep amount >= 100.
    band = "high" if amount >= 150 else "low"
    tax  = amount * 0.1
    Columns: order_id, name, amount, band, tax
    """
    return (
        orders
            .filter(col("amount") >= 100)
            .withColumn("band", when(col("amount") >= 150, "high").otherwise("low"))
            .withColumn("tax", col("amount") * 0.1)
            .join(customers, "cust_id", "left")
            .select("order_id", "name", "amount", "band", "tax")
    )


def ex3_open_total_by_region(
    orders: DataFrame, customers: DataFrame
) -> DataFrame:
    """
    status == "open", inner join, sum amount per region as open_total.
    Columns: region, open_total
    """
    return (
        orders
            .filter(col("status") == "open")
            .join(customers, "cust_id")
            .groupBy("region")
            .agg(
                sum("amount").alias("open_total")
            )
            .select("region", "open_total")
    )


def ex4_spend_by_tier(orders: DataFrame, customers: DataFrame) -> DataFrame:
    """
    Inner join. Per tier: spend = sum(amount), n = count(*).
    Columns: tier, spend, n
    """
    return (
        orders
            .join(customers, "cust_id")
            .groupBy("tier")
            .agg(
                sum("amount").alias("spend"),
                count("*").alias("n")
            )
            .select("tier", "spend", "n")
    )


def ex5_big_east_customers(
    orders: DataFrame, customers: DataFrame
) -> DataFrame:
    """
    Inner join. region == "east".
    Per name: spend = sum(amount). Keep spend > 200.
    Columns: name, spend
    """
    return (
        orders
            .join(customers, "cust_id")
            .filter(col("region") == "east")
            .groupby("name")
            .agg(
                sum("amount").alias("spend")
            )
            .filter(col("spend") > 200)
            .select("name", "spend")
    )


def ex6_order_counts_all_customers(
    orders: DataFrame, customers: DataFrame
) -> DataFrame:
    """
    Left join: keep every customer (customers left, orders right).
    Per name: n_orders = count(order_id)  # null orders → 0
    Columns: name, n_orders
    """
    return (
        orders
            .join(customers, "cust_id", "right")
            .groupby("name")
            .agg(
                count("order_id").alias("n_orders")
            )
            .select("name", "n_orders")
    )


def ex7_high_value_gold(orders: DataFrame, customers: DataFrame) -> DataFrame:
    """
    amount >= 100, inner join, tier == "gold".
    Per name: spend = sum(amount).
    Columns: name, spend
    """
    return (
        orders
            .filter(col("amount") >= 100)
            .join(customers, "cust_id")
            .filter(col("tier") == "gold")
            .groupby("name")
            .agg(sum("amount").alias("spend"))
            .select("name", "spend")
    )


def ex8_open_fee_by_customer(
    orders: DataFrame, customers: DataFrame
) -> DataFrame:
    """
    Full chain:
      1. status == "open"
      2. inner join on cust_id
      3. fee = 0 if tier == "gold" else amount * 0.05
      4. groupBy name: orders = count(*), fee_total = sum(fee)
    Columns: name, orders, fee_total
    """
    return (
        orders
            .filter(col("status") == "open")
            .join(customers, "cust_id")
            .withColumn("fee", when(col("tier")=="gold", 0).otherwise(col("amount") * 0.05))
            .groupby("name")
            .agg(
                count("*").alias("orders"),
                sum("fee").alias("fee_total")
            )
            .select("name", "orders", "fee_total")
    )


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
    (7, 30, 50, "open"),
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
        "06 full pipeline mix (0001–0005)",
        [
            (
                "ex1_open_high_named",
                lambda s: (
                    ex1_open_high_named(_orders(s), _customers(s)),
                    [
                        (1, "Alice", 100, "east"),
                        (2, "Bob", 250, "west"),
                        (6, "Carol", 300, "east"),
                    ],
                    ["order_id", "name", "amount", "region"],
                ),
            ),
            (
                "ex2_band_and_tax",
                lambda s: (
                    ex2_band_and_tax(_orders(s), _customers(s)),
                    [
                        (1, "Alice", 100, "low", 10.0),
                        (2, "Bob", 250, "high", 25.0),
                        (5, "Bob", 150, "high", 15.0),
                        (6, "Carol", 300, "high", 30.0),
                    ],
                    ["order_id", "name", "amount", "band", "tax"],
                ),
            ),
            (
                "ex3_open_total_by_region",
                lambda s: (
                    ex3_open_total_by_region(_orders(s), _customers(s)),
                    [
                        ("east", 450),
                        ("west", 250),
                    ],
                    ["region", "open_total"],
                ),
            ),
            (
                "ex4_spend_by_tier",
                lambda s: (
                    ex4_spend_by_tier(_orders(s), _customers(s)),
                    [
                        ("gold", 490, 4),
                        ("silver", 400, 2),
                    ],
                    ["tier", "spend", "n"],
                ),
            ),
            (
                "ex5_big_east_customers",
                lambda s: (
                    ex5_big_east_customers(_orders(s), _customers(s)),
                    [("Carol", 350)],
                    ["name", "spend"],
                ),
            ),
            (
                "ex6_order_counts_all_customers",
                lambda s: (
                    ex6_order_counts_all_customers(_orders(s), _customers(s)),
                    [
                        ("Alice", 2),
                        ("Bob", 2),
                        ("Carol", 2),
                        ("Dan", 0),
                    ],
                    ["name", "n_orders"],
                ),
            ),
            (
                "ex7_high_value_gold",
                lambda s: (
                    ex7_high_value_gold(_orders(s), _customers(s)),
                    [
                        ("Alice", 100),
                        ("Carol", 300),
                    ],
                    ["name", "spend"],
                ),
            ),
            (
                "ex8_open_fee_by_customer",
                lambda s: (
                    ex8_open_fee_by_customer(_orders(s), _customers(s)),
                    [
                        ("Alice", 1, 0.0),
                        ("Bob", 1, 12.5),
                        ("Carol", 2, 0.0),
                    ],
                    ["name", "orders", "fee_total"],
                ),
            ),
        ],
    )

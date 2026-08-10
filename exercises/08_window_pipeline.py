"""
Lesson 0008 drills — window + full pipeline mix (0001–0007).

No new API. Stack filter / join / withColumn / groupBy / window.
Run:
  ../.venv/bin/python 08_window_pipeline.py

Data (built in checker):

  orders:     order_id INT, cust_id INT, amount INT, status STRING, day INT
  customers:  cust_id INT, name STRING, region STRING, tier STRING

  orders:
    (1, 10, 100, "open",   1)
    (2, 20, 250, "open",   1)
    (3, 10,  40, "closed", 2)
    (4, 99,  80, "open",   2)     # orphan
    (5, 20, 150, "closed", 2)
    (6, 30, 300, "open",   2)
    (7, 30,  50, "open",   3)
    (8, 10, 200, "open",   4)
    (9, 20, 100, "open",   3)

  customers:
    (10, "Alice", "east",  "gold")
    (20, "Bob",   "west",  "silver")
    (30, "Carol", "east",  "gold")
    (40, "Dan",   "north", "bronze")  # no orders

Lesson: lessons/0008-window-pipeline-mix.html
Solutions (after real try): exercises/solutions/08_window_pipeline.py
"""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql.functions import col  # noqa: F401 — use what you need
from pyspark.sql import Window
from pyspark.sql.functions import lag, rank, dense_rank, row_number, sum, when

# ---------------------------------------------------------------------------
# YOUR CODE — replace `raise NotImplementedError`
# ---------------------------------------------------------------------------


def ex1_open_rn_by_region(
    orders: DataFrame, customers: DataFrame
) -> DataFrame:
    """
    status == "open", inner join customers.
    rn = row_number per region, amount DESC then order_id ASC.
    Columns: order_id, name, region, amount, rn
    """
    w = Window.partitionBy("region").orderBy(col("amount").desc(), col("order_id").asc())
    result = (
        orders
            .filter(col("status") == "open")
            .join(customers, "cust_id")
            .withColumn("rn", row_number().over(w))
            .select("order_id", "name", "region", "amount", "rn")
    )
    return result


def ex2_top_open_per_region(    
    orders: DataFrame, customers: DataFrame
) -> DataFrame:
    """
    Open + inner join. Top-1 open order per region by amount DESC, order_id ASC.
    Columns: order_id, name, region, amount
    """
    w = Window.partitionBy("region").orderBy(col("amount").desc(), "order_id")
    result = (
        orders
            .filter(col("status") == "open")
            .join(customers, "cust_id")
            .withColumn("rn", row_number().over(w))
            .filter(col("rn") == 1)
            .select("order_id", "name", "region", "amount")
    )
    return result


def ex3_lag_amount_by_name(
    orders: DataFrame, customers: DataFrame
) -> DataFrame:
    """
    Inner join (all statuses). Per name, order by day.
    prev = lag(amount). First day for that name → null.
    Columns: order_id, name, day, amount, prev
    """
    w = Window.partitionBy("name").orderBy("day")
    result = (
        orders
            .join(customers, "cust_id")
            .withColumn("prev", lag("amount").over(w))
            .select("order_id", "name", "day", "amount", "prev")
    )
    return result


def ex4_open_running_by_region(
    orders: DataFrame, customers: DataFrame
) -> DataFrame:
    """
    Open + inner join. running = sum(amount) over region ordered by day
    (default ordered frame = cumulative).
    Columns: order_id, name, region, day, amount, running
    """
    w = Window.partitionBy("region").orderBy("day")
    result = (
        orders
            .filter(col("status")=="open")
            .join(customers, "cust_id")
            .withColumn("running", sum("amount").over(w))
            .select("order_id", "name", "region", "day", "amount", "running")
            
    )
    return result


def ex5_top_amount_per_tier(
    orders: DataFrame, customers: DataFrame
) -> DataFrame:
    """
    Inner join all rows. Per tier, rank by amount DESC.
    Keep rank == 1 (ties share rank — data has unique tops).
    Columns: order_id, name, tier, amount
    """
    w = Window.partitionBy("tier").orderBy(col("amount").desc())
    result = (
        orders
            .join(customers, "cust_id")
            .withColumn("rank", rank().over(w))
            .filter(col("rank")==1)
            .select("order_id", "name", "tier", "amount")
    )
    return result


def ex6_high_dense_rank(
    orders: DataFrame, customers: DataFrame
) -> DataFrame:
    """
    amount >= 100, inner join.
    d = dense_rank per region, amount DESC.
    Columns: order_id, name, region, amount, d
    """
    w = Window.partitionBy("region").orderBy(col("amount").desc())
    result = (
        orders
            .join(customers, "cust_id")
            .filter(col("amount") >= 100)
            .withColumn("d", dense_rank().over(w))
            .select("order_id", "name", "region", "amount", "d")
    )
    return result


def ex7_open_delta_by_name(
    orders: DataFrame, customers: DataFrame
) -> DataFrame:
    """
    Open + inner join. Per name order by day:
      prev  = lag(amount)
      delta = amount - prev   # null on first open day for that name
    Columns: order_id, name, day, amount, prev, delta
    """
    w = Window.partitionBy("name").orderBy("day")
    result = (
        orders
            .filter(col("status") == "open")
            .join(customers, "cust_id")
            .withColumn("prev", lag("amount").over(w))
            .withColumn("delta", col("amount")-col("prev"))
            .select("order_id", "name", "day", "amount", "prev", "delta")
    )
    return result


def ex8_top2_open_per_region(
    orders: DataFrame, customers: DataFrame
) -> DataFrame:
    """
    Open + inner join.
    rn = row_number per region, amount DESC then order_id ASC.
    Keep rn <= 2.
    Columns: order_id, name, region, amount, rn
    """
    w = Window.partitionBy("region").orderBy(col("amount").desc(), col("order_id"))
    result = (
        orders
            .filter(col("status")=="open")
            .join(customers, "cust_id")
            .withColumn("rn", row_number().over(w))
            .filter(col("rn") <= 2)
            .select("order_id", "name", "region", "amount", "rn")
    )
    return result


# ---------------------------------------------------------------------------
# Checker — do not edit below
# ---------------------------------------------------------------------------

_ORDER_ROWS = [
    (1, 10, 100, "open", 1),
    (2, 20, 250, "open", 1),
    (3, 10, 40, "closed", 2),
    (4, 99, 80, "open", 2),
    (5, 20, 150, "closed", 2),
    (6, 30, 300, "open", 2),
    (7, 30, 50, "open", 3),
    (8, 10, 200, "open", 4),
    (9, 20, 100, "open", 3),
]
_ORDER_SCHEMA = "order_id INT, cust_id INT, amount INT, status STRING, day INT"

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
        "08 window + pipeline mix (0001–0007)",
        [
            (
                "ex1_open_rn_by_region",
                lambda s: (
                    ex1_open_rn_by_region(_orders(s), _customers(s)),
                    [
                        (6, "Carol", "east", 300, 1),
                        (8, "Alice", "east", 200, 2),
                        (1, "Alice", "east", 100, 3),
                        (7, "Carol", "east", 50, 4),
                        (2, "Bob", "west", 250, 1),
                        (9, "Bob", "west", 100, 2),
                    ],
                    ["order_id", "name", "region", "amount", "rn"],
                ),
            ),
            (
                "ex2_top_open_per_region",
                lambda s: (
                    ex2_top_open_per_region(_orders(s), _customers(s)),
                    [
                        (6, "Carol", "east", 300),
                        (2, "Bob", "west", 250),
                    ],
                    ["order_id", "name", "region", "amount"],
                ),
            ),
            (
                "ex3_lag_amount_by_name",
                lambda s: (
                    ex3_lag_amount_by_name(_orders(s), _customers(s)),
                    [
                        (1, "Alice", 1, 100, None),
                        (3, "Alice", 2, 40, 100),
                        (8, "Alice", 4, 200, 40),
                        (2, "Bob", 1, 250, None),
                        (5, "Bob", 2, 150, 250),
                        (9, "Bob", 3, 100, 150),
                        (6, "Carol", 2, 300, None),
                        (7, "Carol", 3, 50, 300),
                    ],
                    ["order_id", "name", "day", "amount", "prev"],
                ),
            ),
            (
                "ex4_open_running_by_region",
                lambda s: (
                    ex4_open_running_by_region(_orders(s), _customers(s)),
                    [
                        (1, "Alice", "east", 1, 100, 100),
                        (6, "Carol", "east", 2, 300, 400),
                        (7, "Carol", "east", 3, 50, 450),
                        (8, "Alice", "east", 4, 200, 650),
                        (2, "Bob", "west", 1, 250, 250),
                        (9, "Bob", "west", 3, 100, 350),
                    ],
                    ["order_id", "name", "region", "day", "amount", "running"],
                ),
            ),
            (
                "ex5_top_amount_per_tier",
                lambda s: (
                    ex5_top_amount_per_tier(_orders(s), _customers(s)),
                    [
                        (6, "Carol", "gold", 300),
                        (2, "Bob", "silver", 250),
                    ],
                    ["order_id", "name", "tier", "amount"],
                ),
            ),
            (
                "ex6_high_dense_rank",
                lambda s: (
                    ex6_high_dense_rank(_orders(s), _customers(s)),
                    [
                        (6, "Carol", "east", 300, 1),
                        (8, "Alice", "east", 200, 2),
                        (1, "Alice", "east", 100, 3),
                        (2, "Bob", "west", 250, 1),
                        (5, "Bob", "west", 150, 2),
                        (9, "Bob", "west", 100, 3),
                    ],
                    ["order_id", "name", "region", "amount", "d"],
                ),
            ),
            (
                "ex7_open_delta_by_name",
                lambda s: (
                    ex7_open_delta_by_name(_orders(s), _customers(s)),
                    [
                        (1, "Alice", 1, 100, None, None),
                        (8, "Alice", 4, 200, 100, 100),
                        (2, "Bob", 1, 250, None, None),
                        (9, "Bob", 3, 100, 250, -150),
                        (6, "Carol", 2, 300, None, None),
                        (7, "Carol", 3, 50, 300, -250),
                    ],
                    ["order_id", "name", "day", "amount", "prev", "delta"],
                ),
            ),
            (
                "ex8_top2_open_per_region",
                lambda s: (
                    ex8_top2_open_per_region(_orders(s), _customers(s)),
                    [
                        (6, "Carol", "east", 300, 1),
                        (8, "Alice", "east", 200, 2),
                        (2, "Bob", "west", 250, 1),
                        (9, "Bob", "west", 100, 2),
                    ],
                    ["order_id", "name", "region", "amount", "rn"],
                ),
            ),
        ],
    )

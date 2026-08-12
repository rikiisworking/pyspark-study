"""
Lesson 0012 drills — capstone mix (0001–0011).

No new API. Interleave filter/join/nulls/window/agg + read/write.
Forces skills often skipped: write partitionBy, filter-after-read.
Run:
  ../.venv/bin/python 12_capstone_mix.py

Data (built in checker):

  orders:     order_id INT, cust_id INT, amount INT, status STRING
  customers:  cust_id INT, name STRING, region STRING

  orders:
    (1, 10, 100,  "open")
    (2, 20, None, "open")
    (3, 10, 40,   None)
    (4, 99, 80,   "open")      # orphan
    (5, 20, 150,  "closed")
    (6, 30, 300,  "open")
    (7, 30, None, "closed")
    (8, 10, 200,  "closed")

  customers:
    (10, "Alice", "east")
    (20, "Bob",   None)        # null region
    (30, "Carol", "east")
    (40, "Dan",   "north")     # no orders

Lesson: lessons/0012-capstone-mix.html
Solutions (after real try): exercises/solutions/12_capstone_mix.py
"""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql.functions import col, coalesce, lit, row_number, sum, when  # noqa: F401

# ---------------------------------------------------------------------------
# YOUR CODE — replace `raise NotImplementedError`
# ---------------------------------------------------------------------------


def ex1_fill_join_sum_by_name(
    orders: DataFrame, customers: DataFrame
) -> DataFrame:
    """
    Fill null amounts with 0, inner join customers.
    Per name: total = sum(amount).
    Columns: name, total
    """
   
    return (
        orders
            .fillna({"amount":0})
            .join(customers,"cust_id")
            .groupBy("name")
            .agg(sum("amount").alias("total"))
            .select("name", "total")
    )


def ex2_drop_join_select(
    orders: DataFrame, customers: DataFrame
) -> DataFrame:
    """
    Drop rows with null amount OR null status.
    Inner join customers.
    Columns: order_id, name, amount, status
    """
    return (
        orders
            .dropna(subset=("amount", "status"))
            .join(customers, "cust_id")
            .select("order_id", "name", "amount", "status")
    )


def ex3_write_open_parquet(
    spark: SparkSession, orders: DataFrame, path: str
) -> DataFrame:
    """
    status == "open", write parquet overwrite, read back.
    Columns: order_id, cust_id, amount, status
    """
    
    orders.filter(col("status")=="open").write.mode("overwrite").parquet(path)

    return (
        spark.read.parquet(path)
            .select("order_id", "cust_id", "amount", "status")
    )

def ex4_partition_by_region_east(
    spark: SparkSession, orders: DataFrame, customers: DataFrame, path: str
) -> DataFrame:
    """
    Fill amount nulls with 0, inner join customers.
    Write parquet partitioned by region, mode overwrite.
    Read path, keep region == "east".
    Columns: order_id, name, region, amount  (this order)
    """

    orders.fillna({"amount":0}).join(customers, "cust_id").write.mode("overwrite").partitionBy("region").parquet(path)
    return (
        spark.read.parquet(path)
            .filter(col("region")=="east")
            .select("order_id", "name", "region", "amount")
    )

def ex5_overwrite_open_then_closed(
    spark: SparkSession, orders: DataFrame, path: str
) -> DataFrame:
    """
    Write open rows parquet overwrite.
    Then write closed rows parquet overwrite (same path).
    Read — only closed remain.
    Columns: order_id, cust_id, amount, status
    """

    orders.filter(col("status")=="open").write.mode("overwrite").parquet(path)
    orders.filter(col("status")=="closed").write.mode("overwrite").parquet(path)
    return (
        spark.read.parquet(path)
            .select("order_id", "cust_id", "amount", "status")
    )

def ex6_csv_full_then_filter_open(
    spark: SparkSession, orders: DataFrame, path: str
) -> DataFrame:
    """
    Write FULL orders as CSV with header, overwrite.
    Read header+inferSchema, THEN filter status == "open".
    (Do not filter before write.)
    Columns: order_id, cust_id, amount, status
    """
    orders.write.mode("overwrite").csv(path, header=True)
    return (
        spark.read.csv(path, inferSchema=True, header=True)
            .filter(col("status")=="open")
            .select("order_id", "cust_id", "amount", "status")
    )


def ex7_top_open_per_name(
    orders: DataFrame, customers: DataFrame
) -> DataFrame:
    """
    status == "open" AND amount is not null.
    Inner join customers.
    rn = row_number per name, amount DESC then order_id ASC.
    Keep rn == 1.
    Columns: order_id, name, amount
    """
    w = Window.partitionBy("name").orderBy(col("amount").desc(), col("order_id").asc())
    return (
        orders
            .filter((col("status")=="open") & (col("amount").isNotNull()))
            .join(customers, "cust_id")
            .withColumn("rn", row_number().over(w))
            .filter(col("rn") == 1)
            .select("order_id", "name", "amount")
    )


def ex8_open_fill_sum_by_region(
    orders: DataFrame, customers: DataFrame
) -> DataFrame:
    """
    Fill amount nulls with 0, keep status == "open", inner join.
    region = coalesce(region, "unknown").
    Per region: total = sum(amount).
    Columns: region, total
    """
    return (
        orders
            .fillna({"amount":0})
            .filter(col("status")=="open")
            .join(customers, "cust_id")
            .withColumn("region", coalesce("region", lit("unknown")))
            .groupBy("region")
            .agg(
                sum("amount").alias("total")
            )
            .select("region", "total")
    )


# ---------------------------------------------------------------------------
# Checker — do not edit below
# ---------------------------------------------------------------------------

import shutil
import tempfile

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


def _run_path(spark, fn, expect, columns, with_customers=False):
    d = tempfile.mkdtemp(prefix="pyspark-cap-")
    try:
        if with_customers:
            got = fn(spark, _orders(spark), _customers(spark), d)
        else:
            got = fn(spark, _orders(spark), d)
        rows = got.collect()
        materialized = spark.createDataFrame(rows, got.schema)
        return materialized, expect, columns
    finally:
        shutil.rmtree(d, ignore_errors=True)


if __name__ == "__main__":
    from harness import run_suite

    run_suite(
        "12 capstone mix (0001–0011)",
        [
            (
                "ex1_fill_join_sum_by_name",
                lambda s: (
                    ex1_fill_join_sum_by_name(_orders(s), _customers(s)),
                    [
                        ("Alice", 340),
                        ("Bob", 150),
                        ("Carol", 300),
                    ],
                    ["name", "total"],
                ),
            ),
            (
                "ex2_drop_join_select",
                lambda s: (
                    ex2_drop_join_select(_orders(s), _customers(s)),
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
                "ex3_write_open_parquet",
                lambda s: _run_path(
                    s,
                    ex3_write_open_parquet,
                    [
                        (1, 10, 100, "open"),
                        (2, 20, None, "open"),
                        (4, 99, 80, "open"),
                        (6, 30, 300, "open"),
                    ],
                    ["order_id", "cust_id", "amount", "status"],
                ),
            ),
            (
                "ex4_partition_by_region_east",
                lambda s: _run_path(
                    s,
                    ex4_partition_by_region_east,
                    [
                        (1, "Alice", "east", 100),
                        (3, "Alice", "east", 40),
                        (6, "Carol", "east", 300),
                        (7, "Carol", "east", 0),
                        (8, "Alice", "east", 200),
                    ],
                    ["order_id", "name", "region", "amount"],
                    with_customers=True,
                ),
            ),
            (
                "ex5_overwrite_open_then_closed",
                lambda s: _run_path(
                    s,
                    ex5_overwrite_open_then_closed,
                    [
                        (5, 20, 150, "closed"),
                        (7, 30, None, "closed"),
                        (8, 10, 200, "closed"),
                    ],
                    ["order_id", "cust_id", "amount", "status"],
                ),
            ),
            (
                "ex6_csv_full_then_filter_open",
                lambda s: _run_path(
                    s,
                    ex6_csv_full_then_filter_open,
                    [
                        (1, 10, 100, "open"),
                        (2, 20, None, "open"),
                        (4, 99, 80, "open"),
                        (6, 30, 300, "open"),
                    ],
                    ["order_id", "cust_id", "amount", "status"],
                ),
            ),
            (
                "ex7_top_open_per_name",
                lambda s: (
                    ex7_top_open_per_name(_orders(s), _customers(s)),
                    [
                        (1, "Alice", 100),
                        (6, "Carol", 300),
                    ],
                    ["order_id", "name", "amount"],
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

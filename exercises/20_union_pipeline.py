"""
Lesson 0022 drills — union + pipeline mix (0001–0021).

No new API. Interleave unionByName / allowMissingColumns / distinct / src-tag
with filter, join, fill, groupBy, window, parquet.
Run:
  ../.venv/bin/python 20_union_pipeline.py

Data (built in checker):

  east: order_id INT, cust_id INT, amount INT, status STRING
    (1, 10, 100,  "open")
    (2, 20,  50,  "open")
    (3, 10, 200,  "closed")
    (4, 30,  80,  "open")
    (8, 20, None, "open")     # null amount

  west_shuf: amount INT, order_id INT, cust_id INT, status STRING
    (90,  5, 20, "open")
    (150, 6, 30, "closed")
    (80,  4, 30, "open")      # twin of east 4

  west_extra: order_id INT, cust_id INT, amount INT, status STRING, region STRING
    (7, 99, 90, "open", "north")   # orphan cust

  customers: cust_id INT, name STRING, region STRING
    (10, "Alice", "east")
    (20, "Bob",   None)
    (30, "Carol", "east")
    (40, "Dan",   "north")     # no orders

Lesson: lessons/0022-union-pipeline-mix.html
Solutions (after real try): exercises/solutions/20_union_pipeline.py
"""

from __future__ import annotations

import shutil
import tempfile

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql.functions import (  # noqa: F401 — use what you need
    coalesce,
    col,
    lit,
    row_number,
    sum,
)

# ---------------------------------------------------------------------------
# YOUR CODE — replace `raise NotImplementedError`
# ---------------------------------------------------------------------------


def ex1_stack_then_open(east: DataFrame, west_shuf: DataFrame) -> DataFrame:
    """
    unionByName east + west_shuf. Keep status == "open".
    Columns: order_id, amount, status
    """
    return (
        east.unionByName(west_shuf)
            .filter(col("status")=="open")
            .select("order_id", "amount", "status")
    )
    

def ex2_stack_inner_name(
    east: DataFrame, west_shuf: DataFrame, customers: DataFrame
) -> DataFrame:
    """
    unionByName east + west_shuf. Inner join customers on cust_id.
    Columns: order_id, name, amount
    """
    return (
        east.unionByName(west_shuf)
            .join(customers, "cust_id")
            .select("order_id", "name", "amount")
    )

def ex3_allow_missing_left_region(
    east: DataFrame, west_extra: DataFrame, customers: DataFrame
) -> DataFrame:
    """
    unionByName east + west_extra, allowMissingColumns=True.
    Left join customers. region = coalesce(stack region, cust region, "unknown").
    Columns: order_id, name, region
    """

    dim = customers.withColumnRenamed("region", "cust_region")

    return (
        east.unionByName(west_extra,allowMissingColumns=True)
            .join(dim, "cust_id", "left")
            .withColumn("region", coalesce(col("region"), col("cust_region"), lit("unknown")))
            .select("order_id", "name", "region")
    )


def ex4_tag_then_west(east: DataFrame, west_shuf: DataFrame) -> DataFrame:
    """
    Tag east src="east", west_shuf src="west", unionByName.
    Keep src == "west".
    Columns: order_id, src, amount
    """
    return (
        east.withColumn("src", lit("east"))
            .unionByName(
                west_shuf.withColumn("src", lit("west"))
            )
            .filter(col("src") == "west")
            .select("order_id", "src", "amount")
    )


def ex5_fill_sum_by_status(east: DataFrame, west_shuf: DataFrame) -> DataFrame:
    """
    unionByName east + west_shuf. Fill amount 0. Sum by status.
    Columns: status, total
    """
    return (
        east.unionByName(west_shuf)
            .fillna({"amount":0})
            .groupBy("status")
            .agg(
                sum("amount").alias("total")
            )
            .select("status", "total")
    )


def ex6_window_top_per_name(
    east: DataFrame, west_shuf: DataFrame, customers: DataFrame
) -> DataFrame:
    """
    unionByName east + west_shuf. Drop null amount. Inner join name.
    Window: top amount per name (ties → smaller order_id).
    Columns: order_id, name, amount
    """
    w = Window.partitionBy("name").orderBy(col("amount").desc(), "order_id")
    return (
        east.unionByName(west_shuf)
            .dropna(subset="amount")
            .join(customers, "cust_id")
            .withColumn("rn", row_number().over(w))
            .filter(col("rn") == 1)
            .select("order_id", "name", "amount")
    )

def ex7_stack_parquet(
    spark: SparkSession, east: DataFrame, west_shuf: DataFrame, path: str
) -> DataFrame:
    """
    unionByName east + west_shuf. Select order_id, amount, status.
    Write overwrite parquet to path, read back.
    Columns: order_id, amount, status
    """
    (
        east.unionByName(west_shuf)
            .select("order_id", "amount", "status")
            .write.mode("overwrite").parquet(path)
    )

    return spark.read.parquet(path)



def ex8_distinct_then_open_join(
    east: DataFrame, west_shuf: DataFrame, customers: DataFrame
) -> DataFrame:
    """
    unionByName east + west_shuf. distinct. Keep status == "open".
    Inner join customers on cust_id.
    Columns: order_id, name, amount
    """
    return (
        east.unionByName(west_shuf)
            .distinct()
            .filter(col("status")=="open")
            .join(customers, "cust_id")
            .select("order_id", "name", "amount")
    )



# ---------------------------------------------------------------------------
# Checker — do not edit below
# ---------------------------------------------------------------------------

_EAST = [
    (1, 10, 100, "open"),
    (2, 20, 50, "open"),
    (3, 10, 200, "closed"),
    (4, 30, 80, "open"),
    (8, 20, None, "open"),
]
_EAST_SCHEMA = "order_id INT, cust_id INT, amount INT, status STRING"

_WEST_SHUF = [
    (90, 5, 20, "open"),
    (150, 6, 30, "closed"),
    (80, 4, 30, "open"),
]
_WEST_SHUF_SCHEMA = "amount INT, order_id INT, cust_id INT, status STRING"

_WEST_EXTRA = [
    (7, 99, 90, "open", "north"),
]
_WEST_EXTRA_SCHEMA = (
    "order_id INT, cust_id INT, amount INT, status STRING, region STRING"
)

_CUSTOMERS = [
    (10, "Alice", "east"),
    (20, "Bob", None),
    (30, "Carol", "east"),
    (40, "Dan", "north"),
]
_CUSTOMERS_SCHEMA = "cust_id INT, name STRING, region STRING"


def _east(spark) -> DataFrame:
    return spark.createDataFrame(_EAST, _EAST_SCHEMA)


def _west_shuf(spark) -> DataFrame:
    return spark.createDataFrame(_WEST_SHUF, _WEST_SHUF_SCHEMA)


def _west_extra(spark) -> DataFrame:
    return spark.createDataFrame(_WEST_EXTRA, _WEST_EXTRA_SCHEMA)


def _customers(spark) -> DataFrame:
    return spark.createDataFrame(_CUSTOMERS, _CUSTOMERS_SCHEMA)


def _run_path(spark: SparkSession, fn, expect, columns):
    d = tempfile.mkdtemp(prefix="pyspark-un-mix-")
    try:
        got = fn(spark, _east(spark), _west_shuf(spark), d)
        rows = got.collect()
        materialized = spark.createDataFrame(rows, got.schema)
        return materialized, expect, columns
    finally:
        shutil.rmtree(d, ignore_errors=True)


if __name__ == "__main__":
    from harness import run_suite

    run_suite(
        "20 union + pipeline mix",
        [
            (
                "ex1_stack_then_open",
                lambda s: (
                    ex1_stack_then_open(_east(s), _west_shuf(s)),
                    [
                        (1, 100, "open"),
                        (2, 50, "open"),
                        (4, 80, "open"),
                        (8, None, "open"),
                        (5, 90, "open"),
                        (4, 80, "open"),
                    ],
                    ["order_id", "amount", "status"],
                ),
            ),
            (
                "ex2_stack_inner_name",
                lambda s: (
                    ex2_stack_inner_name(_east(s), _west_shuf(s), _customers(s)),
                    [
                        (1, "Alice", 100),
                        (2, "Bob", 50),
                        (3, "Alice", 200),
                        (4, "Carol", 80),
                        (8, "Bob", None),
                        (5, "Bob", 90),
                        (6, "Carol", 150),
                        (4, "Carol", 80),
                    ],
                    ["order_id", "name", "amount"],
                ),
            ),
            (
                "ex3_allow_missing_left_region",
                lambda s: (
                    ex3_allow_missing_left_region(
                        _east(s), _west_extra(s), _customers(s)
                    ),
                    [
                        (1, "Alice", "east"),
                        (2, "Bob", "unknown"),
                        (3, "Alice", "east"),
                        (4, "Carol", "east"),
                        (8, "Bob", "unknown"),
                        (7, None, "north"),
                    ],
                    ["order_id", "name", "region"],
                ),
            ),
            (
                "ex4_tag_then_west",
                lambda s: (
                    ex4_tag_then_west(_east(s), _west_shuf(s)),
                    [
                        (5, "west", 90),
                        (6, "west", 150),
                        (4, "west", 80),
                    ],
                    ["order_id", "src", "amount"],
                ),
            ),
            (
                "ex5_fill_sum_by_status",
                lambda s: (
                    ex5_fill_sum_by_status(_east(s), _west_shuf(s)),
                    [
                        ("closed", 350),
                        ("open", 400),
                    ],
                    ["status", "total"],
                ),
            ),
            (
                "ex6_window_top_per_name",
                lambda s: (
                    ex6_window_top_per_name(
                        _east(s), _west_shuf(s), _customers(s)
                    ),
                    [
                        (3, "Alice", 200),
                        (5, "Bob", 90),
                        (6, "Carol", 150),
                    ],
                    ["order_id", "name", "amount"],
                ),
            ),
            (
                "ex7_stack_parquet",
                lambda s: _run_path(
                    s,
                    ex7_stack_parquet,
                    [
                        (1, 100, "open"),
                        (2, 50, "open"),
                        (3, 200, "closed"),
                        (4, 80, "open"),
                        (8, None, "open"),
                        (5, 90, "open"),
                        (6, 150, "closed"),
                        (4, 80, "open"),
                    ],
                    ["order_id", "amount", "status"],
                ),
            ),
            (
                "ex8_distinct_then_open_join",
                lambda s: (
                    ex8_distinct_then_open_join(
                        _east(s), _west_shuf(s), _customers(s)
                    ),
                    [
                        (1, "Alice", 100),
                        (2, "Bob", 50),
                        (4, "Carol", 80),
                        (8, "Bob", None),
                        (5, "Bob", 90),
                    ],
                    ["order_id", "name", "amount"],
                ),
            ),
        ],
    )

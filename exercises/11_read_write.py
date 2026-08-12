"""
Lesson 0011 drills — read / write (parquet + csv).

Fill each function. Paths are temp dirs the checker creates.
Run from this directory:
  ../.venv/bin/python 11_read_write.py

Data (built in checker):

  orders: order_id INT, region STRING, amount INT, status STRING
  rows:
    (1, "east", 100, "open")
    (2, "west",  50, "open")
    (3, "east", 200, "closed")
    (4, "west", 150, "open")

Lesson: lessons/0011-read-write.html
Solutions (after real try): exercises/solutions/11_read_write.py
"""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col  # noqa: F401

import time
# ---------------------------------------------------------------------------
# YOUR CODE — replace `raise NotImplementedError`
# ---------------------------------------------------------------------------


def ex1_parquet_roundtrip(
    spark: SparkSession, orders: DataFrame, path: str
) -> DataFrame:
    """
    Write orders as parquet, mode overwrite.
    Read the path back. Return all columns.
    Columns: order_id, region, amount, status
    """
    orders.write.mode("overwrite").parquet(path)
    return spark.read.parquet(path)


def ex2_csv_header_roundtrip(
    spark: SparkSession, orders: DataFrame, path: str
) -> DataFrame:
    """
    Write orders as CSV with header, mode overwrite.
    Read with header=true and inferSchema=true.
    Columns: order_id, region, amount, status
    """
    orders.write.mode("overwrite").csv(path,header=True)
    return spark.read.csv(path, header=True, inferSchema=True).select("order_id", "region", "amount", "status")


def ex3_write_open_parquet(
    spark: SparkSession, orders: DataFrame, path: str
) -> DataFrame:
    """
    Keep status == "open", write parquet overwrite, read back.
    Columns: order_id, region, amount, status
    """
    orders.filter(col("status")=="open").write.mode("overwrite").parquet(path)
    return spark.read.parquet(path)


def ex4_partition_by_region(
    spark: SparkSession, orders: DataFrame, path: str
) -> DataFrame:
    """
    Write parquet partitioned by region, mode overwrite.
    Read path, keep region == "east".
    Columns: order_id, region, amount, status  (select in this order)
    """
    orders.write.mode("overwrite").partitionBy("region").parquet(path)
    return (
        spark.read.parquet(path)
            .filter(col("region")=="east")
            .select("order_id", "region", "amount", "status")
    )


def ex5_append_doubles(spark: SparkSession, orders: DataFrame, path: str) -> DataFrame:
    """
    Write orders parquet overwrite, then write same orders append.
    Read path. Expect every row twice.
    Columns: order_id, region, amount, status
    """
    orders.select("order_id", "region", "amount", "status").write.mode("overwrite").parquet(path)
    orders.select("order_id", "region", "amount", "status").write.mode("append").parquet(path)
    return spark.read.parquet(path)

def ex6_overwrite_replaces(
    spark: SparkSession, orders: DataFrame, path: str
) -> DataFrame:
    """
    Write open rows parquet overwrite.
    Then write closed rows parquet overwrite (same path).
    Read path — only closed rows remain.
    Columns: order_id, region, amount, status
    """
    orders.filter(col("status")=="open").select("order_id", "region", "amount", "status").write.mode("overwrite").parquet(path)
    orders.filter(col("status")=="closed").select("order_id", "region", "amount", "status").write.mode("overwrite").parquet(path)
    return spark.read.parquet(path)


def ex7_filter_write_select(
    spark: SparkSession, orders: DataFrame, path: str
) -> DataFrame:
    """
    amount >= 100, write parquet overwrite, read back.
    Columns: order_id, amount
    """
    orders.write.mode("overwrite").parquet(path)
    return spark.read.parquet(path).filter(col("amount")>=100).select("order_id", "amount")


def ex8_csv_then_filter_open(
    spark: SparkSession, orders: DataFrame, path: str
) -> DataFrame:
    """
    Write full orders CSV with header, overwrite.
    Read header+inferSchema, k,eep status == "open".
    Columns: order_id, region, amount, status
    """
    orders.filter(col("status")=="open").write.mode("overwrite").csv(path, header=True)
    return spark.read.csv(path, inferSchema=True, header=True)


# ---------------------------------------------------------------------------
# Checker — do not edit below
# ---------------------------------------------------------------------------

import shutil
import tempfile

_ORDER_ROWS = [
    (1, "east", 100, "open"),
    (2, "west", 50, "open"),
    (3, "east", 200, "closed"),
    (4, "west", 150, "open"),
]
_ORDER_SCHEMA = "order_id INT, region STRING, amount INT, status STRING"
_COLS = ["order_id", "region", "amount", "status"]


def _orders(spark):
    return spark.createDataFrame(_ORDER_ROWS, _ORDER_SCHEMA)


def _run(spark, fn, expect, columns):
    """Write/read under a temp path, then materialize so cleanup is safe.

    spark.read is lazy — deleting the path before collect breaks the DF.
    """
    d = tempfile.mkdtemp(prefix="pyspark-rw-")
    try:
        got = fn(spark, _orders(spark), d)
        # Force action while files still exist; rebuild small in-memory DF.
        rows = got.collect()
        materialized = spark.createDataFrame(rows, got.schema)
        return materialized, expect, columns
    finally:
        shutil.rmtree(d, ignore_errors=True)


if __name__ == "__main__":
    from harness import run_suite

    run_suite(
        "11 read / write (parquet + csv)",
        [
            (
                "ex1_parquet_roundtrip",
                lambda s: _run(
                    s,
                    ex1_parquet_roundtrip,
                    list(_ORDER_ROWS),
                    _COLS,
                ),
            ),
            (
                "ex2_csv_header_roundtrip",
                lambda s: _run(
                    s,
                    ex2_csv_header_roundtrip,
                    list(_ORDER_ROWS),
                    _COLS,
                ),
            ),
            (
                "ex3_write_open_parquet",
                lambda s: _run(
                    s,
                    ex3_write_open_parquet,
                    [
                        (1, "east", 100, "open"),
                        (2, "west", 50, "open"),
                        (4, "west", 150, "open"),
                    ],
                    _COLS,
                ),
            ),
            (
                "ex4_partition_by_region",
                lambda s: _run(
                    s,
                    ex4_partition_by_region,
                    [
                        (1, "east", 100, "open"),
                        (3, "east", 200, "closed"),
                    ],
                    _COLS,
                ),
            ),
            (
                "ex5_append_doubles",
                lambda s: _run(
                    s,
                    ex5_append_doubles,
                    list(_ORDER_ROWS) + list(_ORDER_ROWS),
                    _COLS,
                ),
            ),
            (
                "ex6_overwrite_replaces",
                lambda s: _run(
                    s,
                    ex6_overwrite_replaces,
                    [
                        (3, "east", 200, "closed"),
                    ],
                    _COLS,
                ),
            ),
            (
                "ex7_filter_write_select",
                lambda s: _run(
                    s,
                    ex7_filter_write_select,
                    [
                        (1, 100),
                        (3, 200),
                        (4, 150),
                    ],
                    ["order_id", "amount"],
                ),
            ),
            (
                "ex8_csv_then_filter_open",
                lambda s: _run(
                    s,
                    ex8_csv_then_filter_open,
                    [
                        (1, "east", 100, "open"),
                        (2, "west", 50, "open"),
                        (4, "west", 150, "open"),
                    ],
                    _COLS,
                ),
            ),
        ],
    )

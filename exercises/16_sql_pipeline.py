"""
Lesson 0016 drills — SQL interop + pipeline mix (0001–0015).

No new API. Interleave createOrReplaceTempView / spark.sql / selectExpr
with filter, join, fill, groupBy, window, parquet.
Run:
  ../.venv/bin/python 16_sql_pipeline.py

Data (built in checker):

  orders: order_id INT, cust_id INT, amount INT, status STRING
    (1, 10, 100,  "open")
    (2, 20,  50,  "open")
    (3, 10, 200,  "closed")
    (4, 30, 150,  "open")
    (5, 99,  80,  "open")   # orphan
    (6, 20, None, "open")   # null amount

  customers: cust_id INT, name STRING, region STRING
    (10, "Alice", "east")
    (20, "Bob",   None)     # null region
    (30, "Carol", "east")
    (40, "Dan",   "north")  # no orders

Lesson: lessons/0016-sql-pipeline-mix.html
Solutions (after real try): exercises/solutions/16_sql_pipeline.py
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


def ex1_sql_open_select_expr(
    spark: SparkSession, orders: DataFrame
) -> DataFrame:
    """
    Register orders. SQL: open → order_id, amount.
    Then selectExpr amount * 2 AS double_amt.
    Columns: order_id, amount, double_amt
    """
    orders.createOrReplaceTempView("orders")
    return spark.sql("SELECT order_id, amount FROM orders WHERE status = 'open'").selectExpr("order_id", "amount", "amount * 2 AS double_amt")

def ex2_sql_join_then_df_east(
    spark: SparkSession, orders: DataFrame, customers: DataFrame
) -> DataFrame:
    """
    Register both. SQL inner join all orders × customers.
    Then DF: keep region == "east".
    Columns: order_id, name, amount, region
    """
    orders.createOrReplaceTempView("orders")
    customers.createOrReplaceTempView("customers")

    return (
        spark
            .sql("""
            SELECT order_id, name, amount, region
            FROM orders
            JOIN customers ON orders.cust_id = customers.cust_id""")
            .filter(col("region")=="east")
    )

def ex3_sql_sum_then_filter_df(
    spark: SparkSession, orders: DataFrame
) -> DataFrame:
    """
    Register orders. SQL: status, total = SUM(amount) group by status.
    Then DF: total > 250.
    Columns: status, total
    """

    orders.createOrReplaceTempView("orders")
    return (
        spark
            .sql("""
            SELECT status, SUM(amount) AS total
            FROM orders
            GROUP BY status
            """)
            .filter(col("total") > 250)
    )


def ex4_df_open_sql_inner_name(
    spark: SparkSession, orders: DataFrame, customers: DataFrame
) -> DataFrame:
    """
    DF: filter status == "open" → createOrReplaceTempView("open_orders").
    Register customers. SQL inner join on cust_id.
    Columns: order_id, name, amount
    """

    orders.filter(col("status")=="open").createOrReplaceTempView("open_orders")
    customers.createOrReplaceTempView("customers")
    return (
        spark
            .sql("""
            SELECT order_id, name, amount
            FROM open_orders
            JOIN customers ON open_orders.cust_id = customers.cust_id
            """)
    )


def ex5_sql_left_coalesce_region(
    spark: SparkSession, orders: DataFrame, customers: DataFrame
) -> DataFrame:
    """
    Register both. SQL: open orders LEFT JOIN customers.
    Then DF: region = coalesce(region, "unknown").
    Columns: order_id, name, region
    """
    orders.createOrReplaceTempView("orders")
    customers.createOrReplaceTempView("customers")
    return (
        spark
            .sql("""
            SELECT order_id, name, region
            FROM orders
            LEFT JOIN customers ON orders.cust_id = customers.cust_id
            WHERE status = 'open'
            """)
            .withColumn("region", coalesce(col("region"), lit("unknown")))
    )

def ex6_sql_join_window_top(
    spark: SparkSession, orders: DataFrame, customers: DataFrame
) -> DataFrame:
    """
    Register both. SQL: open + amount IS NOT NULL, inner join name.
    Then DF window: top amount per name (ties → smaller order_id).
    Columns: order_id, name, amount
    """
    orders.createOrReplaceTempView("orders")
    customers.createOrReplaceTempView("customers")

    w = Window.partitionBy("name").orderBy(col("amount").desc(), "order_id")
    return (
        spark
            .sql("""
            SELECT order_id, name, amount
            FROM orders
            INNER JOIN customers ON customers.cust_id = orders.cust_id
            WHERE status = 'open' AND amount IS NOT NULL
            """)
            .withColumn("rn", row_number().over(w))
            .filter(col("rn") == 1)
            .select("order_id", "name", "amount")       
    )


def ex7_sql_fill_sum_by_region(
    spark: SparkSession, orders: DataFrame, customers: DataFrame
) -> DataFrame:
    """
    Register both. SQL: open + inner join → region, amount.
    Then DF: fill amount 0, coalesce region "unknown", sum by region.
    Columns: region, total
    """
    orders.createOrReplaceTempView("orders")
    customers.createOrReplaceTempView("customers")

    return (
        spark
            .sql("""
            SELECT region, amount
            FROM orders 
            INNER JOIN customers ON orders.cust_id = customers.cust_id
            WHERE status = 'open'
            """)
            .fillna({"amount":0})
            .withColumn("region", coalesce(col("region"), lit("unknown")))
            .groupBy("region")
            .agg(
                sum(col("amount")).alias("total")
            )
            .select("region", "total")
    )


def ex8_sql_open_parquet(
    spark: SparkSession, orders: DataFrame, path: str
) -> DataFrame:
    """
    Register orders. SQL: open → order_id, cust_id, amount.
    Write overwrite parquet to path, read back.
    Columns: order_id, cust_id, amount
    """
    orders.createOrReplaceTempView("orders")
    spark.sql("""
        SELECT order_id, cust_id, amount
        FROM orders
        WHERE status = 'open'
    """).write.mode("overwrite").parquet(path)

    return spark.read.parquet(path)


# ---------------------------------------------------------------------------
# Checker — do not edit below
# ---------------------------------------------------------------------------

_ORDER_ROWS = [
    (1, 10, 100, "open"),
    (2, 20, 50, "open"),
    (3, 10, 200, "closed"),
    (4, 30, 150, "open"),
    (5, 99, 80, "open"),
    (6, 20, None, "open"),
]
_ORDER_SCHEMA = "order_id INT, cust_id INT, amount INT, status STRING"

_CUSTOMER_ROWS = [
    (10, "Alice", "east"),
    (20, "Bob", None),
    (30, "Carol", "east"),
    (40, "Dan", "north"),
]
_CUSTOMER_SCHEMA = "cust_id INT, name STRING, region STRING"


def _orders(spark: SparkSession) -> DataFrame:
    return spark.createDataFrame(_ORDER_ROWS, _ORDER_SCHEMA)


def _customers(spark: SparkSession) -> DataFrame:
    return spark.createDataFrame(_CUSTOMER_ROWS, _CUSTOMER_SCHEMA)


def _run_path(spark: SparkSession, fn, expect, columns):
    d = tempfile.mkdtemp(prefix="pyspark-sql-mix-")
    try:
        got = fn(spark, _orders(spark), d)
        rows = got.collect()
        materialized = spark.createDataFrame(rows, got.schema)
        return materialized, expect, columns
    finally:
        shutil.rmtree(d, ignore_errors=True)


if __name__ == "__main__":
    from harness import run_suite

    run_suite(
        "16 SQL + pipeline mix",
        [
            (
                "ex1_sql_open_select_expr",
                lambda s: (
                    ex1_sql_open_select_expr(s, _orders(s)),
                    [
                        (1, 100, 200),
                        (2, 50, 100),
                        (4, 150, 300),
                        (5, 80, 160),
                        (6, None, None),
                    ],
                    ["order_id", "amount", "double_amt"],
                ),
            ),
            (
                "ex2_sql_join_then_df_east",
                lambda s: (
                    ex2_sql_join_then_df_east(s, _orders(s), _customers(s)),
                    [
                        (1, "Alice", 100, "east"),
                        (3, "Alice", 200, "east"),
                        (4, "Carol", 150, "east"),
                    ],
                    ["order_id", "name", "amount", "region"],
                ),
            ),
            (
                "ex3_sql_sum_then_filter_df",
                lambda s: (
                    ex3_sql_sum_then_filter_df(s, _orders(s)),
                    [
                        ("open", 380),
                    ],
                    ["status", "total"],
                ),
            ),
            (
                "ex4_df_open_sql_inner_name",
                lambda s: (
                    ex4_df_open_sql_inner_name(s, _orders(s), _customers(s)),
                    [
                        (1, "Alice", 100),
                        (2, "Bob", 50),
                        (4, "Carol", 150),
                        (6, "Bob", None),
                    ],
                    ["order_id", "name", "amount"],
                ),
            ),
            (
                "ex5_sql_left_coalesce_region",
                lambda s: (
                    ex5_sql_left_coalesce_region(s, _orders(s), _customers(s)),
                    [
                        (1, "Alice", "east"),
                        (2, "Bob", "unknown"),
                        (4, "Carol", "east"),
                        (5, None, "unknown"),
                        (6, "Bob", "unknown"),
                    ],
                    ["order_id", "name", "region"],
                ),
            ),
            (
                "ex6_sql_join_window_top",
                lambda s: (
                    ex6_sql_join_window_top(s, _orders(s), _customers(s)),
                    [
                        (1, "Alice", 100),
                        (2, "Bob", 50),
                        (4, "Carol", 150),
                    ],
                    ["order_id", "name", "amount"],
                ),
            ),
            (
                "ex7_sql_fill_sum_by_region",
                lambda s: (
                    ex7_sql_fill_sum_by_region(s, _orders(s), _customers(s)),
                    [
                        ("east", 250),
                        ("unknown", 50),
                    ],
                    ["region", "total"],
                ),
            ),
            (
                "ex8_sql_open_parquet",
                lambda s: _run_path(
                    s,
                    ex8_sql_open_parquet,
                    [
                        (1, 10, 100),
                        (2, 20, 50),
                        (4, 30, 150),
                        (5, 99, 80),
                        (6, 20, None),
                    ],
                    ["order_id", "cust_id", "amount"],
                ),
            ),
        ],
    )

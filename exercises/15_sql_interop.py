"""
Lesson 0015 drills — Spark SQL interop (temp views).

No cluster theory. Cold-write: createOrReplaceTempView, spark.sql,
selectExpr, global_temp.
Run:
  ../.venv/bin/python 15_sql_interop.py

Data (built in checker):

  orders: order_id INT, cust_id INT, amount INT, status STRING
    (1, 10, 100, "open")
    (2, 20,  50, "open")
    (3, 10, 200, "closed")
    (4, 30, 150, "open")

  customers: cust_id INT, name STRING, region STRING
    (10, "Alice", "east")
    (20, "Bob",   "west")
    (30, "Carol", "east")

Lesson: lessons/0015-sql-interop.html
Solutions (after real try): exercises/solutions/15_sql_interop.py
"""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col  # noqa: F401 — use what you need

# ---------------------------------------------------------------------------
# YOUR CODE — replace `raise NotImplementedError`
# ---------------------------------------------------------------------------


def ex1_sql_filter_open(
    spark: SparkSession, orders: DataFrame
) -> DataFrame:
    """
    Register orders as temp view "orders".
    SQL: keep status = 'open'.
    Columns: order_id, amount, status
    """
    orders.createOrReplaceTempView("orders")
    return spark.sql("""
        SELECT order_id, amount, status
        FROM orders
        WHERE status = 'open'
    """)

def ex2_sql_join_name(
    spark: SparkSession, orders: DataFrame, customers: DataFrame
) -> DataFrame:
    """
    Register both as "orders" and "customers".
    SQL inner join on cust_id.
    Columns: order_id, name, amount
    """
    orders.createOrReplaceTempView("orders")
    customers.createOrReplaceTempView("customers")

    return spark.sql("""
        SELECT o.order_id, c.name, o.amount
        FROM orders o
        JOIN customers c ON o.cust_id = c.cust_id
    """)


def ex3_sql_sum_by_status(
    spark: SparkSession, orders: DataFrame
) -> DataFrame:
    """
    Register orders. SQL group by status, total = SUM(amount).
    Columns: status, total
    """

    orders.createOrReplaceTempView("orders")
    return spark.sql("""
        SELECT status, SUM(amount) as total
        FROM orders
        GROUP BY status
    """)

def ex4_select_expr_double(orders: DataFrame) -> DataFrame:
    """
    No view. selectExpr order_id and amount * 2 as double_amt.
    Columns: order_id, double_amt
    """
    return (
        orders.selectExpr("order_id", "amount * 2 AS double_amt")
    )


def ex5_sql_then_tax(
    spark: SparkSession, orders: DataFrame
) -> DataFrame:
    """
    Register orders. SQL: open rows order_id, amount.
    Then DF: withColumn tax = amount * 0.1.
    Columns: order_id, amount, tax
    """

    orders.createOrReplaceTempView("orders")
    return spark.sql("""
        SELECT order_id, amount
        FROM orders
        WHERE status = 'open'
    """).withColumn("tax", col("amount")*0.1)


def ex6_replace_view(
    spark: SparkSession, orders: DataFrame
) -> DataFrame:
    """
    First: open-only → createOrReplaceTempView("v").
    Then: full orders → createOrReplaceTempView("v") again.
    SQL: SELECT order_id, status FROM v
    Expect all 4 rows (replace wins).
    Columns: order_id, status
    """
    orders.filter(col("status") == "open").createOrReplaceTempView("v")
    orders.createOrReplaceTempView("v")
    return spark.sql("""
        SELECT order_id, status FROM v
    """)

def ex7_global_temp_filter(
    spark: SparkSession, orders: DataFrame
) -> DataFrame:
    """
    createOrReplaceGlobalTempView("orders_g").
    SQL from global_temp.orders_g where amount >= 100.
    Columns: order_id, amount
    """
    orders.createOrReplaceGlobalTempView("orders_g")
    return spark.sql("""
        SELECT order_id, amount
        FROM global_temp.orders_g
        WHERE amount >= 100
    """)

def ex8_sql_open_east(
    spark: SparkSession, orders: DataFrame, customers: DataFrame
) -> DataFrame:
    """
    Register both. SQL: open orders in east region.
    Columns: order_id, name, amount, region
    """

    orders.createOrReplaceTempView("orders")
    customers.createOrReplaceTempView("customers")
    return spark.sql("""
        SELECT o.order_id, c.name, o.amount, c.region
        FROM orders o
        JOIN customers c ON o.cust_id = c.cust_id
        WHERE o.status = 'open' AND c.region = 'east'
    """)


# ---------------------------------------------------------------------------
# Checker — do not edit below
# ---------------------------------------------------------------------------

_ORDER_ROWS = [
    (1, 10, 100, "open"),
    (2, 20, 50, "open"),
    (3, 10, 200, "closed"),
    (4, 30, 150, "open"),
]
_ORDER_SCHEMA = "order_id INT, cust_id INT, amount INT, status STRING"

_CUSTOMER_ROWS = [
    (10, "Alice", "east"),
    (20, "Bob", "west"),
    (30, "Carol", "east"),
]
_CUSTOMER_SCHEMA = "cust_id INT, name STRING, region STRING"


def _orders(spark: SparkSession) -> DataFrame:
    return spark.createDataFrame(_ORDER_ROWS, _ORDER_SCHEMA)


def _customers(spark: SparkSession) -> DataFrame:
    return spark.createDataFrame(_CUSTOMER_ROWS, _CUSTOMER_SCHEMA)


if __name__ == "__main__":
    from harness import run_suite

    run_suite(
        "15 SQL interop",
        [
            (
                "ex1_sql_filter_open",
                lambda s: (
                    ex1_sql_filter_open(s, _orders(s)),
                    [
                        (1, 100, "open"),
                        (2, 50, "open"),
                        (4, 150, "open"),
                    ],
                    ["order_id", "amount", "status"],
                ),
            ),
            (
                "ex2_sql_join_name",
                lambda s: (
                    ex2_sql_join_name(s, _orders(s), _customers(s)),
                    [
                        (1, "Alice", 100),
                        (2, "Bob", 50),
                        (3, "Alice", 200),
                        (4, "Carol", 150),
                    ],
                    ["order_id", "name", "amount"],
                ),
            ),
            (
                "ex3_sql_sum_by_status",
                lambda s: (
                    ex3_sql_sum_by_status(s, _orders(s)),
                    [
                        ("closed", 200),
                        ("open", 300),
                    ],
                    ["status", "total"],
                ),
            ),
            (
                "ex4_select_expr_double",
                lambda s: (
                    ex4_select_expr_double(_orders(s)),
                    [
                        (1, 200),
                        (2, 100),
                        (3, 400),
                        (4, 300),
                    ],
                    ["order_id", "double_amt"],
                ),
            ),
            (
                "ex5_sql_then_tax",
                lambda s: (
                    ex5_sql_then_tax(s, _orders(s)),
                    [
                        (1, 100, 10.0),
                        (2, 50, 5.0),
                        (4, 150, 15.0),
                    ],
                    ["order_id", "amount", "tax"],
                ),
            ),
            (
                "ex6_replace_view",
                lambda s: (
                    ex6_replace_view(s, _orders(s)),
                    [
                        (1, "open"),
                        (2, "open"),
                        (3, "closed"),
                        (4, "open"),
                    ],
                    ["order_id", "status"],
                ),
            ),
            (
                "ex7_global_temp_filter",
                lambda s: (
                    ex7_global_temp_filter(s, _orders(s)),
                    [
                        (1, 100),
                        (3, 200),
                        (4, 150),
                    ],
                    ["order_id", "amount"],
                ),
            ),
            (
                "ex8_sql_open_east",
                lambda s: (
                    ex8_sql_open_east(s, _orders(s), _customers(s)),
                    [
                        (1, "Alice", 100, "east"),
                        (4, "Carol", 150, "east"),
                    ],
                    ["order_id", "name", "amount", "region"],
                ),
            ),
        ],
    )

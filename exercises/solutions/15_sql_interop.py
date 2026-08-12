"""Reference solutions for 15_sql_interop.py — peek only after a real try."""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col


def ex1_sql_filter_open(
    spark: SparkSession, orders: DataFrame
) -> DataFrame:
    orders.createOrReplaceTempView("orders")
    return spark.sql(
        """
        SELECT order_id, amount, status
        FROM orders
        WHERE status = 'open'
        """
    )


def ex2_sql_join_name(
    spark: SparkSession, orders: DataFrame, customers: DataFrame
) -> DataFrame:
    orders.createOrReplaceTempView("orders")
    customers.createOrReplaceTempView("customers")
    return spark.sql(
        """
        SELECT o.order_id, c.name, o.amount
        FROM orders o
        INNER JOIN customers c ON o.cust_id = c.cust_id
        """
    )


def ex3_sql_sum_by_status(
    spark: SparkSession, orders: DataFrame
) -> DataFrame:
    orders.createOrReplaceTempView("orders")
    return spark.sql(
        """
        SELECT status, SUM(amount) AS total
        FROM orders
        GROUP BY status
        """
    )


def ex4_select_expr_double(orders: DataFrame) -> DataFrame:
    return orders.selectExpr("order_id", "amount * 2 AS double_amt")


def ex5_sql_then_tax(
    spark: SparkSession, orders: DataFrame
) -> DataFrame:
    orders.createOrReplaceTempView("orders")
    return (
        spark.sql(
            "SELECT order_id, amount FROM orders WHERE status = 'open'"
        )
        .withColumn("tax", col("amount") * 0.1)
        .select("order_id", "amount", "tax")
    )


def ex6_replace_view(
    spark: SparkSession, orders: DataFrame
) -> DataFrame:
    orders.filter(col("status") == "open").createOrReplaceTempView("v")
    orders.createOrReplaceTempView("v")
    return spark.sql("SELECT order_id, status FROM v")


def ex7_global_temp_filter(
    spark: SparkSession, orders: DataFrame
) -> DataFrame:
    orders.createOrReplaceGlobalTempView("orders_g")
    return spark.sql(
        """
        SELECT order_id, amount
        FROM global_temp.orders_g
        WHERE amount >= 100
        """
    )


def ex8_sql_open_east(
    spark: SparkSession, orders: DataFrame, customers: DataFrame
) -> DataFrame:
    orders.createOrReplaceTempView("orders")
    customers.createOrReplaceTempView("customers")
    return spark.sql(
        """
        SELECT o.order_id, c.name, o.amount, c.region
        FROM orders o
        INNER JOIN customers c ON o.cust_id = c.cust_id
        WHERE o.status = 'open' AND c.region = 'east'
        """
    )

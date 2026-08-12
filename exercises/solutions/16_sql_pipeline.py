"""Reference solutions for 16_sql_pipeline.py — peek only after a real try."""

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql.functions import coalesce, col, lit, row_number, sum


def ex1_sql_open_select_expr(
    spark: SparkSession, orders: DataFrame
) -> DataFrame:
    orders.createOrReplaceTempView("orders")
    return spark.sql(
        "SELECT order_id, amount FROM orders WHERE status = 'open'"
    ).selectExpr("order_id", "amount", "amount * 2 AS double_amt")


def ex2_sql_join_then_df_east(
    spark: SparkSession, orders: DataFrame, customers: DataFrame
) -> DataFrame:
    orders.createOrReplaceTempView("orders")
    customers.createOrReplaceTempView("customers")
    return (
        spark.sql(
            """
            SELECT o.order_id, c.name, o.amount, c.region
            FROM orders o
            INNER JOIN customers c ON o.cust_id = c.cust_id
            """
        )
        .filter(col("region") == "east")
        .select("order_id", "name", "amount", "region")
    )


def ex3_sql_sum_then_filter_df(
    spark: SparkSession, orders: DataFrame
) -> DataFrame:
    orders.createOrReplaceTempView("orders")
    return (
        spark.sql(
            """
            SELECT status, SUM(amount) AS total
            FROM orders
            GROUP BY status
            """
        )
        .filter(col("total") > 250)
        .select("status", "total")
    )


def ex4_df_open_sql_inner_name(
    spark: SparkSession, orders: DataFrame, customers: DataFrame
) -> DataFrame:
    orders.filter(col("status") == "open").createOrReplaceTempView(
        "open_orders"
    )
    customers.createOrReplaceTempView("customers")
    return spark.sql(
        """
        SELECT o.order_id, c.name, o.amount
        FROM open_orders o
        INNER JOIN customers c ON o.cust_id = c.cust_id
        """
    )


def ex5_sql_left_coalesce_region(
    spark: SparkSession, orders: DataFrame, customers: DataFrame
) -> DataFrame:
    orders.createOrReplaceTempView("orders")
    customers.createOrReplaceTempView("customers")
    return (
        spark.sql(
            """
            SELECT o.order_id, c.name, c.region
            FROM orders o
            LEFT JOIN customers c ON o.cust_id = c.cust_id
            WHERE o.status = 'open'
            """
        )
        .withColumn("region", coalesce(col("region"), lit("unknown")))
        .select("order_id", "name", "region")
    )


def ex6_sql_join_window_top(
    spark: SparkSession, orders: DataFrame, customers: DataFrame
) -> DataFrame:
    orders.createOrReplaceTempView("orders")
    customers.createOrReplaceTempView("customers")
    joined = spark.sql(
        """
        SELECT o.order_id, c.name, o.amount
        FROM orders o
        INNER JOIN customers c ON o.cust_id = c.cust_id
        WHERE o.status = 'open' AND o.amount IS NOT NULL
        """
    )
    w = Window.partitionBy("name").orderBy(
        col("amount").desc(), col("order_id").asc()
    )
    return (
        joined.withColumn("rn", row_number().over(w))
        .filter(col("rn") == 1)
        .select("order_id", "name", "amount")
    )


def ex7_sql_fill_sum_by_region(
    spark: SparkSession, orders: DataFrame, customers: DataFrame
) -> DataFrame:
    orders.createOrReplaceTempView("orders")
    customers.createOrReplaceTempView("customers")
    return (
        spark.sql(
            """
            SELECT c.region, o.amount
            FROM orders o
            INNER JOIN customers c ON o.cust_id = c.cust_id
            WHERE o.status = 'open'
            """
        )
        .na.fill({"amount": 0})
        .withColumn("region", coalesce(col("region"), lit("unknown")))
        .groupBy("region")
        .agg(sum("amount").alias("total"))
        .select("region", "total")
    )


def ex8_sql_open_parquet(
    spark: SparkSession, orders: DataFrame, path: str
) -> DataFrame:
    orders.createOrReplaceTempView("orders")
    out = spark.sql(
        """
        SELECT order_id, cust_id, amount
        FROM orders
        WHERE status = 'open'
        """
    )
    out.write.mode("overwrite").parquet(path)
    return spark.read.parquet(path)

"""Reference solutions for 04_pipeline_mix.py — peek after a real try."""

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, lit, when


def ex1_open_high_east(
    orders: DataFrame, customers: DataFrame
) -> DataFrame:
    return (
        orders.filter((col("status") == "open") & (col("amount") > 100))
        .join(customers, "cust_id", "inner")
        .filter(col("region") == "east")
        .select("order_id", "name", "amount", "region")
    )


def ex2_enrich_band_and_tax(
    orders: DataFrame, customers: DataFrame
) -> DataFrame:
    return (
        orders.join(customers, "cust_id", "left")
        .withColumn(
            "band", when(col("amount") >= 150, "high").otherwise("low")
        )
        .withColumn("tax", col("amount") * 0.1)
        .drop("status", "tier")
        .select(
            "order_id",
            "cust_id",
            "name",
            "region",
            "amount",
            "band",
            "tax",
        )
    )


def ex3_gold_customers_order_stats_shape(
    orders: DataFrame, customers: DataFrame
) -> DataFrame:
    return (
        customers.filter(col("tier") == "gold")
        .join(orders, "cust_id", "left_semi")
        .withColumn("version", lit("v1"))
        .select("cust_id", "name", "region", "version")
    )


def ex4_orphan_orders_flagged(
    orders: DataFrame, customers: DataFrame
) -> DataFrame:
    return (
        orders.join(customers, "cust_id", "left_anti")
        .withColumn("flag", lit("orphan"))
        .select("order_id", "cust_id", "amount", "flag")
    )


def ex5_closed_west_rename(
    orders: DataFrame, customers: DataFrame
) -> DataFrame:
    return (
        orders.join(customers, "cust_id", "inner")
        .filter((col("status") == "closed") & (col("region") == "west"))
        .withColumnRenamed("amount", "spend")
        .select(
            "order_id",
            "name",
            "spend",
            col("spend").cast("double").alias("spend_d"),
        )
    )


def ex6_full_roster(
    orders: DataFrame, customers: DataFrame
) -> DataFrame:
    return (
        orders.join(customers, "cust_id", "full")
        .withColumn(
            "has_order",
            when(col("order_id").isNotNull(), "yes").otherwise("no"),
        )
        .select("cust_id", "name", "order_id", "has_order")
    )


def ex7_pipeline_chain(
    orders: DataFrame, customers: DataFrame
) -> DataFrame:
    return (
        orders.join(customers, "cust_id", "inner")
        .filter(col("status") == "open")
        .withColumn(
            "fee",
            when(col("tier") == "gold", lit(0.0)).otherwise(
                col("amount") * 0.05
            ),
        )
        .drop("status", "tier", "region")
        .select("order_id", "name", "amount", "fee")
    )


def ex8_column_join_project(
    orders: DataFrame, customers: DataFrame
) -> DataFrame:
    o = orders.alias("o")
    c = customers.alias("c")
    return (
        o.join(c, col("o.cust_id") == col("c.cust_id"), "inner")
        .filter(col("o.amount") >= 100)
        .select(
            col("o.order_id").alias("order_id"),
            col("c.name").alias("customer_name"),
            col("o.amount").alias("amount"),
        )
    )

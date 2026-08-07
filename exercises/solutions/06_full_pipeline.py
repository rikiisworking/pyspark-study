"""Reference solutions for 06_full_pipeline.py — peek after a real try."""

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, count, lit, sum, when


def ex1_open_high_named(orders: DataFrame, customers: DataFrame) -> DataFrame:
    return (
        orders.filter((col("status") == "open") & (col("amount") >= 100))
        .join(customers, "cust_id", "inner")
        .select("order_id", "name", "amount", "region")
    )


def ex2_band_and_tax(orders: DataFrame, customers: DataFrame) -> DataFrame:
    return (
        orders.join(customers, "cust_id", "left")
        .filter(col("amount") >= 100)
        .withColumn(
            "band", when(col("amount") >= 150, "high").otherwise("low")
        )
        .withColumn("tax", col("amount") * 0.1)
        .select("order_id", "name", "amount", "band", "tax")
    )


def ex3_open_total_by_region(
    orders: DataFrame, customers: DataFrame
) -> DataFrame:
    return (
        orders.filter(col("status") == "open")
        .join(customers, "cust_id", "inner")
        .groupBy("region")
        .agg(sum("amount").alias("open_total"))
    )


def ex4_spend_by_tier(orders: DataFrame, customers: DataFrame) -> DataFrame:
    return (
        orders.join(customers, "cust_id", "inner")
        .groupBy("tier")
        .agg(sum("amount").alias("spend"), count("*").alias("n"))
    )


def ex5_big_east_customers(
    orders: DataFrame, customers: DataFrame
) -> DataFrame:
    return (
        orders.join(customers, "cust_id", "inner")
        .filter(col("region") == "east")
        .groupBy("name")
        .agg(sum("amount").alias("spend"))
        .filter(col("spend") > 200)
    )


def ex6_order_counts_all_customers(
    orders: DataFrame, customers: DataFrame
) -> DataFrame:
    return (
        customers.join(orders, "cust_id", "left")
        .groupBy("name")
        .agg(count(col("order_id")).alias("n_orders"))
    )


def ex7_high_value_gold(orders: DataFrame, customers: DataFrame) -> DataFrame:
    return (
        orders.filter(col("amount") >= 100)
        .join(customers, "cust_id", "inner")
        .filter(col("tier") == "gold")
        .groupBy("name")
        .agg(sum("amount").alias("spend"))
    )


def ex8_open_fee_by_customer(
    orders: DataFrame, customers: DataFrame
) -> DataFrame:
    return (
        orders.filter(col("status") == "open")
        .join(customers, "cust_id", "inner")
        .withColumn(
            "fee",
            when(col("tier") == "gold", lit(0.0)).otherwise(
                col("amount") * 0.05
            ),
        )
        .groupBy("name")
        .agg(
            count("*").alias("orders"),
            sum("fee").alias("fee_total"),
        )
    )

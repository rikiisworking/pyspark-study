"""Reference solutions for 20_union_pipeline.py — peek only after a real try."""

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql.functions import coalesce, col, lit, row_number, sum


def ex1_stack_then_open(east: DataFrame, west_shuf: DataFrame) -> DataFrame:
    return (
        east.unionByName(west_shuf)
        .filter(col("status") == "open")
        .select("order_id", "amount", "status")
    )


def ex2_stack_inner_name(
    east: DataFrame, west_shuf: DataFrame, customers: DataFrame
) -> DataFrame:
    return (
        east.unionByName(west_shuf)
        .join(customers, "cust_id")
        .select("order_id", "name", "amount")
    )


def ex3_allow_missing_left_region(
    east: DataFrame, west_extra: DataFrame, customers: DataFrame
) -> DataFrame:
    dim = customers.select(
        "cust_id", "name", col("region").alias("cust_region")
    )
    return (
        east.unionByName(west_extra, allowMissingColumns=True)
        .join(dim, "cust_id", "left")
        .withColumn(
            "region",
            coalesce(col("region"), col("cust_region"), lit("unknown")),
        )
        .select("order_id", "name", "region")
    )


def ex4_tag_then_west(east: DataFrame, west_shuf: DataFrame) -> DataFrame:
    return (
        east.withColumn("src", lit("east"))
        .unionByName(west_shuf.withColumn("src", lit("west")))
        .filter(col("src") == "west")
        .select("order_id", "src", "amount")
    )


def ex5_fill_sum_by_status(east: DataFrame, west_shuf: DataFrame) -> DataFrame:
    return (
        east.unionByName(west_shuf)
        .na.fill({"amount": 0})
        .groupBy("status")
        .agg(sum("amount").alias("total"))
        .select("status", "total")
    )


def ex6_window_top_per_name(
    east: DataFrame, west_shuf: DataFrame, customers: DataFrame
) -> DataFrame:
    w = Window.partitionBy("name").orderBy(
        col("amount").desc(), col("order_id")
    )
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
    (
        east.unionByName(west_shuf)
        .select("order_id", "amount", "status")
        .write.mode("overwrite")
        .parquet(path)
    )
    return spark.read.parquet(path)


def ex8_distinct_then_open_join(
    east: DataFrame, west_shuf: DataFrame, customers: DataFrame
) -> DataFrame:
    return (
        east.unionByName(west_shuf)
        .distinct()
        .filter(col("status") == "open")
        .join(customers, "cust_id")
        .select("order_id", "name", "amount")
    )

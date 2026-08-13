"""Reference solutions for 19_union.py — peek only after a real try."""

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, lit


def ex1_union_aligned(east: DataFrame, west_aligned: DataFrame) -> DataFrame:
    return east.union(west_aligned)


def ex2_union_shuffled_trap(east: DataFrame, west_shuf: DataFrame) -> DataFrame:
    return east.union(west_shuf)


def ex3_union_by_name(east: DataFrame, west_shuf: DataFrame) -> DataFrame:
    return east.unionByName(west_shuf)


def ex4_union_by_name_distinct(
    east: DataFrame, west_aligned: DataFrame
) -> DataFrame:
    return east.unionByName(west_aligned).distinct()


def ex5_allow_missing(east: DataFrame, west_extra: DataFrame) -> DataFrame:
    return east.unionByName(west_extra, allowMissingColumns=True)


def ex6_chain_three(
    east: DataFrame, west_shuf: DataFrame, west_extra: DataFrame
) -> DataFrame:
    return east.unionByName(west_shuf).unionByName(
        west_extra, allowMissingColumns=True
    )


def ex7_then_open(east: DataFrame, west_aligned: DataFrame) -> DataFrame:
    return east.unionByName(west_aligned).filter(col("status") == "open")


def ex8_tag_source(east: DataFrame, west_aligned: DataFrame) -> DataFrame:
    return (
        east.withColumn("src", lit("east"))
        .unionByName(west_aligned.withColumn("src", lit("west")))
        .select("order_id", "src")
    )

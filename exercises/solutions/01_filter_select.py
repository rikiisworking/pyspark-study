"""Reference solutions for 01_filter_select.py — peek only after a real try."""

from pyspark.sql import DataFrame
from pyspark.sql.functions import col


def ex1_high_amount_ids(df: DataFrame) -> DataFrame:
    return df.filter(col("amount") > 100).select("id", "amount")


def ex2_region_east(df: DataFrame) -> DataFrame:
    return df.filter(col("region") == "east")


def ex3_east_and_high(df: DataFrame) -> DataFrame:
    return df.filter((col("region") == "east") & (col("amount") >= 150)).select(
        "id", "region", "amount"
    )


def ex4_west_or_active(df: DataFrame) -> DataFrame:
    return df.filter((col("region") == "west") | (col("status") == "active")).select(
        "id", "region", "status"
    )


def ex5_not_closed(df: DataFrame) -> DataFrame:
    return df.filter(~(col("status") == "closed")).select("id", "status")
    # also fine: col("status") != "closed"


def ex6_regions_in(df: DataFrame) -> DataFrame:
    return df.filter(col("region").isin("east", "west")).select("id", "region")


def ex7_amount_between(df: DataFrame) -> DataFrame:
    return df.filter(col("amount").between(50, 150)).select("id", "amount")


def ex8_project_rename(df: DataFrame) -> DataFrame:
    return (
        df.filter(col("amount") > 100)
        .select("id", "region", col("amount").alias("spend"))
    )

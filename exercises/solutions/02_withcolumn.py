"""Reference solutions for 02_withcolumn.py — peek only after a real try."""

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, lit, when


def ex1_add_double(df: DataFrame) -> DataFrame:
    return df.withColumn("amount2", col("amount") * 2)


def ex2_replace_amount(df: DataFrame) -> DataFrame:
    return df.withColumn("amount", col("amount") + 10)


def ex3_rename_amount(df: DataFrame) -> DataFrame:
    return df.withColumnRenamed("amount", "spend")


def ex4_drop_status(df: DataFrame) -> DataFrame:
    return df.drop("status")


def ex5_cast_amount_double(df: DataFrame) -> DataFrame:
    return df.select("id", "amount", col("amount").cast("double").alias("amount_d"))


def ex6_add_version_lit(df: DataFrame) -> DataFrame:
    return df.select("id", lit("v1").alias("version"))
    # also: df.withColumn("version", lit("v1")).select("id", "version")


def ex7_amount_band(df: DataFrame) -> DataFrame:
    return df.select(
        "id",
        "amount",
        when(col("amount") > 100, "high").otherwise("low").alias("band"),
    )


def ex8_east_with_tax(df: DataFrame) -> DataFrame:
    return (
        df.filter(col("region") == "east")
        .withColumn("tax", col("amount") * 0.1)
        .select("id", "amount", "tax")
    )

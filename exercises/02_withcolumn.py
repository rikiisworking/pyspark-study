"""
Lesson 0002 drills — withColumn, rename, drop, cast, when.

Fill each function. Leave the signature alone.
Run from this directory:
  ../.venv/bin/python 02_withcolumn.py

Data columns (all exercises):
  id INT, region STRING, amount INT, status STRING

Lesson knowledge: lessons/0002-withcolumn-rename.html
Stuck after a real try: exercises/solutions/02_withcolumn.py
"""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, lit, when

# ---------------------------------------------------------------------------
# YOUR CODE — replace `raise NotImplementedError`
# ---------------------------------------------------------------------------


def ex1_add_double(df: DataFrame) -> DataFrame:
    """
    Add column amount2 = amount * 2.
    Keep all original columns; amount2 last.
    """
    modified = df.withColumn("amount2", col("amount")*2)
    return modified


def ex2_replace_amount(df: DataFrame) -> DataFrame:
    """
    Replace amount with amount + 10 (same column name).
    Keep all columns; order unchanged.
    """
    modified = df.withColumn("amount", col("amount")+10)
    return modified

def ex3_rename_amount(df: DataFrame) -> DataFrame:
    """
    Rename amount -> spend. Keep other columns.
    Final names: id, region, spend, status
    """
    modified = df.withColumnRenamed("amount", "spend")
    return modified

def ex4_drop_status(df: DataFrame) -> DataFrame:
    """Drop status. Remaining: id, region, amount."""
    dropped = df.drop("status").select("id", "region", "amount")
    return dropped
    
def ex5_cast_amount_double(df: DataFrame) -> DataFrame:
    """
    Add amount_d = amount cast to double.
    Columns: id, amount, amount_d
    """
    modified = df.withColumn("amount_d", col("amount").cast("double")).select("id", "amount", "amount_d")
    return modified
    
def ex6_add_version_lit(df: DataFrame) -> DataFrame:
    """
    Add version column with literal string "v1" on every row.
    Columns: id, version
    """
    return df.withColumn("version", lit("v1")).select("id", "version")


def ex7_amount_band(df: DataFrame) -> DataFrame:
    """
    Add band:
      "high" if amount > 100
      "low"  otherwise
    Columns: id, amount, band
    """
    return df.withColumn("band", when(col("amount") > 100, "high").otherwise("low")).select("id", "amount", "band")


def ex8_east_with_tax(df: DataFrame) -> DataFrame:
    """
    Keep region == "east".
    Add tax = amount * 0.1  (double arithmetic is fine).
    Columns: id, amount, tax
    """
    return df.filter(col("region")=="east").withColumn("tax", col("amount")*0.1).select("id", "amount", "tax")

# ---------------------------------------------------------------------------
# Checker — do not edit below
# ---------------------------------------------------------------------------

_ROWS = [
    (1, "east", 50, "active"),
    (2, "west", 200, "closed"),
    (3, "east", 150, "active"),
    (4, "north", 120, "pending"),
    (5, "west", 80, "active"),
    (6, "east", 100, "closed"),
]
_SCHEMA = "id INT, region STRING, amount INT, status STRING"


def _base(spark):
    return spark.createDataFrame(_ROWS, _SCHEMA)


if __name__ == "__main__":
    from harness import run_suite

    run_suite(
        "02 withColumn + reshape",
        [
            (
                "ex1_add_double",
                lambda s: (
                    ex1_add_double(_base(s)),
                    [
                        (1, "east", 50, "active", 100),
                        (2, "west", 200, "closed", 400),
                        (3, "east", 150, "active", 300),
                        (4, "north", 120, "pending", 240),
                        (5, "west", 80, "active", 160),
                        (6, "east", 100, "closed", 200),
                    ],
                    ["id", "region", "amount", "status", "amount2"],
                ),
            ),
            (
                "ex2_replace_amount",
                lambda s: (
                    ex2_replace_amount(_base(s)),
                    [
                        (1, "east", 60, "active"),
                        (2, "west", 210, "closed"),
                        (3, "east", 160, "active"),
                        (4, "north", 130, "pending"),
                        (5, "west", 90, "active"),
                        (6, "east", 110, "closed"),
                    ],
                    ["id", "region", "amount", "status"],
                ),
            ),
            (
                "ex3_rename_amount",
                lambda s: (
                    ex3_rename_amount(_base(s)),
                    [
                        (1, "east", 50, "active"),
                        (2, "west", 200, "closed"),
                        (3, "east", 150, "active"),
                        (4, "north", 120, "pending"),
                        (5, "west", 80, "active"),
                        (6, "east", 100, "closed"),
                    ],
                    ["id", "region", "spend", "status"],
                ),
            ),
            (
                "ex4_drop_status",
                lambda s: (
                    ex4_drop_status(_base(s)),
                    [
                        (1, "east", 50),
                        (2, "west", 200),
                        (3, "east", 150),
                        (4, "north", 120),
                        (5, "west", 80),
                        (6, "east", 100),
                    ],
                    ["id", "region", "amount"],
                ),
            ),
            (
                "ex5_cast_amount_double",
                lambda s: (
                    ex5_cast_amount_double(_base(s)),
                    [
                        (1, 50, 50.0),
                        (2, 200, 200.0),
                        (3, 150, 150.0),
                        (4, 120, 120.0),
                        (5, 80, 80.0),
                        (6, 100, 100.0),
                    ],
                    ["id", "amount", "amount_d"],
                ),
            ),
            (
                "ex6_add_version_lit",
                lambda s: (
                    ex6_add_version_lit(_base(s)),
                    [
                        (1, "v1"),
                        (2, "v1"),
                        (3, "v1"),
                        (4, "v1"),
                        (5, "v1"),
                        (6, "v1"),
                    ],
                    ["id", "version"],
                ),
            ),
            (
                "ex7_amount_band",
                lambda s: (
                    ex7_amount_band(_base(s)),
                    [
                        (1, 50, "low"),
                        (2, 200, "high"),
                        (3, 150, "high"),
                        (4, 120, "high"),
                        (5, 80, "low"),
                        (6, 100, "low"),
                    ],
                    ["id", "amount", "band"],
                ),
            ),
            (
                "ex8_east_with_tax",
                lambda s: (
                    ex8_east_with_tax(_base(s)),
                    [
                        (1, 50, 5.0),
                        (3, 150, 15.0),
                        (6, 100, 10.0),
                    ],
                    ["id", "amount", "tax"],
                ),
            ),
        ],
    )

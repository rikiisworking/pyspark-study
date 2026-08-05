"""
Lesson 0001 drills — filter + select.

Fill each function. Leave the signature alone.
Run from this directory:
  ../.venv/bin/python 01_filter_select.py

Data columns (all exercises):
  id INT, region STRING, amount INT, status STRING

Stuck? Peek exercises/solutions/01_filter_select.py — then retype blind.
"""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql.functions import col

# ---------------------------------------------------------------------------
# YOUR CODE — replace `raise NotImplementedError`
# ---------------------------------------------------------------------------


def ex1_high_amount_ids(df: DataFrame) -> DataFrame:
    """Rows with amount > 100. Columns: id, amount (that order)."""
    filtered_df = df.filter(col("amount") > 100).select("id", "amount")
    return filtered_df

def ex2_region_east(df: DataFrame) -> DataFrame:
    """Rows where region == 'east'. All columns, original order."""
    filtered_df = df.filter(col("region") == "east")
    return filtered_df

def ex3_east_and_high(df: DataFrame) -> DataFrame:
    """region == 'east' AND amount >= 150. Columns: id, region, amount."""
    filtered_df = df.filter((col("region") == "east") & (col("amount") >= 150)).select("id", "region", "amount")
    return filtered_df

def ex4_west_or_active(df: DataFrame) -> DataFrame:
    """region == 'west' OR status == 'active'. Columns: id, region, status."""
    filtered_df = df.filter((col("region") == "west") | (col("status") == "active")).select("id", "region", "status")
    return filtered_df

def ex5_not_closed(df: DataFrame) -> DataFrame:
    """status is NOT 'closed'. Columns: id, status."""
    filtered_df = df.filter(col("status") != "closed").select("id", "status")
    return filtered_df


def ex6_regions_in(df: DataFrame) -> DataFrame:
    """region is 'east' or 'west' (use isin). Columns: id, region."""
    target_regions = ["east", "west"]
    filtered_df = df.filter(col("region").isin(target_regions)).select("id", "region")
    return filtered_df

def ex7_amount_between(df: DataFrame) -> DataFrame:
    """amount between 50 and 150 inclusive. Columns: id, amount."""
    filtered_df = df.filter((col("amount") >= 50) & (col("amount") <= 150)).select("id", "amount")
    return filtered_df


def ex8_project_rename(df: DataFrame) -> DataFrame:
    """
    amount > 100. Select:
      id, region, amount aliased as spend
    Column order: id, region, spend
    """
    filtered_df = df.filter(col("amount") > 100).select("id", "region", col("amount").alias("spend"))
    return filtered_df                      


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
        "01 filter + select",
        [
            (
                "ex1_high_amount_ids",
                lambda s: (
                    ex1_high_amount_ids(_base(s)),
                    [(2, 200), (3, 150), (4, 120)],
                    ["id", "amount"],
                ),
            ),
            (
                "ex2_region_east",
                lambda s: (
                    ex2_region_east(_base(s)),
                    [
                        (1, "east", 50, "active"),
                        (3, "east", 150, "active"),
                        (6, "east", 100, "closed"),
                    ],
                    ["id", "region", "amount", "status"],
                ),
            ),
            (
                "ex3_east_and_high",
                lambda s: (
                    ex3_east_and_high(_base(s)),
                    [(3, "east", 150)],
                    ["id", "region", "amount"],
                ),
            ),
            (
                "ex4_west_or_active",
                lambda s: (
                    ex4_west_or_active(_base(s)),
                    [
                        (1, "east", "active"),
                        (2, "west", "closed"),
                        (3, "east", "active"),
                        (5, "west", "active"),
                    ],
                    ["id", "region", "status"],
                ),
            ),
            (
                "ex5_not_closed",
                lambda s: (
                    ex5_not_closed(_base(s)),
                    [(1, "active"), (3, "active"), (4, "pending"), (5, "active")],
                    ["id", "status"],
                ),
            ),
            (
                "ex6_regions_in",
                lambda s: (
                    ex6_regions_in(_base(s)),
                    [
                        (1, "east"),
                        (2, "west"),
                        (3, "east"),
                        (5, "west"),
                        (6, "east"),
                    ],
                    ["id", "region"],
                ),
            ),
            (
                "ex7_amount_between",
                lambda s: (
                    ex7_amount_between(_base(s)),
                    [(1, 50), (3, 150), (4, 120), (5, 80), (6, 100)],
                    ["id", "amount"],
                ),
            ),
            (
                "ex8_project_rename",
                lambda s: (
                    ex8_project_rename(_base(s)),
                    [
                        (2, "west", 200),
                        (3, "east", 150),
                        (4, "north", 120),
                    ],
                    ["id", "region", "spend"],
                ),
            ),
        ],
    )

"""
Lesson 0021 drills — union / unionByName (stack frames).

Spark union is by *position* (SQL UNION ALL). Prefer unionByName.
Run:
  ../.venv/bin/python 19_union.py

Data (built in checker):

  east: order_id INT, amount INT, status STRING
    (1, 100, "open")
    (2,  50, "open")
    (3, 200, "closed")
    (4,  80, "open")

  west_aligned: order_id INT, amount INT, status STRING
    (4,  80, "open")   # same row as east 4
    (5,  40, "open")
    (6, 150, "closed")

  west_shuf: amount INT, order_id INT, status STRING   # names same, seats swapped
    (80, 4, "open")
    (40, 5, "open")
    (150, 6, "closed")

  west_extra: order_id INT, amount INT, status STRING, region STRING
    (7, 90, "open", "north")

Lesson: lessons/0021-union-by-name.html
Solutions (after real try): exercises/solutions/19_union.py
"""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql.functions import (  # noqa: F401 — use what you need
    col,
    lit,
)

# ---------------------------------------------------------------------------
# YOUR CODE — replace `raise NotImplementedError`
# ---------------------------------------------------------------------------


def ex1_union_aligned(east: DataFrame, west_aligned: DataFrame) -> DataFrame:
    """
    Stack east then west_aligned with union (same column order).
    Columns: order_id, amount, status
    """
    return (
        east.union(west_aligned).select("order_id", "amount", "status")
    )

def ex2_union_shuffled_trap(east: DataFrame, west_shuf: DataFrame) -> DataFrame:
    """
    Stack east then west_shuf with union — by *position*, not name.
    Do not unionByName. amount sits in order_id's seat.
    Columns: order_id, amount, status
    """
    return (
        east.union(west_shuf).select("order_id", "amount", "status")
    )

def ex3_union_by_name(east: DataFrame, west_shuf: DataFrame) -> DataFrame:
    """
    Stack east then west_shuf with unionByName.
    Columns: order_id, amount, status
    """
    return (
        east.unionByName(west_shuf)
    )


def ex4_union_by_name_distinct(
    east: DataFrame, west_aligned: DataFrame
) -> DataFrame:
    """
    unionByName east + west_aligned, then distinct (drop the twin row 4).
    Columns: order_id, amount, status
    """
    return (
        east.unionByName(west_aligned).distinct()
    )

def ex5_allow_missing(east: DataFrame, west_extra: DataFrame) -> DataFrame:
    """
    unionByName east + west_extra with allowMissingColumns=True.
    Columns: order_id, amount, status, region
    """
    return (
        east.unionByName(west_extra, allowMissingColumns=True)
    )

def ex6_chain_three(
    east: DataFrame, west_shuf: DataFrame, west_extra: DataFrame
) -> DataFrame:
    """
    east.unionByName(west_shuf) then unionByName(west_extra, allowMissingColumns=True).
    Columns: order_id, amount, status, region
    """
    return (
        east.unionByName(west_shuf)
            .unionByName(west_extra, allowMissingColumns=True)
    )

def ex7_then_open(east: DataFrame, west_aligned: DataFrame) -> DataFrame:
    """
    unionByName east + west_aligned. Keep status == "open".
    Columns: order_id, amount, status
    """
    return (
        east.unionByName(west_aligned)
            .filter(col("status") == "open")
    )

def ex8_tag_source(east: DataFrame, west_aligned: DataFrame) -> DataFrame:
    """
    Tag east src="east", west_aligned src="west", unionByName.
    Columns: order_id, src
    """
    return (
        east.withColumn("src", lit("east")).unionByName(
            west_aligned.withColumn("src", lit("west"))
        ).select("order_id", "src")
    )


# ---------------------------------------------------------------------------
# Checker — do not edit below
# ---------------------------------------------------------------------------

_EAST = [
    (1, 100, "open"),
    (2, 50, "open"),
    (3, 200, "closed"),
    (4, 80, "open"),
]
_EAST_SCHEMA = "order_id INT, amount INT, status STRING"

_WEST_ALIGNED = [
    (4, 80, "open"),
    (5, 40, "open"),
    (6, 150, "closed"),
]
_WEST_ALIGNED_SCHEMA = "order_id INT, amount INT, status STRING"

_WEST_SHUF = [
    (80, 4, "open"),
    (40, 5, "open"),
    (150, 6, "closed"),
]
_WEST_SHUF_SCHEMA = "amount INT, order_id INT, status STRING"

_WEST_EXTRA = [
    (7, 90, "open", "north"),
]
_WEST_EXTRA_SCHEMA = "order_id INT, amount INT, status STRING, region STRING"


def _east(spark) -> DataFrame:
    return spark.createDataFrame(_EAST, _EAST_SCHEMA)


def _west_aligned(spark) -> DataFrame:
    return spark.createDataFrame(_WEST_ALIGNED, _WEST_ALIGNED_SCHEMA)


def _west_shuf(spark) -> DataFrame:
    return spark.createDataFrame(_WEST_SHUF, _WEST_SHUF_SCHEMA)


def _west_extra(spark) -> DataFrame:
    return spark.createDataFrame(_WEST_EXTRA, _WEST_EXTRA_SCHEMA)


if __name__ == "__main__":
    from harness import run_suite

    run_suite(
        "19 union / unionByName",
        [
            (
                "ex1_union_aligned",
                lambda s: (
                    ex1_union_aligned(_east(s), _west_aligned(s)),
                    [
                        (1, 100, "open"),
                        (2, 50, "open"),
                        (3, 200, "closed"),
                        (4, 80, "open"),
                        (4, 80, "open"),
                        (5, 40, "open"),
                        (6, 150, "closed"),
                    ],
                    ["order_id", "amount", "status"],
                ),
            ),
            (
                "ex2_union_shuffled_trap",
                lambda s: (
                    ex2_union_shuffled_trap(_east(s), _west_shuf(s)),
                    [
                        (1, 100, "open"),
                        (2, 50, "open"),
                        (3, 200, "closed"),
                        (4, 80, "open"),
                        (80, 4, "open"),
                        (40, 5, "open"),
                        (150, 6, "closed"),
                    ],
                    ["order_id", "amount", "status"],
                ),
            ),
            (
                "ex3_union_by_name",
                lambda s: (
                    ex3_union_by_name(_east(s), _west_shuf(s)),
                    [
                        (1, 100, "open"),
                        (2, 50, "open"),
                        (3, 200, "closed"),
                        (4, 80, "open"),
                        (4, 80, "open"),
                        (5, 40, "open"),
                        (6, 150, "closed"),
                    ],
                    ["order_id", "amount", "status"],
                ),
            ),
            (
                "ex4_union_by_name_distinct",
                lambda s: (
                    ex4_union_by_name_distinct(_east(s), _west_aligned(s)),
                    [
                        (1, 100, "open"),
                        (2, 50, "open"),
                        (3, 200, "closed"),
                        (4, 80, "open"),
                        (5, 40, "open"),
                        (6, 150, "closed"),
                    ],
                    ["order_id", "amount", "status"],
                ),
            ),
            (
                "ex5_allow_missing",
                lambda s: (
                    ex5_allow_missing(_east(s), _west_extra(s)),
                    [
                        (1, 100, "open", None),
                        (2, 50, "open", None),
                        (3, 200, "closed", None),
                        (4, 80, "open", None),
                        (7, 90, "open", "north"),
                    ],
                    ["order_id", "amount", "status", "region"],
                ),
            ),
            (
                "ex6_chain_three",
                lambda s: (
                    ex6_chain_three(_east(s), _west_shuf(s), _west_extra(s)),
                    [
                        (1, 100, "open", None),
                        (2, 50, "open", None),
                        (3, 200, "closed", None),
                        (4, 80, "open", None),
                        (4, 80, "open", None),
                        (5, 40, "open", None),
                        (6, 150, "closed", None),
                        (7, 90, "open", "north"),
                    ],
                    ["order_id", "amount", "status", "region"],
                ),
            ),
            (
                "ex7_then_open",
                lambda s: (
                    ex7_then_open(_east(s), _west_aligned(s)),
                    [
                        (1, 100, "open"),
                        (2, 50, "open"),
                        (4, 80, "open"),
                        (4, 80, "open"),
                        (5, 40, "open"),
                    ],
                    ["order_id", "amount", "status"],
                ),
            ),
            (
                "ex8_tag_source",
                lambda s: (
                    ex8_tag_source(_east(s), _west_aligned(s)),
                    [
                        (1, "east"),
                        (2, "east"),
                        (3, "east"),
                        (4, "east"),
                        (4, "west"),
                        (5, "west"),
                        (6, "west"),
                    ],
                    ["order_id", "src"],
                ),
            ),
        ],
    )

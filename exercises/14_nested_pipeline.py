"""
Lesson 0014 drills — nested/JSON + pipeline mix (0001–0013).

No new API. Interleave from_json / explode / get_json_object with
filter, join, fill, groupBy, window, parquet.
Run:
  ../.venv/bin/python 14_nested_pipeline.py

Data (built in checker):

  events: event_id INT, raw STRING
    1  {"user_id":10,"amount":100,"status":"open","tags":["a","b"]}
    2  {"user_id":20,"amount":50,"status":"open","tags":["x"]}
    3  {"user_id":10,"amount":200,"status":"closed","tags":[]}
    4  {"user_id":99,"amount":80,"status":"open","tags":["z"]}   # orphan
    5  {"user_id":30,"amount":300,"status":"open","tags":["y"]}
    6  {"user_id":20,"amount":150,"status":"closed","tags":["x"]}
    7  {"user_id":10,"amount":null,"status":"open","tags":["a"]}

  customers: cust_id INT, name STRING, region STRING
    (10, "Alice", "east")
    (20, "Bob",   None)
    (30, "Carol", "east")
    (40, "Dan",   "north")     # no events

  JSON_SCHEMA:
    user_id INT, amount INT, status STRING, tags ARRAY<STRING>

Lesson: lessons/0014-nested-pipeline-mix.html
Solutions (after real try): exercises/solutions/14_nested_pipeline.py
"""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql.functions import (  # noqa: F401
    coalesce,
    col,
    explode,
    explode_outer,
    from_json,
    get_json_object,
    lit,
    row_number,
    sum,
)

JSON_SCHEMA = "user_id INT, amount INT, status STRING, tags ARRAY<STRING>"

# ---------------------------------------------------------------------------
# YOUR CODE — replace `raise NotImplementedError`
# ---------------------------------------------------------------------------


def ex1_parse_project(events: DataFrame) -> DataFrame:
    """
    from_json raw.
    Columns: event_id, user_id, amount, status
    """
    return events.withColumn("p", from_json("raw", JSON_SCHEMA)).select(
        "event_id", "p.user_id", "p.amount", "p.status"
    )


def ex2_open_inner_name(events: DataFrame, customers: DataFrame) -> DataFrame:
    """
    from_json, status == "open", inner join customers on user_id == cust_id.
    Columns: event_id, name, amount
    """
    return (
        events.withColumn("p", from_json("raw", JSON_SCHEMA))
        .filter(col("p.status") == "open")
        .withColumn("cust_id", col("p.user_id"))
        .join(customers, "cust_id")
        .select("event_id", "name", "p.amount")
    )


def ex3_explode_tag_x(events: DataFrame) -> DataFrame:
    """
    from_json, explode tags, keep tag == "x".
    Columns: event_id, user_id, tag
    """
    return (
        events.withColumn("p", from_json("raw", JSON_SCHEMA))
        .select("event_id", "p.user_id", explode(col("p.tags")).alias("tag"))
        .filter(col("tag") == "x")
    )


def ex4_outer_tag_left_name(events: DataFrame, customers: DataFrame) -> DataFrame:
    """
    from_json, explode_outer tags, left join customers.
    Columns: event_id, name, tag
    """
    return (
        events.withColumn("p", from_json("raw", JSON_SCHEMA))
        .withColumn("cust_id", col("p.user_id"))
        .join(customers, "cust_id", "left")
        .select("event_id", "name", explode_outer(col("p.tags")).alias("tag"))
    )


def ex5_fill_open_sum_by_name(events: DataFrame, customers: DataFrame) -> DataFrame:
    """
    from_json, fill null amount with 0, status == "open",
    inner join customers, total = sum(amount) per name.
    Columns: name, total
    """
    return (
        events.withColumn("p", from_json("raw", JSON_SCHEMA))
        .withColumn("cust_id", col("p.user_id"))
        .withColumn("amount", col("p.amount"))
        .fillna({"amount": 0})
        .filter(col("p.status") == "open")
        .join(customers, "cust_id")
        .groupby("name")
        .agg(sum("amount").alias("total"))
        .select("name", "total")
    )


def ex6_top_open_per_user(events: DataFrame, customers: DataFrame) -> DataFrame:
    """
    from_json, status == "open", amount is not null.
    rn = row_number per user_id, amount DESC then event_id ASC.
    Keep rn == 1, inner join customers.
    Columns: event_id, name, amount
    """
    w = Window.partitionBy("cust_id").orderBy(
        col("amount").desc(), col("event_id").asc()
    )
    return (
        events.withColumn("p", from_json("raw", JSON_SCHEMA))
        .withColumn("cust_id", col("p.user_id"))
        .withColumn("amount", col("p.amount"))
        .filter((col("p.status") == "open") & (col("amount").isNotNull()))
        .withColumn("rn", row_number().over(w))
        .filter(col("rn") == 1)
        .join(customers, "cust_id")
        .select("event_id", "name", "amount")
    )


def ex7_sum_by_region(events: DataFrame, customers: DataFrame) -> DataFrame:
    """
    from_json, fill amount nulls with 0, inner join customers.
    region = coalesce(region, "unknown").
    Per region: total = sum(amount).
    Columns: region, total
    """
    return (
        events.withColumn("p", from_json("raw", JSON_SCHEMA))
        .withColumn("cust_id", col("p.user_id"))
        .withColumn("amount", col("p.amount"))
        .fillna({"amount": 0})
        .join(customers, "cust_id")
        .withColumn("region", coalesce(col("region"), lit("unknown")))
        .groupby("region")
        .agg(sum(col("amount")).alias("total"))
        .select("region", "total")
    )


def ex8_open_parquet_roundtrip(
    spark: SparkSession, events: DataFrame, path: str
) -> DataFrame:
    """
    from_json, status == "open", write parquet overwrite, read back.
    Columns: event_id, user_id, amount
    """
    (
        events.withColumn("p", from_json("raw", JSON_SCHEMA))
        .withColumn("user_id", col("p.user_id"))
        .withColumn("amount", col("p.amount"))
        .filter(col("p.status") == "open")
        .write.mode("overwrite")
        .parquet(path)
    )

    return spark.read.parquet(path).select("event_id", "user_id", "amount")


# ---------------------------------------------------------------------------
# Checker — do not edit below
# ---------------------------------------------------------------------------

import shutil
import tempfile

_EVENT_ROWS = [
    (1, '{"user_id":10,"amount":100,"status":"open","tags":["a","b"]}'),
    (2, '{"user_id":20,"amount":50,"status":"open","tags":["x"]}'),
    (3, '{"user_id":10,"amount":200,"status":"closed","tags":[]}'),
    (4, '{"user_id":99,"amount":80,"status":"open","tags":["z"]}'),
    (5, '{"user_id":30,"amount":300,"status":"open","tags":["y"]}'),
    (6, '{"user_id":20,"amount":150,"status":"closed","tags":["x"]}'),
    (7, '{"user_id":10,"amount":null,"status":"open","tags":["a"]}'),
]
_EVENT_SCHEMA = "event_id INT, raw STRING"

_CUST_ROWS = [
    (10, "Alice", "east"),
    (20, "Bob", None),
    (30, "Carol", "east"),
    (40, "Dan", "north"),
]
_CUST_SCHEMA = "cust_id INT, name STRING, region STRING"


def _events(spark):
    return spark.createDataFrame(_EVENT_ROWS, _EVENT_SCHEMA)


def _customers(spark):
    return spark.createDataFrame(_CUST_ROWS, _CUST_SCHEMA)


def _run_path(spark, fn, expect, columns):
    d = tempfile.mkdtemp(prefix="pyspark-njp-")
    try:
        got = fn(spark, _events(spark), d)
        rows = got.collect()
        materialized = spark.createDataFrame(rows, got.schema)
        return materialized, expect, columns
    finally:
        shutil.rmtree(d, ignore_errors=True)


if __name__ == "__main__":
    from harness import run_suite

    run_suite(
        "14 nested + pipeline mix",
        [
            (
                "ex1_parse_project",
                lambda s: (
                    ex1_parse_project(_events(s)),
                    [
                        (1, 10, 100, "open"),
                        (2, 20, 50, "open"),
                        (3, 10, 200, "closed"),
                        (4, 99, 80, "open"),
                        (5, 30, 300, "open"),
                        (6, 20, 150, "closed"),
                        (7, 10, None, "open"),
                    ],
                    ["event_id", "user_id", "amount", "status"],
                ),
            ),
            (
                "ex2_open_inner_name",
                lambda s: (
                    ex2_open_inner_name(_events(s), _customers(s)),
                    [
                        (1, "Alice", 100),
                        (2, "Bob", 50),
                        (5, "Carol", 300),
                        (7, "Alice", None),
                    ],
                    ["event_id", "name", "amount"],
                ),
            ),
            (
                "ex3_explode_tag_x",
                lambda s: (
                    ex3_explode_tag_x(_events(s)),
                    [
                        (2, 20, "x"),
                        (6, 20, "x"),
                    ],
                    ["event_id", "user_id", "tag"],
                ),
            ),
            (
                "ex4_outer_tag_left_name",
                lambda s: (
                    ex4_outer_tag_left_name(_events(s), _customers(s)),
                    [
                        (1, "Alice", "a"),
                        (1, "Alice", "b"),
                        (2, "Bob", "x"),
                        (3, "Alice", None),
                        (4, None, "z"),
                        (5, "Carol", "y"),
                        (6, "Bob", "x"),
                        (7, "Alice", "a"),
                    ],
                    ["event_id", "name", "tag"],
                ),
            ),
            (
                "ex5_fill_open_sum_by_name",
                lambda s: (
                    ex5_fill_open_sum_by_name(_events(s), _customers(s)),
                    [
                        ("Alice", 100),
                        ("Bob", 50),
                        ("Carol", 300),
                    ],
                    ["name", "total"],
                ),
            ),
            (
                "ex6_top_open_per_user",
                lambda s: (
                    ex6_top_open_per_user(_events(s), _customers(s)),
                    [
                        (1, "Alice", 100),
                        (2, "Bob", 50),
                        (5, "Carol", 300),
                    ],
                    ["event_id", "name", "amount"],
                ),
            ),
            (
                "ex7_sum_by_region",
                lambda s: (
                    ex7_sum_by_region(_events(s), _customers(s)),
                    [
                        ("east", 600),
                        ("unknown", 200),
                    ],
                    ["region", "total"],
                ),
            ),
            (
                "ex8_open_parquet_roundtrip",
                lambda s: _run_path(
                    s,
                    ex8_open_parquet_roundtrip,
                    [
                        (1, 10, 100),
                        (2, 20, 50),
                        (4, 99, 80),
                        (5, 30, 300),
                        (7, 10, None),
                    ],
                    ["event_id", "user_id", "amount"],
                ),
            ),
        ],
    )

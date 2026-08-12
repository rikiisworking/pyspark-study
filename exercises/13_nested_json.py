"""
Lesson 0013 drills — nested columns + JSON strings.

No cluster theory. Cold-write: struct dots, from_json, get_json_object, explode.
Run:
  ../.venv/bin/python 13_nested_json.py

Data (built in checker):

  orders:  order_id INT, user STRUCT<id:INT, name:STRING>, amount INT
    (1, (10, "Alice"), 100)
    (2, (20, "Bob"),    50)
    (3, (10, "Alice"), 200)
    (4, (30, "Carol"),  80)

  events:  event_id INT, raw STRING  (JSON text)
    1  {"user":{"id":10,"name":"Alice"},"amount":100,"tags":["a","b"]}
    2  {"user":{"id":20,"name":"Bob"},"amount":50,"tags":["x"]}
    3  {"user":{"id":10,"name":"Alice"},"amount":200,"tags":[]}
    4  {"user":{"id":30,"name":"Carol"},"amount":80,"tags":["y","z"]}

  JSON schema (DDL string for from_json):
    user STRUCT<id:INT, name:STRING>, amount INT, tags ARRAY<STRING>

Lesson: lessons/0013-nested-json.html
Solutions (after real try): exercises/solutions/13_nested_json.py
"""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql.functions import (  # noqa: F401
    col,
    explode,
    explode_outer,
    from_json,
    get_json_object,
    sum,
)

# Shared DDL for from_json drills (copy into solutions if you like).
JSON_SCHEMA = "user STRUCT<id:INT, name:STRING>, amount INT, tags ARRAY<STRING>"

# ---------------------------------------------------------------------------
# YOUR CODE — replace `raise NotImplementedError`
# ---------------------------------------------------------------------------


def ex1_struct_field_select(orders: DataFrame) -> DataFrame:
    """
    Nested struct already typed. Select:
      order_id, name (= user.name), amount
    Columns: order_id, name, amount
    """
    return (
        orders.select("order_id", "user.name", "amount")
    )


def ex2_filter_nested_user(orders: DataFrame) -> DataFrame:
    """
    Keep rows where user.id == 10.
    Columns: order_id, name, amount  (name = user.name)
    """
    return (
        orders
            .filter(col("user.id")==10)
            .select("order_id", "user.name", "amount")
    )


def ex3_from_json_select(events: DataFrame) -> DataFrame:
    """
    Parse raw with from_json(..., JSON_SCHEMA).
    Columns: event_id, user_id (= p.user.id), amount (= p.amount)
    """
    return (
        events.withColumn("p", from_json(col("raw"), JSON_SCHEMA))
            .select("event_id", col("p.user.id").alias("user_id"), "p.amount")
    )

def ex4_get_json_object_user_id(events: DataFrame) -> DataFrame:
    """
    No full schema. Extract $.user.id via get_json_object, cast to INT.
    Columns: event_id, user_id
    """
    
    return (
        events
            .withColumn(
                "user_id",
                get_json_object(col("raw"),"$.user.id").cast("INT")
            )
            .select("event_id", "user_id")
    )


def ex5_explode_tags(events: DataFrame) -> DataFrame:
    """
    from_json → explode tags (drops empty arrays).
    Columns: event_id, tag
    """

    
    return (
        events
            .withColumn("p", from_json(col("raw"), JSON_SCHEMA))
            .select("event_id", explode(col("p.tags")).alias("tag"))
    )


def ex6_explode_outer_tags(events: DataFrame) -> DataFrame:
    """
    from_json → explode_outer tags (empty array → one row, tag null).
    Columns: event_id, tag
    """
    return (
        events
            .withColumn("p", from_json(col("raw"), JSON_SCHEMA))
            .select("event_id", explode_outer(col("p.tags")).alias("tag"))
    )


def ex7_json_filter_amount(events: DataFrame) -> DataFrame:
    """
    from_json, keep amount >= 100.
    Columns: event_id, user_id, amount
    """
    return (
        events
            .withColumn("p", from_json(col("raw"), JSON_SCHEMA))
            .withColumn("amount", col("p.amount").cast("int"))
            .filter(col("amount") >= 100)
            .select("event_id", col("p.user.id").alias("user_id"), "amount")
    )


def ex8_json_sum_by_user(events: DataFrame) -> DataFrame:
    """
    from_json, groupBy user_id, total = sum(amount).
    Columns: user_id, total
    """
    
   

    return (
        events
        .withColumn("p", from_json(col("raw"), JSON_SCHEMA))
        .withColumn("amount", col("p.amount").cast("int"))
        .withColumn("user_id", col("p.user.id").cast("int"))
        .groupBy("user_id")
        .agg(
            sum("amount").alias("total")
        )
        .select("user_id", "total")
    )
# ---------------------------------------------------------------------------
# Checker — do not edit below
# ---------------------------------------------------------------------------

_ORDER_ROWS = [
    (1, (10, "Alice"), 100),
    (2, (20, "Bob"), 50),
    (3, (10, "Alice"), 200),
    (4, (30, "Carol"), 80),
]
_ORDER_SCHEMA = "order_id INT, user STRUCT<id:INT, name:STRING>, amount INT"

_EVENT_ROWS = [
    (1, '{"user":{"id":10,"name":"Alice"},"amount":100,"tags":["a","b"]}'),
    (2, '{"user":{"id":20,"name":"Bob"},"amount":50,"tags":["x"]}'),
    (3, '{"user":{"id":10,"name":"Alice"},"amount":200,"tags":[]}'),
    (4, '{"user":{"id":30,"name":"Carol"},"amount":80,"tags":["y","z"]}'),
]
_EVENT_SCHEMA = "event_id INT, raw STRING"


def _orders(spark):
    return spark.createDataFrame(_ORDER_ROWS, _ORDER_SCHEMA)


def _events(spark):
    return spark.createDataFrame(_EVENT_ROWS, _EVENT_SCHEMA)


if __name__ == "__main__":
    from harness import run_suite

    run_suite(
        "13 nested JSON",
        [
            (
                "ex1_struct_field_select",
                lambda s: (
                    ex1_struct_field_select(_orders(s)),
                    [
                        (1, "Alice", 100),
                        (2, "Bob", 50),
                        (3, "Alice", 200),
                        (4, "Carol", 80),
                    ],
                    ["order_id", "name", "amount"],
                ),
            ),
            (
                "ex2_filter_nested_user",
                lambda s: (
                    ex2_filter_nested_user(_orders(s)),
                    [
                        (1, "Alice", 100),
                        (3, "Alice", 200),
                    ],
                    ["order_id", "name", "amount"],
                ),
            ),
            (
                "ex3_from_json_select",
                lambda s: (
                    ex3_from_json_select(_events(s)),
                    [
                        (1, 10, 100),
                        (2, 20, 50),
                        (3, 10, 200),
                        (4, 30, 80),
                    ],
                    ["event_id", "user_id", "amount"],
                ),
            ),
            (
                "ex4_get_json_object_user_id",
                lambda s: (
                    ex4_get_json_object_user_id(_events(s)),
                    [
                        (1, 10),
                        (2, 20),
                        (3, 10),
                        (4, 30),
                    ],
                    ["event_id", "user_id"],
                ),
            ),
            (
                "ex5_explode_tags",
                lambda s: (
                    ex5_explode_tags(_events(s)),
                    [
                        (1, "a"),
                        (1, "b"),
                        (2, "x"),
                        (4, "y"),
                        (4, "z"),
                    ],
                    ["event_id", "tag"],
                ),
            ),
            (
                "ex6_explode_outer_tags",
                lambda s: (
                    ex6_explode_outer_tags(_events(s)),
                    [
                        (1, "a"),
                        (1, "b"),
                        (2, "x"),
                        (3, None),
                        (4, "y"),
                        (4, "z"),
                    ],
                    ["event_id", "tag"],
                ),
            ),
            (
                "ex7_json_filter_amount",
                lambda s: (
                    ex7_json_filter_amount(_events(s)),
                    [
                        (1, 10, 100),
                        (3, 10, 200),
                    ],
                    ["event_id", "user_id", "amount"],
                ),
            ),
            (
                "ex8_json_sum_by_user",
                lambda s: (
                    ex8_json_sum_by_user(_events(s)),
                    [
                        (10, 300),
                        (20, 50),
                        (30, 80),
                    ],
                    ["user_id", "total"],
                ),
            ),
        ],
    )

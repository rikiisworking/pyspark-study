"""Tiny checker for PySpark DF exercises. Compare row bags + column order."""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import Row


def make_spark(app_name: str = "pyspark-study") -> SparkSession:
    return (
        SparkSession.builder.master("local[1]")
        .appName(app_name)
        .config("spark.ui.enabled", "false")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )


def as_tuples(df: DataFrame) -> list[tuple]:
    """Collect rows as tuples; sort so order of rows does not matter."""
    cols = df.columns
    rows = [tuple(r[c] for c in cols) for r in df.collect()]
    return sorted(rows, key=lambda t: tuple((x is None, x) for x in t))


def check(name: str, got: DataFrame, expect: list[tuple], columns: list[str]) -> bool:
    if list(got.columns) != list(columns):
        print(f"FAIL  {name}")
        print(f"  columns got:      {list(got.columns)}")
        print(f"  columns expected: {columns}")
        return False
    g = as_tuples(got)
    e = sorted(expect, key=lambda t: tuple((x is None, x) for x in t))
    if g != e:
        print(f"FAIL  {name}")
        print(f"  rows got:      {g}")
        print(f"  rows expected: {e}")
        return False
    print(f"PASS  {name}")
    return True


def run_suite(title: str, cases: list[tuple]) -> None:
    """
    cases: list of (name, thunk) where thunk() -> (got_df, expect_rows, columns)
    """
    spark = make_spark(title)
    spark.sparkContext.setLogLevel("ERROR")
    print(f"\n=== {title} ===")
    ok = 0
    try:
        for name, thunk in cases:
            try:
                got, expect, columns = thunk(spark)
                if check(name, got, expect, columns):
                    ok += 1
            except NotImplementedError:
                print(f"TODO  {name}  (fill in the function body)")
            except Exception as exc:  # noqa: BLE001 — student feedback
                print(f"FAIL  {name}")
                print(f"  error: {type(exc).__name__}: {exc}")
    finally:
        spark.stop()
    total = len(cases)
    print(f"\n{ok}/{total} passed")
    raise SystemExit(0 if ok == total else 1)

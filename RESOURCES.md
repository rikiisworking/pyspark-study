# PySpark Resources

## Knowledge

- [Quickstart: DataFrame — PySpark official](https://spark.apache.org/docs/latest/api/python/getting_started/quickstart_df.html)
  Short official notebook-style tour of DF API. Use for: first exposure to create/view/select/filter patterns.
- [Spark SQL, DataFrames and Datasets Guide](https://spark.apache.org/docs/latest/sql-programming-guide.html)
  Canonical conceptual guide (DF = Dataset of rows with named columns). Use for: untyped vs typed mental model, when API claims need grounding.
- [PySpark User Guide — Chapter 1: DataFrames](https://spark.apache.org/docs/latest/api/python/user_guide/dataframes.html)
  Official chapter: create, view, manipulate. Use for: lesson content on day-1 DF ops.
- [pyspark.sql.DataFrame API reference](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/dataframe.html)
  Method index. Use for: exact signatures when building drills or reference sheets.
- [DataFrame.filter](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.filter.html) · [DataFrame.select](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.select.html) · [functions.col](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.col.html)
  Official method docs. Use for: filter/select/col syntax truth.
- [DataFrame.groupBy](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.groupBy.html) · [GroupedData.agg](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.GroupedData.agg.html) · [functions.sum](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.sum.html) · [functions.count](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.count.html) · [functions.avg](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.avg.html)
  Official groupBy/agg docs. Use for: lesson 0005, default col names vs alias, multi-agg.
- [DataFrame.join](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.join.html)
  Join types + ambiguous keys. Use for: lesson 0003 and join+groupBy pipelines.
- [SparkSession.createDataFrame](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.SparkSession.createDataFrame.html)
  How toy frames get built for practice. Use for: local drills without real files.
- [pyspark.sql.Window](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.Window.html) · [Window API index](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/window.html)
  partitionBy / orderBy / rowsBetween / rangeBetween + default frames. Use for: lesson 0007.
- [functions.row_number](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.row_number.html) · [rank](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.rank.html) · [dense_rank](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.dense_rank.html) · [lag](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.lag.html) · [lead](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.lead.html)
  Ranking + offset window fns. Use for: top-N, day-over-day, running totals with sum().over.
- [Column.isNull](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.Column.isNull.html) · [isNotNull](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.Column.isNotNull.html) · [eqNullSafe](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.Column.eqNullSafe.html)
  Null tests + null-safe equality. Use for: lesson 0009 filters and join keys with nulls.
- [DataFrameNaFunctions](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrameNaFunctions.html) · [fill](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrameNaFunctions.fill.html) · [drop](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrameNaFunctions.drop.html)
  na.fill / na.drop (aliases fillna / dropna). Use for: bulk null handling.
- [functions.coalesce](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.coalesce.html)
  First non-null column. Use for: default values mid-pipeline.
- [DataFrameWriter](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrameWriter.html) · [DataFrameReader](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrameReader.html) · [Writer.mode](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrameWriter.mode.html)
  Save/load API + overwrite/append/ignore/error. Use for: lesson 0011.
- [Parquet Files](https://spark.apache.org/docs/latest/sql-data-sources-parquet.html) · [CSV Files](https://spark.apache.org/docs/latest/sql-data-sources-csv.html) · [Generic load/save](https://spark.apache.org/docs/latest/sql-data-sources-load-save-functions.html)
  Official data source guides. Use for: format options, partition discovery.
- [functions.from_json](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.from_json.html) · [get_json_object](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.get_json_object.html) · [explode](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.explode.html) · [explode_outer](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.explode_outer.html)
  Nested/JSON parse + array flatten. Use for: lesson 0013.
- [Running SQL Queries Programmatically — Getting Started](https://spark.apache.org/docs/latest/sql-getting-started.html#running-sql-queries-programmatically) · [Global Temporary View](https://spark.apache.org/docs/latest/sql-getting-started.html#global-temporary-view)
  Official bridge: register DF as view, `spark.sql`, local vs global temp. Use for: lesson 0015.
- [createOrReplaceTempView](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.createOrReplaceTempView.html) · [createOrReplaceGlobalTempView](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.createOrReplaceGlobalTempView.html) · [SparkSession.sql](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.SparkSession.sql.html) · [selectExpr](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.selectExpr.html)
  API truth for views + SQL + expression fragments. Use for: lesson 0015 drills and reference.

## Wisdom (Communities)

- [Stack Overflow — [pyspark]](https://stackoverflow.com/questions/tagged/pyspark)
  High volume Q&A; check version tags. Use for: "why does this expression fail" error hunting.
- [Apache Spark user mailing list](https://lists.apache.org/list.html?user@spark.apache.org)
  Official user list. Use for: design questions beyond SO snippets.
- [r/apachespark](https://www.reddit.com/r/apachespark/)
  Informal peer discussion. Use for: "is this pattern normal at work?" sanity checks.

## Gaps

- No workplace-specific job/style guide yet (naming, when SQL vs DF). Add when user pastes team conventions.

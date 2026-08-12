# Spark SQL interop 8/8

User completed `15_sql_interop` green: createOrReplaceTempView + filter/join/agg, SQL→DF tax chain, replace-view wins, global_temp.qualify, open+east join filter.

**Fluency gap (still PASS):**
- ex4: used `withColumn` + `select` instead of `selectExpr` for `amount * 2 AS double_amt`. Same rows; missed the no-view SQL-fragment path the drill targets.

Implications: temp views + `spark.sql` + global_temp solid. Prefer rewrite ex4 with `selectExpr` once for storage strength. Mission track item 9 (SQL interop) demonstrated — “know when SQL is fine” now has concrete evidence. Core DF + nested + SQL interop track complete unless new mission scope opens.

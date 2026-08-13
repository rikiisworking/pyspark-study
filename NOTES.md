# Notes

## User prefs
- Goal: syntax muscle memory for **work pipelines**
- Level: seen code, can't write cold
- Track: DataFrame API first
- Modes: caveman + ponytail full (terse, minimal scaffolding)
- Lessons: short, retrieval-heavy, DF chain focused

## Teaching bets
- Skip RDD / cluster theory until asked
- Prefer Column expressions (`col("x") > 1`) over SQL-string filters early — both valid, Column scales better in pipelines
- Every lesson: one write-from-memory drill + tight quiz
- Runnable drills: `exercises/*.py` via `.venv` (pyspark 4.2). Checker in `exercises/harness.py`
- Lesson HTML must include knowledge enough to write exercise solutions (map knowledge → ex#)
- After isolated PASS, offer integration mix before next new API (`04` after joins; `06` after groupBy)
- After core chain fluent: window → mix → nulls → optional read/write
- Window vs groupBy is the main conceptual fork — hammer it in 0007
- After isolated PASS, offer integration mix before next new API (`04` after joins; `06` after groupBy; `08` after window)
- Null trap: `col == value` drops nulls (three-valued logic); use `isNull` / `eqNullSafe` when needed
- After nulls PASS: nulls+pipeline mix (10), then read/write (parquet/csv)
- Mix 0010 hammers: fill-before-agg, open-OR-isNull, left+coalesce region
- Read/write (0011): mode deliberate; CSV needs header+inferSchema (or schema); write partitionBy ≠ window partitionBy
- Capstone 0012: re-hammer write partitionBy + filter-after-CSV-read (gaps from 0011 PASS)
- After core+capstone: nested/JSON (0013) — work payloads; then optional mix; then Spark SQL interop
- Nested traps: from_json needs schema; get_json_object → string; explode drops empty; explode_outer keeps
- Mix 0014: parse early + alias flat; join user_id↔cust_id; fill before sum; null amount out of top-N
- After nested track: SQL interop (0015) — mission “know when SQL is fine”
- SQL interop traps: createOrReplaceTempView preferred; global_temp.qualify; spark.sql → DF; selectExpr no view
- After SQL isolated PASS: mix 0016 — SQL↔DF both directions; re-hammer selectExpr; fill-before-sum; left+coalesce
- 0016 ex6 stall: SQL + Window spec written, `w` unused — forgot row_number().over(w) + filter rn==1 (same as 07/08). Unstuck: 0017
- 0016 ex6 checker gap: SQL-only already matches expected bag (one open non-null order per name). filter(rn==1) is a no-op; extra `rn` column fails. Green ≠ window ran.
- 0016 8/8. Core track closed. Stretch pick: datetime (0018 / 17_datetime.py)
- Spark 4: to_date raises on junk; teach try_to_date. try_to_timestamp format needs lit().

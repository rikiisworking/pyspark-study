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

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
- After core chain fluent, optional: window, read/write + nulls

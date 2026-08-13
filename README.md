# PySpark study

Cold-write fluency for the **DataFrame API** — work pipelines without flipping to colleague notebooks.

See [MISSION.md](MISSION.md) for why and success criteria.

## Setup (once)

```bash
python3 -m venv .venv
.venv/bin/pip install -r requirements.txt
```

Needs a JDK (Spark local). Repo uses `pyspark` from [requirements.txt](requirements.txt).

## How to study

1. Open the next lesson HTML in a browser (`lessons/000N-….html`).
2. Fill the matching file in `exercises/` (replace `NotImplementedError`).
3. Run the checker:

```bash
cd exercises
../.venv/bin/python 12_capstone_mix.py   # example
```

4. Peek at `exercises/solutions/` only after a real try.
5. Ask the teaching agent follow-ups when stuck.

**Reference sheet (print-friendly):** [reference/dataframe-core-syntax.html](reference/dataframe-core-syntax.html)

**Drill index:** [exercises/README.md](exercises/README.md)

## Track

Dates are when the learning record was written (git add date).

| # | Topic | Lesson | Drill | Passed |
|---|--------|--------|--------|--------|
| 1 | filter + select | [0001](lessons/0001-filter-select-cold.html) | `01_filter_select.py` | [2026-08-05](learning-records/0002-filter-select-mastered.md) |
| 2 | withColumn / reshape | [0002](lessons/0002-withcolumn-rename.html) | `02_withcolumn.py` | [2026-08-05](learning-records/0003-withcolumn-mastered.md) |
| 3 | joins | [0003](lessons/0003-joins.html) | `03_joins.py` | [2026-08-05](learning-records/0004-joins-passed-fluency-gap.md) |
| 3b | pipeline mix | [0004](lessons/0004-pipeline-mix.html) | `04_pipeline_mix.py` | [2026-08-06](learning-records/0005-pipeline-mix-passed.md) |
| 4 | groupBy + agg | [0005](lessons/0005-groupby-agg.html) | `05_groupby_agg.py` | [2026-08-07](learning-records/0006-groupby-agg-passed.md) |
| 4b | full pipeline mix | [0006](lessons/0006-full-pipeline-mix.html) | `06_full_pipeline.py` | [2026-08-07](learning-records/0007-full-pipeline-passed.md) |
| 5 | window functions | [0007](lessons/0007-window-functions.html) | `07_window.py` | [2026-08-07](learning-records/0008-window-functions-passed.md) |
| 5b | window + pipeline mix | [0008](lessons/0008-window-pipeline-mix.html) | `08_window_pipeline.py` | [2026-08-10](learning-records/0009-window-pipeline-passed.md) |
| 6 | nulls | [0009](lessons/0009-nulls.html) | `09_nulls.py` | [2026-08-10](learning-records/0010-nulls-passed.md) |
| 6b | nulls + pipeline mix | [0010](lessons/0010-nulls-pipeline-mix.html) | `10_nulls_pipeline.py` | [2026-08-10](learning-records/0011-nulls-pipeline-passed.md) |
| 7 | read / write | [0011](lessons/0011-read-write.html) | `11_read_write.py` | [2026-08-12](learning-records/0012-read-write-passed.md) |
| 7b | capstone mix | [0012](lessons/0012-capstone-mix.html) | `12_capstone_mix.py` | [2026-08-12](learning-records/0013-capstone-mix-passed.md) |
| 8 | nested + JSON | [0013](lessons/0013-nested-json.html) | `13_nested_json.py` | [2026-08-12](learning-records/0014-nested-json-passed.md) |
| 8b | nested + pipeline mix | [0014](lessons/0014-nested-pipeline-mix.html) | `14_nested_pipeline.py` | [2026-08-12](learning-records/0015-nested-pipeline-passed.md) |
| 9 | Spark SQL interop | [0015](lessons/0015-sql-interop.html) | `15_sql_interop.py` | [2026-08-12](learning-records/0016-sql-interop-passed.md) |
| 9b | SQL + pipeline mix | [0016](lessons/0016-sql-pipeline-mix.html) | `16_sql_pipeline.py` | [2026-08-13](learning-records/0018-sql-pipeline-mix-passed.md) |
| 10 | datetime | [0018](lessons/0018-datetime.html) | `17_datetime.py` | [2026-08-13](learning-records/0019-datetime-passed.md) |
| 10b | datetime + pipeline mix | [0020](lessons/0020-datetime-pipeline-mix.html) | `18_datetime_pipeline.py` | [2026-08-13](learning-records/0020-datetime-pipeline-passed.md) |
| 11 | union / unionByName | [0021](lessons/0021-union-by-name.html) | `19_union.py` | [2026-08-13](learning-records/0021-union-by-name-passed.md) |
| 11b | union + pipeline mix | [0022](lessons/0022-union-pipeline-mix.html) | `20_union_pipeline.py` | [2026-08-13](learning-records/0022-union-pipeline-passed.md) |

## Layout

```
lessons/            # short HTML lessons (open in browser)
exercises/          # runnable PASS/FAIL drills + solutions/
reference/          # compressed syntax cheat sheet
assets/             # shared lesson CSS / quiz JS
learning-records/   # what was demonstrated (teaching state)
MISSION.md          # goal + track status
RESOURCES.md        # official docs + communities
```

## Out of scope (for now)

Cluster ops, RDD API, Structured Streaming, MLlib, deep Catalyst/AQE — until core DF syntax is solid.

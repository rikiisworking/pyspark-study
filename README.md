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
../.venv/bin/python 09_nulls.py   # example
```

4. Peek at `exercises/solutions/` only after a real try.
5. Ask the teaching agent follow-ups when stuck.

**Reference sheet (print-friendly):** [reference/dataframe-core-syntax.html](reference/dataframe-core-syntax.html)

**Drill index:** [exercises/README.md](exercises/README.md)

## Track

| # | Topic | Lesson | Drill |
|---|--------|--------|--------|
| 1 | filter + select | [0001](lessons/0001-filter-select-cold.html) | `01_filter_select.py` |
| 2 | withColumn / reshape | [0002](lessons/0002-withcolumn-rename.html) | `02_withcolumn.py` |
| 3 | joins | [0003](lessons/0003-joins.html) | `03_joins.py` |
| 3b | pipeline mix | [0004](lessons/0004-pipeline-mix.html) | `04_pipeline_mix.py` |
| 4 | groupBy + agg | [0005](lessons/0005-groupby-agg.html) | `05_groupby_agg.py` |
| 4b | full pipeline mix | [0006](lessons/0006-full-pipeline-mix.html) | `06_full_pipeline.py` |
| 5 | window functions | [0007](lessons/0007-window-functions.html) | `07_window.py` |
| 5b | window + pipeline mix | [0008](lessons/0008-window-pipeline-mix.html) | `08_window_pipeline.py` |
| 6 | nulls | [0009](lessons/0009-nulls.html) | `09_nulls.py` |

Optional later: nulls+pipeline mix; read/write (parquet/csv).

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

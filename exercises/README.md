# Exercises

Local Spark. Fill functions. Run file. Get PASS/FAIL.

## Setup (once)

From repo root:

```bash
python3 -m venv .venv
.venv/bin/pip install -r requirements.txt
```

## Run

```bash
cd exercises
../.venv/bin/python 01_filter_select.py
```

## Files

| File | Topic | Lesson |
|------|--------|--------|
| `01_filter_select.py` | filter, `&` `\|`, isin, between, select + alias | [0001](../lessons/0001-filter-select-cold.html) |
| `02_withcolumn.py` | withColumn, rename, drop, cast, lit, when | [0002](../lessons/0002-withcolumn-rename.html) |
| `03_joins.py` | inner/left/right/full/semi/anti, Column join | [0003](../lessons/0003-joins.html) |
| `04_pipeline_mix.py` | mix 0001–0003 into mini pipelines | [0004 mix](../lessons/0004-pipeline-mix.html) |
| `solutions/` | reference answers — after a real try | |

Row order does not matter. Column **names and order** do.

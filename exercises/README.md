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
| `05_groupby_agg.py` | groupBy, count/sum/avg/max, multi-key, having, join+agg | [0005](../lessons/0005-groupby-agg.html) |
| `06_full_pipeline.py` | mix 0001–0005 into full work-style chains | [0006 mix](../lessons/0006-full-pipeline-mix.html) |
| `07_window.py` | partitionBy/orderBy, rank, lag/lead, running sum, top-N | [0007](../lessons/0007-window-functions.html) |
| `08_window_pipeline.py` | mix 0001–0007: filter/join + window top-N / lag / running | [0008 mix](../lessons/0008-window-pipeline-mix.html) |
| `09_nulls.py` | isNull / isNotNull, na.drop, na.fill, coalesce, when | [0009](../lessons/0009-nulls.html) |
| `10_nulls_pipeline.py` | nulls + filter/join/groupBy mix | [0010 mix](../lessons/0010-nulls-pipeline-mix.html) |
| `11_read_write.py` | parquet/csv read write, modes, partitionBy | [0011](../lessons/0011-read-write.html) |
| `12_capstone_mix.py` | full track + I/O interleaved | [0012 mix](../lessons/0012-capstone-mix.html) |
| `13_nested_json.py` | struct dots, from_json, get_json_object, explode | [0013](../lessons/0013-nested-json.html) |
| `14_nested_pipeline.py` | nested/JSON + join/fill/window/I/O mix | [0014 mix](../lessons/0014-nested-pipeline-mix.html) |
| `solutions/` | reference answers — after a real try | |

Row order does not matter. Column **names and order** do.

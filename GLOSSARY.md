# PySpark study glossary

Terms the user has used correctly. Prefer these words in later lessons.

## Dates

**ISO parse**:
In this workspace, convert a string with `try_to_date(..., "yyyy-MM-dd")`. Matches ISO 8601 calendar day `YYYY-MM-DD`.
_Avoid_: iso_parse (not a Spark function)

**EU parse**:
In this workspace, convert a day-first string with `try_to_date(..., "dd/MM/yyyy")`.
_Avoid_: calling this ISO

## Stack

**union**:
Spark `DataFrame.union` — SQL `UNION ALL` by **position**. No dedup. `unionAll` is an alias.
_Avoid_: assuming it matches names

**unionByName**:
Spark `DataFrame.unionByName` — stack by column name. Extra/missing name raises unless `allowMissingColumns=True` (null in the hole).

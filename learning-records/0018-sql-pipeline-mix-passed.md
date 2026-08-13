# SQL + pipeline mix 8/8

User completed `16_sql_pipeline` green: SQL↔DF both directions, selectExpr, left+coalesce, window top-1, fill-before-sum, parquet roundtrip.

**Fluency notes (still PASS):**
- ex6: built `Window` then omitted `.over` at first; later saw SQL-only also greens this toy set (one open non-null row per name).
- ex7–ex8: written without a new unstuck lesson (`fillna` alias of `na.fill` is fine).

Implications: mission core track (1–9b) demonstrated. Dates closed (LR-0019/0020). unionByName closed (LR-0021). Remaining stalls: broadcast, pivot, array HOFs.

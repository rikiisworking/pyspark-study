# Datetime 8/8

User completed `17_datetime` green: try_to_date ISO, coalesce ISO+EU, to/try_to_timestamp, date_trunc+date_format, datediff(end, start), year/month, report format, sum by month after drop-null sold.

**Fluency notes (still PASS):**
- ex3 first pointed `try_to_timestamp` at `sold_at` (date-only vs `HH:mm:ss`) → all nulls; then corrected to `ts` + `lit(pattern)`.
- ex2 `coalesce` is EU then ISO (docstring said ISO then EU). Same bag — one pattern always nulls.
- Asked what “ISO parse” means; then used `try_to_date(..., "yyyy-MM-dd")` correctly in ex4–ex8.

Implications: stretch item 10 (datetime) demonstrated. Optional next: datetime + pipeline mix (filter/join/window/I/O) before another new API.

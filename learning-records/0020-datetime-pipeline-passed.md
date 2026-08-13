# Datetime + pipeline mix 8/8

User completed `18_datetime_pipeline` green: ISO parse then filter/join, coalesce ISO+EU then east, datediff then lag>2, try_to_timestamp on `ts`+lit, fill-before-sum by month, parquet roundtrip.

**Fluency note (still PASS):** ex6 window is `orderBy("order_id")` only — not amount desc, then order_id. This fixture’s min `order_id` per month is also the max amount (Jan 1/100, Feb 3/200), so the bag matches without ranking by amount. Same class of gap as 0016: green ≠ the intended window.

Implications: stretch 10b demonstrated. Next new API only after ex6 `orderBy` is amount desc, then order_id (or the fixture changes so min-id ≠ top amount).

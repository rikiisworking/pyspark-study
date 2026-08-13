# Datetime + pipeline mix 8/8

User completed `18_datetime_pipeline` green: ISO parse then filter/join, coalesce ISO+EU then east, datediff then lag>2, try_to_timestamp on `ts`+lit, fill-before-sum by month, parquet roundtrip.

**Fluency note:** first pass ordered the window by `order_id` only. Toy min-id == max amount, so the bag was already green. Rewrote to `orderBy(col("amount").desc(), col("order_id"))` after review.

Implications: stretch 10b demonstrated, including top-N by amount. Next: pick a new stretch API.

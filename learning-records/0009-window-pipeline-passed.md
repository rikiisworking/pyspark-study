# Window + pipeline mix 8/8

User completed `08_window_pipeline` green: open filter → join → row_number/rank/dense_rank, lag, running sum, top-N, amount−lag delta. Correctly used dense_rank for top-1 (works with full order key); preferred pattern is `row_number` + filter.

Implications: core DF chain + window compose fluent. Next new API: nulls (`isNull` / `na.fill` / `coalesce`) before read/write.

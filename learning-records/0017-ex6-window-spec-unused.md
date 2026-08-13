# 0016 ex6: Window spec built, ranking not applied

User stalled on `ex6_sql_join_window_top` with the SQL join already correct and `w = Window.partitionBy("name").orderBy(...)` defined, then returned the SQL DataFrame. `w` was unused.

This is fluency, not a new-API gap — they already passed 07/08 top-1 (`row_number` + `filter(rn == 1)`). Mix load dropped the apply step.

Follow-up: SQL-only also PASSes. After `open AND amount IS NOT NULL` + inner join, the toy set is already one row per name — the expected bag. The harness never sees whether a window ran. `filter(rn == 1)` is a no-op here; leaving `rn` on the frame fails columns.

Implications: when SQL and Window sit in one function, prompt the three-step apply (paint / filter / drop rn) before teaching anything new. Do not re-teach Window from scratch. Do not treat a green checker as proof the window fired when the prefilter already unique-keys the partition.

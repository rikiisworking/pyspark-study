# Read/write 8/8

User completed `11_read_write` green: parquet/csv roundtrip, filter-then-write, append doubles, overwrite replaces, amount filter write+select.

**Fluency gaps (still PASS):**
- ex4: filtered `region=="east"` then wrote plain parquet — never used write `partitionBy("region")` + read filter. Same rows; missed disk-layout skill.
- ex8: filtered open before CSV write — lesson intent is write **full** then filter on read. Same rows; missed post-read filter habit.

Implications: I/O modes solid. Optional: rewrite ex4/ex8 the “intended” way once for storage strength. Core DF track complete through read/write.

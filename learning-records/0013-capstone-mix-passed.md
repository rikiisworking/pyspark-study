# Capstone mix 8/8

User completed `12_capstone_mix` green: fill/join/agg, dropna join, parquet open, partitionBy region + east filter, overwrite open-then-closed, CSV full write then open filter, window top-open-per-name, open fill sum by coalesce region.

**ex3 nuance (still PASS):** wrote *all* orders then filtered open on read. Lesson intent closer to filter open → write → read (disk holds only open). Final rows match; missed “filter before write” path that ex6 deliberately inverts.

Implications: 0001–0011 skills interleaved and solid. partitionBy + filter-after-read fluency gaps from LR-0012 closed here (ex4, ex6). Optional: re-do ex3 as filter-then-write once for storage strength. Capstone complete.

# Nulls 8/8

User completed `09_nulls` green: `isNull` / `isNotNull`, equality filter dropping null status, drop via dual `isNotNull` (equiv `na.drop(subset=…)`), `fillna` amount→0, `coalesce` region default, null-aware `when` flag, open-or-null-status with `|`.

Implications: null API fluent. Next was nulls+pipeline mix (done in LR-0011); after that read/write (parquet/csv).

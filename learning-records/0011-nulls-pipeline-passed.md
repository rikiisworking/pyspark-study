# Nulls + pipeline mix 8/8

User completed `10_nulls_pipeline` green: fill/drop mid-chain, left join + `coalesce` region, open-OR-null status, fill→join→sum by name, `when` missing-amount flag, open fill sum by region.

**Evidence:** 8/8 checker PASS. Order of fill vs filter/join flexible on toy data when ops commute.

**Trap (latent):** ex5 wrote `isNull | open & isNotNull` without parens on the OR. Passes because no null-status+null-amount row; intended is `(isNull | open) & isNotNull`. Column `&` binds tighter than `|`.

Implications: nulls compose fluent with filter/join/agg. Core DF track complete through mix. Next optional: read/write (parquet/csv).

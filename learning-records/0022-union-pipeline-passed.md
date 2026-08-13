# Union + pipeline mix 8/8

User completed `20_union_pipeline` green: unionByName then filter/join, allowMissing + left + coalesce region, src-tag before stack, fill-before-sum, window top-N by amount, parquet, distinct then open join.

**Fluency note (still PASS):** ex3 stalled on two `region` columns after left join. Fixed by renaming the dim to `cust_region` (`withColumnRenamed`) then `coalesce(stack region, cust_region, "unknown")`. Window `orderBy` was amount desc, then order_id on the first write — fixture would have caught min-id.

Implications: stretch 11b demonstrated. Next new API from remaining stalls: broadcast, pivot, array HOFs.

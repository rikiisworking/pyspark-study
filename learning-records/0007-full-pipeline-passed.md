# Full pipeline mix 8/8

User completed `06_full_pipeline` with all cases green: open-high named filter+join, left join band/tax, open totals by region, spend by tier, east spend HAVING, all-customer order counts (right/left null→0), high-value gold, open fee chain with when+agg.

Implications: mission core DF chain fluent end-to-end (filter → withColumn → join → groupBy → agg). Optional next: window functions, or read/write + nulls.

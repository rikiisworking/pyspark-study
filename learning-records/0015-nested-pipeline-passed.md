# Nested pipeline mix 8/8

User completed `14_nested_pipeline` green: parse+project, open+inner join, explode filter tag, explode_outer+left join, fill+open+sum by name, window top open per user, sum by coalesce region, open parquet roundtrip.

Style notes (still PASS): string select `"p.user_id"` → leaf col names; alias `cust_id` from `p.user_id` for string-key join (clean). ex4 joins before explode_outer — same rows as explode-then-join here. Window partitionBy `"cust_id"` after rename = per user_id.

Implications: nested compose with join/window/I/O solid. Next optional: Spark SQL interop (temp views).

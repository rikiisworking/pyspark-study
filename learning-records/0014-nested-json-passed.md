# Nested + JSON 8/8

User completed `13_nested_json` green: struct field select/filter, from_json project, get_json_object+cast, explode vs explode_outer, parse+filter, parse+groupBy sum.

Style note (still PASS): string select `"user.name"` / `"p.amount"` worked (Spark leaf names). Explicit `col(...).alias("name")` clearer under review. ex7/ex8 re-cast amount/user_id to int — redundant after from_json DDL already typed INT.

Implications: nested access + JSON parse solid. Next: nested/JSON pipeline mix (interleave with join/window/I/O) or Spark SQL interop.

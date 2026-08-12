# Mission: PySpark DataFrame syntax fluency

## Why
You already own/write Spark pipelines at work, but cold-writing DataFrame API trips you up. Goal: stop flipping to colleague notebooks — type `select` / `filter` / transforms from memory under deadline.

## Success looks like
- Write a multi-step DataFrame chain (`filter` → `select` → `withColumn` → `groupBy` → `join`) without docs open
- Read a colleague's pipeline and edit it without "what does this API do?" stalls
- Prefer DataFrame API as default; know when SQL string is fine

## Constraints
- Prior: seen PySpark (notebooks / colleague code), not zero
- Track first: **DataFrame API** (not Spark SQL)
- Prefer short syntax drills over architecture theory
- Caveman + ponytail: tight lessons, no fluff scaffolding

## Core track
1. filter + select — done
2. withColumn / reshape — done
3. joins — done
3b. pipeline mix (fluency, no new API) — done
4. groupBy + agg — done
4b. full pipeline mix (filter→join→groupBy→agg) — done
5. window functions — done
5b. window + pipeline mix — done
6. nulls (isNull / fill / coalesce) — done
6b. nulls + pipeline mix — done
7. read / write (parquet + csv) — done
7b. capstone mix (full track + I/O) — done
8. nested + JSON (struct dots, from_json, explode) — done

Optional later: nested/JSON pipeline mix; Spark SQL interop (temp views).

## Out of scope
- Cluster ops, YARN/K8s, cost tuning (until syntax solid)
- RDD API
- Structured Streaming / MLlib (until core DF fluent)
- Deep Catalyst/AQE internals

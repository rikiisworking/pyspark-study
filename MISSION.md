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

## Core track (4 lessons)
1. filter + select — done
2. withColumn / reshape — done
3. joins — current
4. groupBy + agg — last core piece for mission chain

Optional later: window, read/write + nulls.

## Out of scope
- Cluster ops, YARN/K8s, cost tuning (until syntax solid)
- RDD API
- Structured Streaming / MLlib (until core DF fluent)
- Deep Catalyst/AQE internals

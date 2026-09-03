# Databricks notebook source
# MAGIC %md
# MAGIC # Class 1 — Spark Batch Processing
# MAGIC
# MAGIC This notebook explains **how Apache Spark executes batch jobs**, from the programming model down to the
# MAGIC physical execution engine. It is meant to be read top to bottom, then run cell by cell in Databricks.
# MAGIC
# MAGIC ## 1. What problem Spark solves
# MAGIC
# MAGIC A single machine cannot hold or process a dataset that is terabytes in size in reasonable time. Spark solves
# MAGIC this by:
# MAGIC
# MAGIC - **Partitioning** data across many machines (executors).
# MAGIC - Describing computation as a **logical plan** (a DAG of transformations) rather than executing eagerly.
# MAGIC - **Optimizing** that plan (Catalyst optimizer) before generating code (Tungsten/whole-stage codegen).
# MAGIC - **Scheduling** the optimized plan as stages of parallel tasks across the cluster.
# MAGIC
# MAGIC ## 2. Architecture
# MAGIC
# MAGIC ```text
# MAGIC                +------------------+
# MAGIC                |      Driver       |   <- builds logical plan, negotiates resources,
# MAGIC                |  (SparkContext)   |      schedules tasks, collects results
# MAGIC                +---------+--------+
# MAGIC                          |
# MAGIC          +---------------+----------------+
# MAGIC          |               |                |
# MAGIC  +-------v------+ +------v-------+ +------v-------+
# MAGIC  |  Executor 1   | |  Executor 2   | |  Executor N   |
# MAGIC  |  (JVM proc)   | |  (JVM proc)   | |  (JVM proc)   |
# MAGIC  |  tasks/cores  | |  tasks/cores  | |  tasks/cores  |
# MAGIC  |  partitions   | |  partitions   | |  partitions   |
# MAGIC  +---------------+ +---------------+ +---------------+
# MAGIC ```
# MAGIC
# MAGIC - The **driver** runs your code, builds the DAG, and asks the cluster manager (in Databricks: the Databricks
# MAGIC   runtime scheduler on top of the cluster) for executors.
# MAGIC - **Executors** run **tasks** — one task per partition per stage. Executors hold data in memory/disk (cache)
# MAGIC   and report results/status back to the driver.
# MAGIC - A **job** is triggered by an **action** (e.g. `count()`, `collect()`, `write`). A job is split into
# MAGIC   **stages**, and stages are split by **shuffle boundaries**. Each stage is a set of **tasks** that can run
# MAGIC   without moving data between partitions.
# MAGIC
# MAGIC ## 3. Transformations vs actions (laziness)
# MAGIC
# MAGIC - **Transformations** (`select`, `filter`, `withColumn`, `join`, `groupBy`) build the logical plan. Nothing
# MAGIC   executes yet.
# MAGIC - **Actions** (`count`, `collect`, `show`, `write.save`, `toTable`) trigger execution of the plan built so far.
# MAGIC
# MAGIC This laziness lets Catalyst see the *entire* plan before deciding how to execute it — e.g. it can push a
# MAGIC filter below a join (predicate pushdown) even though you wrote the filter after the join in your code.
# MAGIC
# MAGIC ## 4. Narrow vs wide transformations (why shuffle matters)
# MAGIC
# MAGIC - **Narrow**: each output partition depends on exactly one input partition (`select`, `filter`,
# MAGIC   `withColumn`, `union`, a join against a **broadcast** table). No data movement across the network.
# MAGIC - **Wide**: an output partition depends on *many* input partitions, which requires a **shuffle** — data is
# MAGIC   repartitioned and moved across the network/disk (`groupBy`, `distinct`, `repartition`, a sort-merge join).
# MAGIC
# MAGIC Shuffles are the single biggest cost lever in Spark: they involve disk I/O, network I/O, and serialization.
# MAGIC Most performance tuning is really "reduce or reshape shuffles."
# MAGIC
# MAGIC ## 5. Catalyst, Tungsten, and Adaptive Query Execution (AQE)
# MAGIC
# MAGIC - **Catalyst** is the logical/physical query optimizer: it rewrites your DataFrame/SQL plan (predicate
# MAGIC   pushdown, column pruning, constant folding, join reordering) before picking a physical strategy
# MAGIC   (broadcast-hash-join vs sort-merge-join, etc.).
# MAGIC - **Tungsten** is the execution engine: off-heap binary row format, whole-stage code generation (compiles a
# MAGIC   chain of operators into a single JVM bytecode loop instead of interpreting each operator).
# MAGIC - **AQE** (on by default in modern Spark) re-optimizes the plan *during* execution using actual runtime
# MAGIC   statistics: it can coalesce many small shuffle partitions into fewer larger ones, switch a sort-merge join
# MAGIC   to a broadcast join if a table turns out to be small, and split skewed partitions.
# MAGIC
# MAGIC ## 6. Partitioning
# MAGIC
# MAGIC - **In-memory partitions** control parallelism (`spark.sql.shuffle.partitions`, `repartition`,
# MAGIC   `coalesce`). Too few partitions under-utilizes the cluster; too many creates task-scheduling overhead and
# MAGIC   tiny output files.
# MAGIC - **On-disk partitioning** (`partitionBy("event_date")` when writing) creates a directory-per-value layout
# MAGIC   so downstream readers can skip irrelevant data (partition pruning).
# MAGIC
# MAGIC Below, we run these concepts against real data using the retail clickstream dataset shared across this
# MAGIC project.

# COMMAND ----------

try:
    dbutils.widgets.text("catalog", "main", "Unity Catalog catalog")
    dbutils.widgets.text("schema", "retail_lakehouse", "Schema/database")
    dbutils.widgets.text("volume", "raw", "Unity Catalog volume")
    dbutils.widgets.text("base_path", "", "Optional override; blank uses /Volumes/<catalog>/<schema>/<volume>")
except Exception:
    pass

from pyspark.sql import functions as F

catalog = dbutils.widgets.get("catalog") if "dbutils" in globals() else "main"
schema = dbutils.widgets.get("schema") if "dbutils" in globals() else "retail_lakehouse"
volume = dbutils.widgets.get("volume") if "dbutils" in globals() else "raw"
base_path = dbutils.widgets.get("base_path") if "dbutils" in globals() else ""
if not base_path:
    base_path = f"/Volumes/{catalog}/{schema}/{volume}"


def table(name: str) -> str:
    return f"`{catalog}`.`{schema}`.`{name}`"


def path(*parts: str) -> str:
    return base_path.rstrip("/") + "/" + "/".join(p.strip("/") for p in parts)


spark.sql(f"CREATE CATALOG IF NOT EXISTS `{catalog}`")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{catalog}`.`{schema}`")
spark.sql(f"CREATE VOLUME IF NOT EXISTS `{catalog}`.`{schema}`.`{volume}`")
spark.sql(f"USE CATALOG `{catalog}`")
spark.sql(f"USE SCHEMA `{schema}`")
print(f"catalog={catalog}, schema={schema}, volume={volume}, base_path={base_path}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Generate a small synthetic dataset
# MAGIC
# MAGIC We generate deterministic data in-notebook so this class runs on any cluster with no external dependency.

# COMMAND ----------

import random
from datetime import datetime, timedelta, timezone

random.seed(7)

products = [
    (f"p{i:04d}", random.choice(["electronics", "grocery", "home", "apparel", "beauty"]),
     random.choice(["acme", "northstar", "evergreen", "summit", "nova"]), round(random.uniform(3, 500), 2))
    for i in range(1, 51)
]
customers = [
    (f"u{i:05d}", random.choice(["new", "active", "loyal", "at_risk"]), random.choice(["west", "central", "south", "east"]))
    for i in range(1, 201)
]
events = []
start = datetime.now(timezone.utc) - timedelta(hours=2)
for i in range(200_000):
    event_type = random.choice(["view", "add_to_cart", "purchase", "search", "checkout"])
    qty = random.randint(1, 4) if event_type in ("purchase", "checkout") else None
    price = round(random.uniform(5, 250), 2) if qty else None
    events.append((
        f"evt-{i:08d}",
        start + timedelta(seconds=random.randint(0, 7200)),
        random.choice(customers)[0],
        random.choice(products)[0],
        event_type, qty, price,
    ))

products_df = spark.createDataFrame(products, "product_id string, category string, brand string, price double")
customers_df = spark.createDataFrame(customers, "user_id string, segment string, region string")
events_df = spark.createDataFrame(
    events, "event_id string, event_ts timestamp, user_id string, product_id string, event_type string, quantity int, price double"
)

print("Event rows:", events_df.count())
num_partitions = events_df.select(F.spark_partition_id().alias("p")).distinct().count()
print("Default partitions of events_df:", num_partitions)

# COMMAND ----------
# MAGIC %md
# MAGIC ## Laziness in action
# MAGIC
# MAGIC The next cell defines transformations only. Nothing runs yet — Spark just extends the logical plan.

# COMMAND ----------

normalized = (
    events_df
    .withColumn("event_date", F.to_date("event_ts"))
    .withColumn("event_hour", F.date_trunc("hour", F.col("event_ts")))
    .withColumn("quantity", F.coalesce(F.col("quantity"), F.lit(0)))
    .withColumn("price", F.coalesce(F.col("price"), F.lit(0.0)))
    .withColumn("gross_amount", F.round(F.col("quantity") * F.col("price"), 2))
)
print(type(normalized))  # still just a DataFrame wrapping a logical plan — no job has run

# COMMAND ----------
# MAGIC %md
# MAGIC Calling `.count()` is an **action**: it triggers a job. Open the Spark UI (Databricks: cluster ->
# MAGIC "Spark UI" tab) and look at the Jobs/Stages page while this cell runs — you'll see one job with one stage
# MAGIC (narrow transformations only, no shuffle).

# COMMAND ----------

print("Row count (triggers a job):", normalized.count())

# COMMAND ----------
# MAGIC %md
# MAGIC ## Narrow vs wide: broadcast join (narrow) vs groupBy (wide)
# MAGIC
# MAGIC `products_df` is tiny (50 rows), so Spark (via AQE, or explicitly via `F.broadcast`) sends the *whole*
# MAGIC table to every executor instead of shuffling the large `events` table. This turns what would otherwise be a
# MAGIC wide join into a narrow one — no shuffle for the join itself.
# MAGIC
# MAGIC The subsequent `groupBy` **is** wide: rows with the same grouping key can live on any executor, so Spark
# MAGIC must shuffle data so each key's rows land on one executor before aggregating.

# COMMAND ----------

enriched = normalized.join(F.broadcast(products_df), "product_id", "left")

revenue_by_category_hour = (
    enriched
    .filter(F.col("event_type").isin("purchase", "checkout"))
    .groupBy("event_hour", "category")
    .agg(F.count("*").alias("orders"), F.round(F.sum("gross_amount"), 2).alias("revenue"))
)

revenue_by_category_hour.explain("formatted")

# COMMAND ----------
# MAGIC %md
# MAGIC Read the plan above bottom-up. You should see:
# MAGIC
# MAGIC - A `BroadcastHashJoin` (no shuffle for the join — one side was small enough to broadcast).
# MAGIC - A `HashAggregate` -> `Exchange` (shuffle) -> `HashAggregate` pair around the `groupBy`. Spark
# MAGIC   pre-aggregates partially on each executor (the first `HashAggregate`) *before* shuffling, then
# MAGIC   finishes the aggregation after the shuffle (the second `HashAggregate`) — this is a map-side
# MAGIC   combine, the same trick MapReduce combiners use, and it drastically cuts shuffle volume.

# COMMAND ----------

display(revenue_by_category_hour.orderBy(F.desc("revenue")).limit(20))

# COMMAND ----------
# MAGIC %md
# MAGIC ## Partition count and file sizing
# MAGIC
# MAGIC `spark.sql.shuffle.partitions` controls how many partitions a shuffle produces. The default (200) is
# MAGIC usually wrong for small clusters/datasets — it creates many tiny tasks and tiny output files. AQE's
# MAGIC `coalescePartitions` feature fixes this automatically at runtime in modern Spark, but it's important to
# MAGIC understand the knob.

# COMMAND ----------

print("Current shuffle partitions:", spark.conf.get("spark.sql.shuffle.partitions"))
spark.conf.set("spark.sql.shuffle.partitions", "8")
print("Set to 8 for this small demo dataset. In production, size this to (cluster core count) x 2-3, "
      "or leave AQE to coalesce automatically.")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Writing partitioned output (on-disk partitioning)
# MAGIC
# MAGIC `partitionBy("event_date")` writes one directory per date. A downstream reader that filters on
# MAGIC `event_date` can skip entire directories (partition pruning) without reading their data at all.

# COMMAND ----------

(
    normalized.join(F.broadcast(products_df), "product_id", "left")
    .join(customers_df, "user_id", "left")
    .write.mode("overwrite")
    .partitionBy("event_date")
    .format("delta")
    .saveAsTable(table("class_batch_bronze_events"))
)

display(spark.sql(f"DESCRIBE DETAIL {table('class_batch_bronze_events')}"))

# COMMAND ----------
# MAGIC %md
# MAGIC ## Common batch performance problems (talking points)
# MAGIC
# MAGIC 1. **Too many small files** — usually caused by over-partitioning on write or many small streaming
# MAGIC    micro-batches. Fix with `OPTIMIZE` (Delta) or by reducing output partition count.
# MAGIC 2. **Data skew** — one join/grouping key has far more rows than others, so one task takes far longer than
# MAGIC    the rest. AQE's skew join optimization splits the largest partitions automatically; salting keys is the
# MAGIC    manual fix when AQE isn't enough.
# MAGIC 3. **Wrong join strategy** — broadcasting a table that's actually large causes executor OOMs; failing to
# MAGIC    broadcast a genuinely small table causes an unnecessary shuffle. Check `explain()` to confirm.
# MAGIC 4. **Repeated scans** — reading the same source multiple times instead of caching an intermediate result
# MAGIC    that's reused. Cache deliberately, and `unpersist()` when done.
# MAGIC 5. **Missing predicate/column pushdown** — reading Parquet/Delta with a `filter` before a `select` lets
# MAGIC    Spark skip files/row-groups and unused columns entirely at the source. `explain()` shows `PushedFilters`.
# MAGIC
# MAGIC ## What's next
# MAGIC
# MAGIC Batch processing assumes the data is already fully available. `class/02_spark_structured_streaming.py`
# MAGIC covers what changes when data arrives continuously and you cannot wait for "all of it."

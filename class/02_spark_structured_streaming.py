# Databricks notebook source
# MAGIC %md
# MAGIC # Class 2 — Spark Structured Streaming
# MAGIC
# MAGIC This notebook explains how Spark processes **unbounded** data — streams that never "finish" — and how that
# MAGIC changes the programming model, correctness guarantees, and operational concerns compared to batch.
# MAGIC
# MAGIC ## 1. The core idea: "streaming as an unbounded table"
# MAGIC
# MAGIC Structured Streaming's foundational trick is to treat a stream as a table that keeps growing:
# MAGIC
# MAGIC ```text
# MAGIC Batch mental model:            Streaming mental model:
# MAGIC
# MAGIC   [ fixed table ]                [ input table, rows keep arriving ]
# MAGIC         |                              row1
# MAGIC   run query once                       row2
# MAGIC         |                              row3  <- new micro-batch appends here
# MAGIC   [ result ]                           ...
# MAGIC                                   query re-runs incrementally on each micro-batch,
# MAGIC                                   producing/appending/updating a result table
# MAGIC ```
# MAGIC
# MAGIC You write the **same DataFrame/SQL transformations** you'd write for batch. The engine is what's different:
# MAGIC instead of running once, it repeatedly (a) reads new data since the last checkpoint, (b) applies the
# MAGIC transformations incrementally, and (c) writes/updates the result to a sink.
# MAGIC
# MAGIC ## 2. Micro-batch vs continuous processing
# MAGIC
# MAGIC - **Micro-batch** (the default, and what this project uses): Spark runs a small batch job every trigger
# MAGIC   interval. Latency is bounded by the trigger interval (commonly hundreds of ms to seconds/minutes).
# MAGIC   Supports the full DataFrame API, joins, aggregations, and all sinks.
# MAGIC - **Continuous processing** (experimental, narrow operator support): true low-latency (~1ms) processing
# MAGIC   without discrete batches. Rarely used in production Databricks pipelines — micro-batch is the practical
# MAGIC   default, and this project uses it exclusively.
# MAGIC
# MAGIC ## 3. Triggers
# MAGIC
# MAGIC - `trigger(processingTime="30 seconds")` — fixed-interval micro-batches; a true "always on" stream.
# MAGIC - `trigger(availableNow=True)` — process everything currently available then **stop**. This turns a
# MAGIC   streaming query into a finite job, which is why our production notebooks use it for
# MAGIC   scheduled/batch-like streaming runs (cheaper, and Databricks Jobs can just re-trigger on a schedule
# MAGIC   instead of paying for an always-on cluster).
# MAGIC - `trigger(once=True)` — legacy single micro-batch; superseded by `availableNow`.
# MAGIC
# MAGIC ## 4. Output modes
# MAGIC
# MAGIC - **Append** — only new result rows since the last trigger are emitted. Required for most sinks (Delta,
# MAGIC   Kafka). Works for non-aggregated data, and for aggregations *with* a watermark.
# MAGIC - **Update** — only rows whose aggregate value changed are emitted. Useful for sinks that support
# MAGIC   upsert (e.g. via `foreachBatch` + MERGE).
# MAGIC - **Complete** — the entire result table is re-emitted every trigger. Only viable for small aggregated
# MAGIC   results (e.g. a dashboard-sized summary table).
# MAGIC
# MAGIC ## 5. Event time, watermarks, and late data
# MAGIC
# MAGIC Two different clocks matter in streaming:
# MAGIC
# MAGIC - **Event time** — when the event actually happened (a timestamp field in the payload).
# MAGIC - **Processing time** — when Spark received/processed the event.
# MAGIC
# MAGIC Network delays, retries, and buffering mean events don't arrive in event-time order. If you window by event
# MAGIC time (e.g. "revenue per 5-minute window"), Spark needs a rule for **when it's safe to stop waiting** for
# MAGIC more late data for a given window and finalize/emit it. That rule is the **watermark**:
# MAGIC
# MAGIC ```text
# MAGIC withWatermark("event_ts", "10 minutes")
# MAGIC ```
# MAGIC
# MAGIC This tells Spark: "once I've seen an event with timestamp T, assume I will never see an event more than
# MAGIC 10 minutes older than T. I can drop/finalize state for windows older than (max event time seen - 10m)."
# MAGIC
# MAGIC It is a **tradeoff knob**, not a filter:
# MAGIC
# MAGIC - Too short -> legitimately late events get dropped, windows are "wrong."
# MAGIC - Too long -> Spark keeps more state in memory for longer, and results lag further behind wall-clock time.
# MAGIC
# MAGIC ## 6. State, checkpoints, and fault tolerance
# MAGIC
# MAGIC - **State** — data Spark must remember between micro-batches (e.g. partial aggregates per window,
# MAGIC   dedup keys). Stored in the executors' state store, backed by the checkpoint location.
# MAGIC - **Checkpoint** — a directory (must be durable storage — S3/DBFS, never local disk) storing (a) source
# MAGIC   read progress (e.g. Kafka offsets consumed), (b) the state store, and (c) sink commit metadata.
# MAGIC   On restart, Spark resumes exactly where the checkpoint says it left off. **Never delete or share a
# MAGIC   production checkpoint** unless you intend to reset/reprocess the stream from scratch.
# MAGIC - Correctness end to end also depends on the **sink** being idempotent or transactional for the
# MAGIC   at-least-once-delivery guarantee to become effectively-exactly-once. Delta Lake's transactional writes
# MAGIC   plus streaming checkpoint offsets are what make Kafka -> Spark -> Delta exactly-once in practice.
# MAGIC
# MAGIC ## 7. Stateful operations at a glance
# MAGIC
# MAGIC | Operation | Needs watermark? | Why |
# MAGIC |---|---|---|
# MAGIC | `groupBy(window(...))` aggregation | Yes, to bound state and allow append mode | Otherwise Spark keeps every window's state forever |
# MAGIC | `dropDuplicates(["id"])` on a stream | Yes | Otherwise Spark must remember every id ever seen |
# MAGIC | Stream-stream join | Yes (on both sides) | Both sides buffer unmatched rows until the watermark expires them |
# MAGIC | Stream-static join (e.g. enrich with a dimension table) | No | The static side has no "state" to expire |
# MAGIC
# MAGIC Below, we build a small runnable streaming pipeline using Spark's `rate` source (a synthetic, always-available
# MAGIC generator) so these concepts can be observed without needing Kafka/MSK yet — that's covered in
# MAGIC `class/03_kafka_and_msk.py`.

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


def checkpoint(name: str) -> str:
    return path("checkpoints", name)


spark.sql(f"CREATE CATALOG IF NOT EXISTS `{catalog}`")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{catalog}`.`{schema}`")
spark.sql(f"CREATE VOLUME IF NOT EXISTS `{catalog}`.`{schema}`.`{volume}`")
spark.sql(f"USE CATALOG `{catalog}`")
spark.sql(f"USE SCHEMA `{schema}`")
print(f"catalog={catalog}, schema={schema}, volume={volume}, base_path={base_path}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Step 1 — A streaming source
# MAGIC
# MAGIC `rate` emits rows with a monotonically increasing `value` and a `timestamp`, at a controlled rate. We shape
# MAGIC it into fake clickstream events so the mechanics look identical to a real Kafka payload.

# COMMAND ----------

raw_stream = (
    spark.readStream.format("rate")
    .option("rowsPerSecond", 50)
    .option("numPartitions", 2)
    .load()
)

events_stream = raw_stream.select(
    F.concat(F.lit("evt-"), F.col("value")).alias("event_id"),
    F.col("timestamp").alias("event_ts"),
    F.concat(F.lit("u"), F.lpad((F.col("value") % 200).cast("string"), 5, "0")).alias("user_id"),
    F.concat(F.lit("p"), F.lpad(((F.col("value") % 50) + 1).cast("string"), 4, "0")).alias("product_id"),
    F.when((F.col("value") % 10) == 0, "purchase").otherwise("view").alias("event_type"),
    F.when((F.col("value") % 10) == 0, 1).otherwise(0).cast("int").alias("quantity"),
    F.when((F.col("value") % 10) == 0, 29.99).otherwise(0.0).cast("double").alias("price"),
)

print("Is streaming:", events_stream.isStreaming)

# COMMAND ----------
# MAGIC %md
# MAGIC ## Step 2 — Stateless streaming write (append mode, no aggregation)
# MAGIC
# MAGIC This is the simplest streaming query: no state, no watermark needed. Every micro-batch is just filtered
# MAGIC and appended.

# COMMAND ----------

bronze_query = (
    events_stream
    .withColumn("_ingest_ts", F.current_timestamp())
    .writeStream
    .format("delta")
    .option("checkpointLocation", checkpoint("class_streaming_bronze"))
    .outputMode("append")
    .trigger(availableNow=True)
    .toTable(table("class_streaming_bronze"))
)

bronze_query.awaitTermination()
print("AvailableNow bronze streaming run completed")
display(spark.table(table("class_streaming_bronze")).orderBy(F.desc("event_ts")).limit(10))

# COMMAND ----------
# MAGIC %md
# MAGIC Inspect the query's recent progress. `numInputRows`, `inputRowsPerSecond`, and `durationMs` are the metrics
# MAGIC you'd wire into monitoring/alerting in production (see `class/04` and the production notebook
# MAGIC `09_observability_testing_performance.py`).

# COMMAND ----------

import json
for p in bronze_query.recentProgress[-3:]:
    print(json.dumps({k: p[k] for k in ("timestamp", "numInputRows", "inputRowsPerSecond", "durationMs")}, indent=2))

# COMMAND ----------
# MAGIC %md
# MAGIC ## Step 3 — Stateful streaming: watermark + windowed aggregation + de-duplication
# MAGIC
# MAGIC Now we read the bronze table *as a stream* (Delta tables can be both streaming sinks and streaming sources)
# MAGIC and compute a windowed aggregate with a watermark, plus de-duplicate by `event_id`.

# COMMAND ----------

bronze_stream = spark.readStream.table(table("class_streaming_bronze"))

# Keep watermark + dedup in PySpark because it is the most portable streaming API.
deduped = (
    bronze_stream
    .withWatermark("event_ts", "2 minutes")
    .dropDuplicates(["event_id"])
)

# Register the streaming DataFrame, then express the business aggregation in Spark SQL.
deduped.createOrReplaceTempView("deduped_bronze_stream")

windowed_revenue = spark.sql("""
SELECT
  window(event_ts, '30 seconds') AS window,
  product_id,
  COUNT(*) AS orders,
  ROUND(SUM(quantity * price), 2) AS revenue
FROM deduped_bronze_stream
WHERE event_type = 'purchase'
GROUP BY
  window(event_ts, '30 seconds'),
  product_id
""")

gold_query = (
    windowed_revenue.writeStream
    .format("delta")
    .option("checkpointLocation", checkpoint("class_streaming_gold_windows"))
    .outputMode("append")  # append mode is legal because watermark is defined above
    .trigger(availableNow=True)
    .toTable(table("class_streaming_gold_windows"))
)
gold_query.awaitTermination()

display(spark.table(table("class_streaming_gold_windows")).orderBy(F.desc("window.start")).limit(20))

# COMMAND ----------
# MAGIC %md
# MAGIC ## Step 4 — foreachBatch: escaping to batch logic per micro-batch
# MAGIC
# MAGIC Some things aren't expressible as a pure streaming DataFrame op — most commonly, an **upsert (MERGE)**
# MAGIC into a dimensional/gold table so re-running or late-arriving corrections update existing rows instead of
# MAGIC duplicating them. `foreachBatch` hands you a regular (batch) DataFrame for each micro-batch, which you can
# MAGIC run *any* batch code against, including `DeltaTable.merge`.

# COMMAND ----------

# Serverless-friendly pattern: first materialize a deduplicated silver stream, then run SQL MERGE as batch.
silver_table = table("class_streaming_silver_deduped")
gold_table = table("class_streaming_gold_upsert")

silver_query = (
    spark.readStream.table(table("class_streaming_bronze"))
    .withWatermark("event_ts", "2 minutes")
    .dropDuplicates(["event_id"])
    .writeStream
    .format("delta")
    .option("checkpointLocation", checkpoint("class_streaming_silver_deduped"))
    .outputMode("append")
    .trigger(availableNow=True)
    .toTable(silver_table)
)
silver_query.awaitTermination()

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {gold_table} (
  window_start TIMESTAMP,
  product_id STRING,
  orders BIGINT,
  revenue DOUBLE
)
USING DELTA
""")

spark.sql(f"""
MERGE INTO {gold_table} AS t
USING (
  SELECT
    window(event_ts, '30 seconds').start AS window_start,
    product_id,
    COUNT(*) AS orders,
    ROUND(SUM(quantity * price), 2) AS revenue
  FROM {silver_table}
  WHERE event_type = 'purchase'
  GROUP BY
    window(event_ts, '30 seconds').start,
    product_id
) AS s
ON t.window_start = s.window_start
   AND t.product_id = s.product_id
WHEN MATCHED THEN UPDATE SET
  t.orders = s.orders,
  t.revenue = s.revenue
WHEN NOT MATCHED THEN INSERT (
  window_start,
  product_id,
  orders,
  revenue
)
VALUES (
  s.window_start,
  s.product_id,
  s.orders,
  s.revenue
)
""")

display(spark.table(gold_table).orderBy(F.desc("revenue")).limit(20))

# COMMAND ----------
# MAGIC %md
# MAGIC ## Common streaming pitfalls (talking points)
# MAGIC
# MAGIC 1. **No watermark on a stateful op** — Spark keeps state forever; memory grows unbounded until the job dies.
# MAGIC 2. **Deleting/moving a checkpoint** — the stream loses its offset/state history; you will either reprocess
# MAGIC    everything from the source's earliest offset (duplicates downstream unless the sink is idempotent) or
# MAGIC    silently skip data, depending on the source's default starting offset.
# MAGIC 3. **Non-deterministic transformations upstream of aggregation** — e.g. relying on wall-clock time inside a
# MAGIC    UDF — makes replay produce different results than the original run.
# MAGIC 4. **Output mode mismatch** — trying `append` mode on an aggregation with no watermark raises an
# MAGIC    `AnalysisException`; this is Spark protecting you from an unbounded/undefined result table.
# MAGIC 5. **Assuming exactly-once "just happens"** — it requires (a) a source that can replay from a durable
# MAGIC    offset (Kafka can; a plain socket cannot), (b) a checkpoint, and (c) an idempotent/transactional sink
# MAGIC    (Delta's transaction log makes `toTable`/`foreachBatch`+MERGE idempotent per checkpoint+batch id).
# MAGIC
# MAGIC ## What's next
# MAGIC
# MAGIC The `rate` source is a teaching convenience. Real systems need a durable, replayable, ordered log that many
# MAGIC producers and consumers can share — that's exactly what Kafka/MSK provides, and what
# MAGIC `class/03_kafka_and_msk.py` covers next.

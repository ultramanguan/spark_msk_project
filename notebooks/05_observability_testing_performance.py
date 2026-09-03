# Databricks notebook source
# MAGIC %md
# MAGIC > Retail Lakehouse — Production Pipeline (AWS Databricks + MSK + S3 + Delta)

# COMMAND ----------

try:
    dbutils.widgets.text("catalog", "main", "Unity Catalog catalog")
    dbutils.widgets.text("schema", "retail_lakehouse", "Schema/database")
    dbutils.widgets.text("base_path", "", "Optional override; blank uses /Volumes/<catalog>/<schema>/raw")
except Exception:
    pass

from pyspark.sql import functions as F

catalog = dbutils.widgets.get("catalog") if "dbutils" in globals() else "main"
schema = dbutils.widgets.get("schema") if "dbutils" in globals() else "retail_lakehouse"
base_path = dbutils.widgets.get("base_path") if "dbutils" in globals() else ""
if not base_path:
    base_path = f"/Volumes/{catalog}/{schema}/raw"

from retail_lakehouse.config import PipelineConfig
cfg = PipelineConfig(catalog=catalog, schema=schema, base_path=base_path)
spark.sql(f"USE CATALOG `{cfg.catalog}`")
spark.sql(f"USE SCHEMA `{cfg.schema}`")

# MAGIC %md
# MAGIC # 05 — Observability, testing, and performance tuning
# MAGIC
# MAGIC **Objective:** teach the operational practices a production on-call engineer actually uses: streaming
# MAGIC query metrics, table history/detail, query plans, and data quality checks.

# COMMAND ----------
# MAGIC %md
# MAGIC ## Streaming query metrics
# MAGIC
# MAGIC Every `StreamingQuery` exposes `.lastProgress` / `.recentProgress` with metrics you should alert on:
# MAGIC
# MAGIC - `numInputRows` / `inputRowsPerSecond` — throughput; a sudden drop can mean an upstream (MSK) problem.
# MAGIC - `durationMs` per phase (`addBatch`, `getBatch`, `queryPlanning`) — where time is actually spent.
# MAGIC - `sources[].endOffset` vs the topic's actual latest offset — this delta is **consumer lag**. Growing lag
# MAGIC   means the pipeline can't keep up with the input rate.
# MAGIC - `stateOperators[].numRowsTotal` — state store size; unbounded growth usually means a missing/too-long
# MAGIC   watermark or a `dropDuplicates` key that never converges.
# MAGIC
# MAGIC In production, ship these metrics via a `StreamingQueryListener` to CloudWatch/Datadog/Prometheus rather
# MAGIC than reading them interactively. A minimal listener:
# MAGIC
# MAGIC ```python
# MAGIC from pyspark.sql.streaming import StreamingQueryListener
# MAGIC
# MAGIC class MetricsListener(StreamingQueryListener):
# MAGIC     def onQueryProgress(self, event):
# MAGIC         p = event.progress
# MAGIC         # push p.numInputRows, p.inputRowsPerSecond, p.durationMs.get("addBatch") to your metrics sink
# MAGIC         pass
# MAGIC     def onQueryStarted(self, event): pass
# MAGIC     def onQueryTerminated(self, event): pass
# MAGIC
# MAGIC spark.streams.addListener(MetricsListener())
# MAGIC ```

# COMMAND ----------
# MAGIC %md
# MAGIC ## Table history and storage detail

# COMMAND ----------

for tbl in ["bronze_clickstream_kafka", "silver_clickstream_streaming", "gold_revenue_windows_streaming"]:
    try:
        print("==", tbl, "history ==")
        display(spark.sql(f"DESCRIBE HISTORY {cfg.table(tbl)}"))
        print("==", tbl, "detail ==")
        display(spark.sql(f"DESCRIBE DETAIL {cfg.table(tbl)}"))
    except Exception as e:
        print(tbl, "not available yet:", str(e)[:200])

# COMMAND ----------
# MAGIC %md
# MAGIC ## Query plan and tuning levers
# MAGIC
# MAGIC Look for scan filters (`PushedFilters`), broadcast exchanges, sort-merge joins, shuffle exchanges, and
# MAGIC skew hints in `explain("formatted")` output.

# COMMAND ----------

if spark.catalog.tableExists(f"{cfg.catalog}.{cfg.schema}.silver_clickstream_batch"):
    q = spark.table(cfg.table("silver_clickstream_batch")).groupBy("event_type").count()
    q.explain("formatted")
    display(q)

# COMMAND ----------
# MAGIC %md
# MAGIC ## Data quality summary

# COMMAND ----------

from retail_lakehouse.quality import quality_summary, assert_no_duplicate_keys

if spark.catalog.tableExists(f"{cfg.catalog}.{cfg.schema}.silver_clickstream_batch"):
    display(quality_summary(spark.table(cfg.table("silver_clickstream_batch"))))
    assert_no_duplicate_keys(spark.table(cfg.table("silver_clickstream_batch")), ["event_id"])
    print("No duplicate event_id in silver_clickstream_batch")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Production tuning and operations checklist
# MAGIC
# MAGIC - Start with a correct data model and partition strategy; fix the model before tuning code.
# MAGIC - Keep file sizes healthy (target ~128MB-1GB per file); `OPTIMIZE` after ingestion bursts, not every
# MAGIC   micro-batch.
# MAGIC - Use broadcast joins only for genuinely small dimensions; check `explain()` to confirm the plan Spark
# MAGIC   actually picked, don't assume.
# MAGIC - Trust AQE for partition coalescing and skew joins; only hand-tune `spark.sql.shuffle.partitions` when
# MAGIC   AQE's defaults are measurably wrong for your workload.
# MAGIC - Cache only reused intermediate data, and `unpersist()` when done.
# MAGIC - Size streaming state and watermarks deliberately — measure actual event lateness in your data before
# MAGIC   picking a watermark delay.
# MAGIC - Alert on consumer lag and state store size growth, not just job failure/success.
# MAGIC - Never delete a production streaming checkpoint casually — it is the source of truth for "what has been
# MAGIC   processed."
# MAGIC
# MAGIC ## Next
# MAGIC
# MAGIC `06_capstone_end_to_end.py` runs the full pipeline as a single validated flow — this is also what
# MAGIC `resources/jobs.yml` orchestrates as a scheduled Databricks Job.

# Databricks notebook source
# MAGIC %md
# MAGIC > Retail Lakehouse — Production Pipeline (AWS Databricks + MSK + S3 + Delta)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Ensure `retail_lakehouse` is importable
# MAGIC
# MAGIC No-op when this runs as part of the deployed job (the wheel is already installed via
# MAGIC `resources/jobs.yml`'s `environments` block). When running this notebook interactively, installs the
# MAGIC wheel from the current user's `dev`-target bundle deployment. Placed before any other state is set up
# MAGIC so that the `dbutils.library.restartPython()` below (needed for the newly installed package to be
# MAGIC importable) has nothing to lose.

# COMMAND ----------

try:
    import retail_lakehouse  # noqa: F401
except ModuleNotFoundError:
    _user = spark.sql("SELECT current_user()").first()[0]
    _wheel = f"/Workspace/Users/{_user}/.bundle/retail_lakehouse/dev/files/dist/retail_lakehouse-0.1.0-py3-none-any.whl"
    get_ipython().run_line_magic("pip", f"install {_wheel}")
    dbutils.library.restartPython()

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
# MAGIC # 03 — Streaming silver/gold with watermarking, de-dup, and idempotent upsert
# MAGIC
# MAGIC **Objective:** consume `bronze_clickstream_kafka` (written by notebook 02) as a stream, produce a
# MAGIC watermarked/deduplicated silver stream, enrich with dimensions, and upsert windowed revenue into gold
# MAGIC using `foreachBatch` + `MERGE` for idempotent, replay-safe writes.
# MAGIC
# MAGIC If notebook 02 hasn't been run yet (no MSK available), this notebook still runs against whatever bronze
# MAGIC table exists — including the file/rate-based fallback in `notebooks/08_file_rate_streaming_fallback.py`
# MAGIC if you point `bronze_table` at it via the widget below.

# COMMAND ----------

try:
    dbutils.widgets.text("bronze_table", "bronze_clickstream_kafka", "Bronze table to stream from")
except Exception:
    pass
bronze_table_name = dbutils.widgets.get("bronze_table") if "dbutils" in globals() else "bronze_clickstream_kafka"

if not spark.catalog.tableExists(f"{cfg.catalog}.{cfg.schema}.{bronze_table_name}"):
    raise ValueError(
        f"{bronze_table_name} does not exist yet. Run 02_kafka_msk_streaming_ingest.py (or the "
        "08_file_rate_streaming_fallback.py fallback and point the bronze_table widget at its output table) first."
    )

# COMMAND ----------
# MAGIC %md
# MAGIC ## Silver: watermark, de-duplicate, normalize, enrich

# COMMAND ----------

from retail_lakehouse.streaming import deduplicate_stream
from retail_lakehouse.transformations import normalize_click_events, enrich_with_product_customer

bronze_stream = spark.readStream.table(cfg.table(bronze_table_name))

deduped_stream = deduplicate_stream(bronze_stream, watermark_col="event_ts", watermark_delay="10 minutes")
normalized_stream = normalize_click_events(deduped_stream)

products = spark.table(cfg.table("dim_product_seed"))
customers = spark.table(cfg.table("dim_customer_seed"))
silver_stream = enrich_with_product_customer(normalized_stream, products, customers)

silver_query = (
    silver_stream.writeStream
    .format("delta")
    .option("checkpointLocation", cfg.checkpoint("silver_clickstream_streaming"))
    .outputMode("append")
    .trigger(availableNow=True)
    .toTable(cfg.table("silver_clickstream_streaming"))
)
silver_query.awaitTermination()
print("Silver rows:", spark.table(cfg.table("silver_clickstream_streaming")).count())

# COMMAND ----------
# MAGIC %md
# MAGIC ## Gold: windowed revenue via idempotent MERGE upsert
# MAGIC
# MAGIC `make_gold_upsert` (from `retail_lakehouse.streaming`) builds a `foreachBatch` function that MERGEs each
# MAGIC micro-batch's aggregated rows by `(window_start, category)`. If a micro-batch is ever replayed (e.g. after
# MAGIC a job retry from the last committed checkpoint offset), re-running it updates the same gold rows instead of
# MAGIC double-counting revenue — this is what "effectively-exactly-once" means in practice for a streaming
# MAGIC aggregation sink.

# COMMAND ----------

from retail_lakehouse.streaming import windowed_revenue, make_gold_upsert

gold_table = cfg.table("gold_revenue_windows_streaming")
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {gold_table} (
  window STRUCT<start: TIMESTAMP, end: TIMESTAMP>,
  category STRING,
  orders LONG,
  revenue DOUBLE
) USING DELTA
""")

silver_for_gold = (
    spark.readStream.table(cfg.table("silver_clickstream_streaming"))
    .withWatermark("event_ts", "10 minutes")
)
windowed = windowed_revenue(silver_for_gold, window_duration="5 minutes", watermark_delay="10 minutes")

upsert_fn = make_gold_upsert(
    target_table=gold_table,
    merge_keys=["window", "category"],
    update_cols=["orders", "revenue"],
)

gold_query = (
    windowed.writeStream
    .foreachBatch(upsert_fn)
    .option("checkpointLocation", cfg.checkpoint("gold_revenue_windows_streaming"))
    .trigger(availableNow=True)
    .start()
)
gold_query.awaitTermination()

display(spark.table(gold_table).orderBy(F.desc("window.start"), F.desc("revenue")).limit(20))

# COMMAND ----------
# MAGIC %md
# MAGIC ## Verify idempotency
# MAGIC
# MAGIC Re-running the gold query against already-processed offsets (simulated here by restarting from the same
# MAGIC checkpoint) should not change row counts or revenue totals, because MERGE keys on `(window, category)`.
# MAGIC Try re-running the previous cell now — row count and revenue sums should be identical.

# COMMAND ----------

before = spark.table(gold_table).agg(F.sum("revenue")).first()[0]
print("Total gold revenue (should stay identical across reruns of the previous cell):", before)

# COMMAND ----------
# MAGIC %md
# MAGIC ## Next
# MAGIC
# MAGIC `04_kafka_output_sink.py` demonstrates the reverse direction: writing an alerting/notification stream
# MAGIC back out to a Kafka/MSK topic. `07_observability_testing_performance.py` covers monitoring these queries
# MAGIC in production.

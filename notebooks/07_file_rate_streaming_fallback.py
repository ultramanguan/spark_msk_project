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
spark.sql(f"CREATE CATALOG IF NOT EXISTS `{cfg.catalog}`")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{cfg.catalog}`.`{cfg.schema}`")
spark.sql(f"CREATE VOLUME IF NOT EXISTS `{cfg.catalog}`.`{cfg.schema}`.`raw`")
spark.sql(f"USE CATALOG `{cfg.catalog}`")
spark.sql(f"USE SCHEMA `{cfg.schema}`")

# MAGIC %md
# MAGIC # 07 — File/rate streaming fallback (no MSK required)
# MAGIC
# MAGIC **Objective:** exercise the exact same streaming mechanics as `02_kafka_msk_streaming_ingest.py` and
# MAGIC `03_streaming_silver_gold_delta.py`, without requiring a live MSK cluster. Use this while `infra/terraform`
# MAGIC is being provisioned, or for a classroom/demo run with no AWS networking dependency.
# MAGIC
# MAGIC This writes to `bronze_clickstream_rate`. Point notebook 03's `bronze_table` widget at
# MAGIC `bronze_clickstream_rate` to run the rest of the pipeline against this fallback source.

# COMMAND ----------

stream_events = (
    spark.readStream.format("rate")
    .option("rowsPerSecond", 50)
    .option("numPartitions", 2)
    .load()
    .select(
        F.concat(F.lit("rate-"), F.col("value")).alias("event_id"),
        F.col("timestamp").alias("event_ts"),
        F.concat(F.lit("u"), F.lpad((F.col("value") % 200).cast("string"), 5, "0")).alias("user_id"),
        F.concat(F.lit("s"), F.lpad((F.col("value") % 50).cast("string"), 5, "0")).alias("session_id"),
        F.concat(F.lit("p"), F.lpad(((F.col("value") % 50) + 1).cast("string"), 4, "0")).alias("product_id"),
        F.when((F.col("value") % 10) == 0, "purchase").when((F.col("value") % 5) == 0, "add_to_cart").otherwise("view").alias("event_type"),
        F.when((F.col("value") % 10) == 0, 1).otherwise(0).cast("int").alias("quantity"),
        F.when((F.col("value") % 10) == 0, 19.99).otherwise(0.0).cast("double").alias("price"),
        F.lit("product").alias("page"),
        F.lit("rate_source").alias("user_agent"),
    )
)

from retail_lakehouse.transformations import add_ingest_metadata
bronze_stream = add_ingest_metadata(stream_events, "rate_source_fallback")

query = (
    bronze_stream.writeStream
    .format("delta")
    .option("checkpointLocation", cfg.checkpoint("rate_bronze_clickstream"))
    .outputMode("append")
    .trigger(availableNow=True)
    .toTable(cfg.table("bronze_clickstream_rate"))
)

query.awaitTermination()
print("AvailableNow fallback stream completed")
display(spark.table(cfg.table("bronze_clickstream_rate")).orderBy(F.desc("event_ts")).limit(20))

# COMMAND ----------
# MAGIC %md
# MAGIC ## Next
# MAGIC
# MAGIC Run `03_streaming_silver_gold_delta.py` with the `bronze_table` widget set to
# MAGIC `bronze_clickstream_rate` to build silver/gold from this fallback source using the same package code that
# MAGIC will run against real MSK data.

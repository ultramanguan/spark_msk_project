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
try:
    current_user = spark.sql("SELECT current_user()").first()[0].replace("@", "_").replace(".", "_")
except Exception:
    current_user = "local_user"
if not base_path:
    base_path = f"/Volumes/{catalog}/{schema}/raw"

from retail_lakehouse.config import PipelineConfig
cfg = PipelineConfig(catalog=catalog, schema=schema, base_path=base_path)
spark.sql(f"USE CATALOG `{cfg.catalog}`")
spark.sql(f"USE SCHEMA `{cfg.schema}`")

# MAGIC %md
# MAGIC # 01 — Batch lakehouse: bronze / silver / gold
# MAGIC
# MAGIC **Objective:** build the medallion pipeline from the batch seed files created in notebook 00, using
# MAGIC the shared `retail_lakehouse` transformation functions (the exact same functions the streaming path in
# MAGIC notebook 03 reuses for silver enrichment). This is the baseline to compare the streaming path against.

# COMMAND ----------

from retail_lakehouse.transformations import (
    normalize_click_events, filter_valid_events, deduplicate_events,
    enrich_with_product_customer, revenue_by_hour,
)

raw_events = spark.read.json(cfg.path("source", "events_json"))
products = spark.table(cfg.table("dim_product_seed"))
customers = spark.table(cfg.table("dim_customer_seed"))

print("Raw event rows:", raw_events.count())

# COMMAND ----------
# MAGIC %md
# MAGIC ## Bronze — minimally transformed, ingestion metadata only

# COMMAND ----------

from retail_lakehouse.transformations import add_ingest_metadata

bronze = add_ingest_metadata(raw_events, "batch_file_seed")
bronze.write.mode("overwrite").format("delta").partitionBy("_ingest_date").saveAsTable(cfg.table("bronze_clickstream_batch"))
display(spark.table(cfg.table("bronze_clickstream_batch")).limit(5))

# COMMAND ----------
# MAGIC %md
# MAGIC ## Silver — normalized, validated, deduplicated

# COMMAND ----------

silver = deduplicate_events(filter_valid_events(normalize_click_events(bronze)))
silver.write.mode("overwrite").format("delta").partitionBy("event_date").saveAsTable(cfg.table("silver_clickstream_batch"))
print("Silver rows:", spark.table(cfg.table("silver_clickstream_batch")).count())

# COMMAND ----------
# MAGIC %md
# MAGIC ## Gold — enriched with dimensions, aggregated to a business metric

# COMMAND ----------

enriched = enrich_with_product_customer(spark.table(cfg.table("silver_clickstream_batch")), products, customers)
gold = revenue_by_hour(enriched.withColumn(
    "is_purchase", F.col("event_type").isin("purchase", "checkout")
))
gold.write.mode("overwrite").format("delta").saveAsTable(cfg.table("gold_revenue_by_hour_batch"))

display(spark.table(cfg.table("gold_revenue_by_hour_batch")).orderBy(F.desc("revenue")))

# COMMAND ----------
# MAGIC %md
# MAGIC ## Query plan check
# MAGIC
# MAGIC Confirm the product join broadcasts (small dimension) while the aggregation shuffles (expected for
# MAGIC `groupBy`). See `class/01_spark_batch_processing.py` for the full explanation of this plan shape.

# COMMAND ----------

enriched.explain("formatted")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Next: the same medallion shape, built by streaming from MSK
# MAGIC
# MAGIC `02_kafka_msk_streaming_ingest.py` ingests the same kind of event from a live MSK topic instead of a
# MAGIC static file, and `03_streaming_silver_gold_delta.py` reuses `normalize_click_events` /
# MAGIC `enrich_with_product_customer` again — the transformation code doesn't care whether it's fed by batch or
# MAGIC streaming, only the orchestration around it differs.

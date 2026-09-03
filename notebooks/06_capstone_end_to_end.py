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
# MAGIC # 06 — Capstone: end-to-end lakehouse pipeline
# MAGIC
# MAGIC **Objective:** run the full batch + streaming medallion pipeline as a single flow and validate outputs.
# MAGIC This mirrors what `resources/jobs.yml` runs as a scheduled Databricks Job in `infra`/CI-CD.

# COMMAND ----------

from retail_lakehouse.generate import synthetic_products, synthetic_customers, synthetic_events
from retail_lakehouse.transformations import (
    normalize_click_events, filter_valid_events, deduplicate_events,
    enrich_with_product_customer, revenue_by_hour, add_ingest_metadata,
)
import random
random.seed(11)

# COMMAND ----------
# MAGIC %md
# MAGIC ## Seed + batch bronze/silver/gold

# COMMAND ----------

products = spark.createDataFrame(synthetic_products(50))
customers = spark.createDataFrame(synthetic_customers(200))
events = spark.createDataFrame(list(synthetic_events(5000)))

products.write.mode("overwrite").format("delta").saveAsTable(cfg.table("dim_product_seed"))
customers.write.mode("overwrite").format("delta").saveAsTable(cfg.table("dim_customer_seed"))

bronze = add_ingest_metadata(events, "capstone_batch")
silver = deduplicate_events(filter_valid_events(normalize_click_events(bronze)))
silver.write.mode("overwrite").format("delta").partitionBy("event_date").saveAsTable(cfg.table("capstone_silver_events"))

enriched = enrich_with_product_customer(silver, products, customers)
gold = revenue_by_hour(enriched.withColumn("is_purchase", F.col("event_type").isin("purchase", "checkout")))
gold.write.mode("overwrite").format("delta").saveAsTable(cfg.table("capstone_gold_revenue"))

# COMMAND ----------
# MAGIC %md
# MAGIC ## Validate outputs
# MAGIC
# MAGIC A real Databricks Job task would fail the job (raise) on a failed assertion, which is exactly what this
# MAGIC cell does — that's the mechanism `resources/jobs.yml`'s `capstone` task relies on for pipeline health.

# COMMAND ----------

from retail_lakehouse.quality import assert_no_duplicate_keys

assert spark.table(cfg.table("capstone_silver_events")).count() > 0, "silver produced no rows"
assert spark.table(cfg.table("capstone_gold_revenue")).count() > 0, "gold produced no rows"
assert_no_duplicate_keys(spark.table(cfg.table("capstone_silver_events")), ["event_id"])

display(spark.table(cfg.table("capstone_gold_revenue")).orderBy(F.desc("revenue")))
print("Capstone pipeline validated successfully.")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Discussion questions
# MAGIC
# MAGIC 1. Which parts of this pipeline should move from `availableNow` scheduled streaming to a true always-on
# MAGIC    continuous stream, and what would that cost in cluster spend vs latency improvement?
# MAGIC 2. Where should MSK credentials live, and how would you rotate them without downtime?
# MAGIC 3. Which tables should be governed as Delta vs Iceberg if a second (non-Databricks) query engine joins
# MAGIC    this platform?
# MAGIC 4. How would you test a schema change to the clickstream event contract without breaking consumers?
# MAGIC 5. What SLOs would you define for streaming lag and gold table freshness, and how would you alert on them?
# MAGIC 6. If MSK ingestion falls behind for an hour then catches up, what do you need to verify about the gold
# MAGIC    table afterward?

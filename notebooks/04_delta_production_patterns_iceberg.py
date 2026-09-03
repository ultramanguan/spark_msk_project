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
# MAGIC # 04 — Delta Lake production patterns and Iceberg comparison
# MAGIC
# MAGIC **Objective:** demonstrate `DESCRIBE HISTORY`, `MERGE`, time travel, `OPTIMIZE`/`ZORDER`, `VACUUM`, and a
# MAGIC managed Iceberg table for comparison, all against the tables built in notebooks 01-03.

# COMMAND ----------

from delta.tables import DeltaTable

# COMMAND ----------
# MAGIC %md
# MAGIC ## Table history

# COMMAND ----------

display(spark.sql(f"DESCRIBE HISTORY {cfg.table('silver_clickstream_batch')}"))

# COMMAND ----------
# MAGIC %md
# MAGIC ## MERGE upsert into a customer dimension (CDC-style update)

# COMMAND ----------

spark.table(cfg.table("dim_customer_seed")).write.mode("overwrite").format("delta").saveAsTable(cfg.table("dim_customer_current"))

updates = spark.createDataFrame([
    ("u00001", "loyal", "west", 1704067200),
    ("u99999", "new", "central", 1735689600),
], "user_id string, segment string, region string, signup_epoch long")

delta_t = DeltaTable.forName(spark, cfg.table("dim_customer_current"))
(delta_t.alias("t")
 .merge(updates.alias("s"), "t.user_id = s.user_id")
 .whenMatchedUpdateAll()
 .whenNotMatchedInsertAll()
 .execute())

display(spark.table(cfg.table("dim_customer_current")).filter("user_id in ('u00001','u99999')"))

# COMMAND ----------
# MAGIC %md
# MAGIC ## Time travel

# COMMAND ----------

history = spark.sql(f"DESCRIBE HISTORY {cfg.table('dim_customer_current')}")
versions = [r.version for r in history.select("version").collect()]
if len(versions) >= 2:
    earliest = min(versions)
    print("Reading version", earliest, "(before the MERGE):")
    display(spark.read.option("versionAsOf", earliest).table(cfg.table("dim_customer_current")).limit(10))

# COMMAND ----------
# MAGIC %md
# MAGIC ## OPTIMIZE + ZORDER and VACUUM
# MAGIC
# MAGIC `OPTIMIZE`/`ZORDER` require a Databricks Runtime cluster. `VACUUM` physically deletes files no longer
# MAGIC referenced by the log and older than the retention threshold (default 7 days) — run it on a schedule, not
# MAGIC ad hoc, and never with a retention below 7 days on a table with concurrent readers/time-travel users.

# COMMAND ----------

try:
    spark.sql(f"OPTIMIZE {cfg.table('silver_clickstream_batch')} ZORDER BY (user_id, product_id)")
except Exception as e:
    print("OPTIMIZE requires Databricks Runtime/permissions:", str(e)[:300])

display(spark.sql(f"DESCRIBE DETAIL {cfg.table('silver_clickstream_batch')}"))

# COMMAND ----------
# MAGIC %md
# MAGIC ## Managed Iceberg table (Unity Catalog) for comparison
# MAGIC
# MAGIC Falls back to a Delta comparison table if the workspace/runtime doesn't support managed Iceberg tables yet.

# COMMAND ----------

iceberg_table = cfg.table("iceberg_clickstream_sample")
fallback_delta = cfg.table("delta_iceberg_comparison_sample")
source = spark.table(cfg.table("silver_clickstream_batch")).limit(200)

try:
    spark.sql(f"DROP TABLE IF EXISTS {iceberg_table}")
    spark.sql(f"""
    CREATE TABLE {iceberg_table} (
      event_id STRING, event_ts TIMESTAMP, user_id STRING, product_id STRING,
      event_type STRING, event_date DATE
    ) USING ICEBERG
    """)
    (source.select("event_id", "event_ts", "user_id", "product_id", "event_type", "event_date")
          .write.mode("append").saveAsTable(iceberg_table))
    print("Created managed Iceberg table:", iceberg_table)
except Exception as e:
    print("Managed Iceberg not available in this workspace, falling back to Delta:", str(e)[:300])
    (source.select("event_id", "event_ts", "user_id", "product_id", "event_type", "event_date")
          .write.mode("overwrite").format("delta").saveAsTable(fallback_delta))

# COMMAND ----------
# MAGIC %md
# MAGIC ## Next
# MAGIC
# MAGIC `05_observability_testing_performance.py` covers monitoring these tables and streaming queries in
# MAGIC production, and `06_capstone_end_to_end.py` ties the whole pipeline together in one job.

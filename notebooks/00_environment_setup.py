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

print(f"catalog={cfg.catalog}, schema={cfg.schema}, base_path={cfg.base_path}")

# MAGIC %md
# MAGIC # 00 — Environment setup
# MAGIC
# MAGIC **Objective:** create the Unity Catalog schema, generate seed source files (products, customers,
# MAGIC clickstream events), and validate that the `retail_lakehouse` wheel is importable.
# MAGIC
# MAGIC In a Databricks Asset Bundle job, the wheel is installed as a task library (see
# MAGIC `resources/jobs.yml`). In an interactive Repos-backed notebook without the wheel installed, the import
# MAGIC below will fail with a clear message telling you to install it (`%pip install -e .` from the repo root, or
# MAGIC attach the built wheel to the cluster).

# COMMAND ----------

from retail_lakehouse.transformations import normalize_click_events  # noqa: F401 -- import validates package availability
from retail_lakehouse.generate import synthetic_products, synthetic_customers, synthetic_events, write_jsonl
print("retail_lakehouse package imported successfully")

# COMMAND ----------

spark.sql(f"CREATE CATALOG IF NOT EXISTS `{cfg.catalog}`")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{cfg.catalog}`.`{cfg.schema}`")
spark.sql(f"CREATE VOLUME IF NOT EXISTS `{cfg.catalog}`.`{cfg.schema}`.`raw`")
spark.sql(f"USE CATALOG `{cfg.catalog}`")
spark.sql(f"USE SCHEMA `{cfg.schema}`")
spark.conf.set("spark.sql.shuffle.partitions", "8")
print("Spark version:", spark.version)

# COMMAND ----------
# MAGIC %md
# MAGIC ## Generate seed data
# MAGIC
# MAGIC Products and customers are batch dimension data (as if loaded from an OLTP export). Events simulate what
# MAGIC will otherwise arrive continuously from MSK in `02_kafka_msk_streaming_ingest.py` — having a batch copy
# MAGIC lets us also demonstrate the pure-batch path for comparison in `01_batch_lakehouse_bronze_silver_gold.py`.

# COMMAND ----------

import random
random.seed(42)

product_rows = synthetic_products(50)
customer_rows = synthetic_customers(200)
event_rows = list(synthetic_events(2000))

product_df = spark.createDataFrame(product_rows)
customer_df = spark.createDataFrame(customer_rows)
event_df = spark.createDataFrame(event_rows)

product_df.write.mode("overwrite").format("delta").saveAsTable(cfg.table("dim_product_seed"))
customer_df.write.mode("overwrite").format("delta").saveAsTable(cfg.table("dim_customer_seed"))
event_df.write.mode("overwrite").json(cfg.path("source", "events_json"))
product_df.write.mode("overwrite").option("header", True).csv(cfg.path("source", "products_csv"))
customer_df.write.mode("overwrite").option("header", True).csv(cfg.path("source", "customers_csv"))

print("Created seed tables and files")
display(event_df.limit(10))

# COMMAND ----------
# MAGIC %md
# MAGIC ## Tables created so far

# COMMAND ----------

display(spark.sql(f"SHOW TABLES IN `{cfg.catalog}`.`{cfg.schema}`"))

# COMMAND ----------
# MAGIC %md
# MAGIC ## Next steps
# MAGIC
# MAGIC - `01_batch_lakehouse_bronze_silver_gold.py` — batch path from the seed files.
# MAGIC - `02_kafka_msk_streaming_ingest.py` — real MSK ingestion (requires `infra/terraform` provisioned; see
# MAGIC   `RUNBOOK.md`).

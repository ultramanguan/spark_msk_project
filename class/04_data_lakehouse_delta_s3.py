# Databricks notebook source
# MAGIC %md
# MAGIC # Class 4 — The Data Lakehouse: S3, Delta Lake, and the Medallion Architecture
# MAGIC
# MAGIC ## 1. Why raw files on S3 aren't enough
# MAGIC
# MAGIC A data lake is "just files in object storage" (S3, ADLS, GCS). That's cheap and infinitely scalable, but
# MAGIC plain Parquet/CSV/JSON files on S3 give you none of the guarantees a database gives you:
# MAGIC
# MAGIC - **No atomicity** — if a job writing 500 Parquet files crashes after writing 300, readers can see a
# MAGIC   half-written, inconsistent dataset.
# MAGIC - **No isolation** — a reader running concurrently with a writer can see a mix of old and new files.
# MAGIC - **No schema enforcement** — nothing stops a bad job from writing a column with the wrong type or an extra
# MAGIC   column, silently corrupting downstream consumers.
# MAGIC - **No update/delete** — object storage is append/overwrite-oriented; there's no efficient way to update
# MAGIC   or delete a subset of rows without rewriting entire files/partitions.
# MAGIC - **No versioning/audit trail** — you can't easily ask "what did this table look like an hour ago" or "who
# MAGIC   changed this row and when."
# MAGIC
# MAGIC The **lakehouse** pattern layers a **table format** (a transaction log/metadata layer) on top of plain files
# MAGIC in object storage, giving you database-like guarantees (ACID transactions, schema enforcement, time travel)
# MAGIC while keeping data lake economics (cheap object storage, open file formats, any engine can read it).
# MAGIC Delta Lake, Apache Iceberg, and Apache Hudi are the three major table formats; this project uses **Delta
# MAGIC Lake** because it's the Databricks-native default.
# MAGIC
# MAGIC ## 2. How Delta Lake actually works
# MAGIC
# MAGIC A Delta table is: (a) a directory of Parquet data files, plus (b) a `_delta_log/` directory of JSON (and
# MAGIC periodic Parquet checkpoint) files describing every change ever made to the table.
# MAGIC
# MAGIC ```text
# MAGIC s3://bucket/tables/silver_clickstream/
# MAGIC ├── _delta_log/
# MAGIC │   ├── 00000000000000000000.json   <- version 0: CREATE TABLE + initial files
# MAGIC │   ├── 00000000000000000001.json   <- version 1: added files from an append
# MAGIC │   ├── 00000000000000000002.json   <- version 2: a MERGE (some files removed, new files added)
# MAGIC │   └── ...
# MAGIC ├── part-00000-....snappy.parquet
# MAGIC ├── part-00001-....snappy.parquet
# MAGIC └── ...
# MAGIC ```
# MAGIC
# MAGIC Each log entry is a set of **actions**: `add` a file, `remove` a file, change metadata, etc. A reader
# MAGIC determines "what does this table look like right now" by replaying the log from the last checkpoint forward
# MAGIC and computing the *current* set of active files — this is what gives you **snapshot isolation**: a query
# MAGIC always sees a single consistent version, never a half-written state, because a new version only becomes
# MAGIC visible when its full commit JSON is atomically written.
# MAGIC
# MAGIC This design is what enables:
# MAGIC
# MAGIC - **ACID transactions** — a write either fully commits (one new log entry) or doesn't happen at all.
# MAGIC - **Time travel** — `SELECT * FROM t VERSION AS OF 5` or `TIMESTAMP AS OF '2026-01-01'` just replays the
# MAGIC   log up to that point.
# MAGIC - **Schema enforcement/evolution** — the log's metadata action records the schema; writes that don't match
# MAGIC   are rejected unless you explicitly opt into evolution (`mergeSchema`).
# MAGIC - **MERGE (upsert)** — expressed as one transaction that removes affected old files and adds new files
# MAGIC   with updated rows, atomically.
# MAGIC - **Streaming source + sink from the same table** — because "new data since version N" is a
# MAGIC   well-defined, log-derived concept, a Delta table can be tailed as a stream, not just queried as a batch
# MAGIC   table.
# MAGIC
# MAGIC ## 3. The medallion architecture (bronze / silver / gold)
# MAGIC
# MAGIC This project (and most production lakehouses) organizes tables into three progressively refined layers:
# MAGIC
# MAGIC ```text
# MAGIC   Kafka/MSK,          BRONZE                 SILVER                  GOLD
# MAGIC   batch files    -->  raw, append-only, -->  cleaned, deduped,  -->  business-level
# MAGIC                       schema-on-write,       validated,             aggregates,
# MAGIC                       minimal transform      enriched with          dimensional models,
# MAGIC                                              dimensions             ready for BI/ML
# MAGIC ```
# MAGIC
# MAGIC - **Bronze** — as close to the source as possible, plus ingestion metadata (`_ingest_ts`, `_source`).
# MAGIC   Kept append-only so you can always reprocess silver/gold from bronze if downstream logic changes or has a
# MAGIC   bug. This is your "reprocessing insurance."
# MAGIC - **Silver** — validated (bad rows filtered/quarantined), deduplicated, type-cast, enriched with reference
# MAGIC   data (joins against dimension tables). This is the layer most internal engineering consumers query.
# MAGIC - **Gold** — aggregated, business-metric tables shaped for a specific consumption pattern (a dashboard, an
# MAGIC   ML feature table, a report). Usually small and heavily optimized for read performance.
# MAGIC
# MAGIC Each layer can be written by **either batch or streaming jobs** — the medallion pattern is orthogonal to
# MAGIC batch vs streaming. This project's production pipeline demonstrates bronze/silver/gold built by streaming
# MAGIC jobs (from MSK) *and* by batch jobs (from files), landing in the same schema, to make that point concrete.
# MAGIC
# MAGIC ## 4. Delta Lake operational patterns you'll use constantly
# MAGIC
# MAGIC - `MERGE INTO` — the standard upsert pattern for CDC ingestion, slowly-changing dimensions, and streaming
# MAGIC   `foreachBatch` writes to gold tables (see class 2, step 4).
# MAGIC - `OPTIMIZE table ZORDER BY (col1, col2)` — compacts small files into larger ones and co-locates rows with
# MAGIC   similar values in `col1`/`col2` so range/filter queries on those columns skip more data. Run periodically
# MAGIC   after ingestion bursts, not on every micro-batch.
# MAGIC - `VACUUM table` — physically deletes data files no longer referenced by the log and older than the
# MAGIC   retention threshold (default 7 days). Needed because time travel/`remove` actions don't delete files
# MAGIC   immediately — until vacuumed, old versions remain queryable.
# MAGIC - `DESCRIBE HISTORY table` — every version, its operation type, and metrics. Your audit trail.
# MAGIC
# MAGIC ## 5. Delta vs Iceberg (why you'll hear both names)
# MAGIC
# MAGIC | | Delta Lake | Apache Iceberg |
# MAGIC |---|---|---|
# MAGIC | Native engine | Databricks (also OSS, multi-engine via Delta Standalone/UniForm) | Multi-engine by design (Spark, Trino, Flink, Snowflake, etc.) |
# MAGIC | Metadata layout | `_delta_log` JSON commits + periodic Parquet checkpoints | metadata.json + manifest lists + manifests (finer-grained snapshotting) |
# MAGIC | Databricks default | Yes | Supported via Unity Catalog managed tables where enabled |
# MAGIC | When to choose | Databricks-centric platform, want the deepest feature/performance integration | Strong multi-engine interoperability requirement (e.g. Trino + Spark + Snowflake all writing/reading the same tables) |
# MAGIC
# MAGIC They solve the *same* underlying problem (transactional metadata over object storage) with different
# MAGIC metadata designs. A mature platform sometimes runs both, with a clear standard for which team/workload uses
# MAGIC which. This project defaults to Delta everywhere since it targets Databricks, and demonstrates a managed
# MAGIC Iceberg table for comparison purposes in the production notebooks.
# MAGIC
# MAGIC Below, a compact runnable walkthrough of the concepts above.

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


spark.sql(f"CREATE CATALOG IF NOT EXISTS `{catalog}`")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{catalog}`.`{schema}`")
spark.sql(f"CREATE VOLUME IF NOT EXISTS `{catalog}`.`{schema}`.`{volume}`")
spark.sql(f"USE CATALOG `{catalog}`")
spark.sql(f"USE SCHEMA `{schema}`")

# COMMAND ----------
# MAGIC %md
# MAGIC ## ACID + schema enforcement

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, DoubleType

dim_customer = spark.createDataFrame(
    [("u00001", "loyal", "west"), ("u00002", "new", "east")],
    "user_id string, segment string, region string",
)
dim_customer.write.mode("overwrite").format("delta").saveAsTable(table("class_lakehouse_dim_customer"))

try:
    bad_schema = spark.createDataFrame([("u00003", 42)], "user_id string, segment int")  # wrong type for segment
    bad_schema.write.mode("append").format("delta").saveAsTable(table("class_lakehouse_dim_customer"))
except Exception as e:
    print("Rejected as expected — Delta enforces schema on write:")
    print(type(e).__name__, str(e)[:300])

# COMMAND ----------
# MAGIC %md
# MAGIC ## MERGE (upsert) — the core dimensional/CDC pattern

# COMMAND ----------

from delta.tables import DeltaTable

updates = spark.createDataFrame(
    [("u00001", "loyal", "central"), ("u00003", "active", "south")],
    "user_id string, segment string, region string",
)
target = DeltaTable.forName(spark, table("class_lakehouse_dim_customer"))
(
    target.alias("t")
    .merge(updates.alias("s"), "t.user_id = s.user_id")
    .whenMatchedUpdateAll()
    .whenNotMatchedInsertAll()
    .execute()
)
display(spark.table(table("class_lakehouse_dim_customer")))

# COMMAND ----------
# MAGIC %md
# MAGIC ## Time travel

# COMMAND ----------

history = spark.sql(f"DESCRIBE HISTORY {table('class_lakehouse_dim_customer')}")
display(history)

earliest_version = history.select(F.min("version")).first()[0]
print(f"Table as of version {earliest_version} (before the MERGE):")
display(spark.read.option("versionAsOf", earliest_version).table(table("class_lakehouse_dim_customer")))

# COMMAND ----------
# MAGIC %md
# MAGIC ## OPTIMIZE and file layout

# COMMAND ----------

try:
    spark.sql(f"OPTIMIZE {table('class_lakehouse_dim_customer')} ZORDER BY (region)")
except Exception as e:
    print("OPTIMIZE requires a Databricks Runtime cluster; error if run elsewhere:", str(e)[:200])

display(spark.sql(f"DESCRIBE DETAIL {table('class_lakehouse_dim_customer')}"))

# COMMAND ----------
# MAGIC %md
# MAGIC ## Discussion / what's next
# MAGIC
# MAGIC You've now seen every concept the production pipeline uses:
# MAGIC
# MAGIC - Batch DataFrame processing and Spark's execution model (class 1).
# MAGIC - Structured Streaming, watermarks, and stateful aggregation (class 2).
# MAGIC - Kafka/MSK as the durable ingestion log and its schema-parsing contract with Spark (class 3).
# MAGIC - Delta Lake as the transactional table format that makes bronze/silver/gold reliable (class 4).
# MAGIC
# MAGIC Move to `RUNBOOK.md` and `notebooks/00_environment_setup.py` to build the real, end-to-end retail
# MAGIC clickstream pipeline: S3 + MSK + Databricks + Delta, with production Python packaging and CI/CD.

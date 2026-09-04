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
    dbutils.widgets.text("kafka_bootstrap_servers", "", "MSK bootstrap broker string, e.g. b-1.xxx.kafka.us-east-1.amazonaws.com:9098")
    dbutils.widgets.text("kafka_topic", "retail-clickstream", "MSK topic name")
    dbutils.widgets.dropdown("kafka_auth_mode", "IAM", ["IAM", "SASL_SCRAM", "PLAINTEXT_DEV_ONLY"], "MSK auth mode")
    dbutils.widgets.text("starting_offsets", "earliest", "earliest or latest")
except Exception:
    pass

from pyspark.sql import functions as F

catalog = dbutils.widgets.get("catalog") if "dbutils" in globals() else "main"
schema = dbutils.widgets.get("schema") if "dbutils" in globals() else "retail_lakehouse"
base_path = dbutils.widgets.get("base_path") if "dbutils" in globals() else ""
if not base_path:
    base_path = f"/Volumes/{catalog}/{schema}/raw"

bootstrap = dbutils.widgets.get("kafka_bootstrap_servers") if "dbutils" in globals() else ""
topic = dbutils.widgets.get("kafka_topic") if "dbutils" in globals() else "retail-clickstream"
auth_mode = dbutils.widgets.get("kafka_auth_mode") if "dbutils" in globals() else "IAM"
starting_offsets = dbutils.widgets.get("starting_offsets") if "dbutils" in globals() else "earliest"

from retail_lakehouse.config import PipelineConfig
cfg = PipelineConfig(catalog=catalog, schema=schema, base_path=base_path,
                      kafka_bootstrap_servers=bootstrap, kafka_topic=topic)
spark.sql(f"USE CATALOG `{cfg.catalog}`")
spark.sql(f"USE SCHEMA `{cfg.schema}`")

# MAGIC %md
# MAGIC # 02 — Kafka/MSK streaming ingest
# MAGIC
# MAGIC **Objective:** read clickstream events from a live Amazon MSK topic with Spark Structured Streaming and
# MAGIC land them as an append-only bronze Delta table on S3.
# MAGIC
# MAGIC **Requires:**
# MAGIC
# MAGIC 1. An MSK cluster reachable from the Databricks VPC (see `infra/terraform/`).
# MAGIC 2. A topic (default `retail-clickstream`) — created via `scripts/create_msk_topics.sh` (see `RUNBOOK.md`
# MAGIC    step 2) or manually.
# MAGIC 3. Producer traffic on that topic — run `scripts/seed_kafka_topic.py` to produce synthetic events, or
# MAGIC    point a real producer at it.
# MAGIC 4. IAM auth is the default (recommended for MSK + Databricks on AWS): the cluster needs the
# MAGIC    `aws-msk-iam-auth` library and an instance profile with `kafka-cluster:*` MSK IAM policy actions
# MAGIC    scoped to this cluster/topic. See `infra/terraform/iam.tf` for the exact IAM policy JSON, and
# MAGIC    `RUNBOOK.md` step 2 for how to attach it to your cluster.
# MAGIC
# MAGIC If you don't have MSK provisioned yet, this notebook will raise a clear error rather than silently doing
# MAGIC nothing — use `notebooks/01_batch_lakehouse_bronze_silver_gold.py` to keep working on the rest of the
# MAGIC pipeline while infra is being set up.

# COMMAND ----------

if not bootstrap:
    raise ValueError(
        "Set the kafka_bootstrap_servers widget to your MSK bootstrap broker string before running this "
        "notebook. Get it with: aws kafka get-bootstrap-brokers --cluster-arn <arn>"
    )

# COMMAND ----------
# MAGIC %md
# MAGIC ## Configure the Kafka source
# MAGIC
# MAGIC The `kafka.*`-prefixed options are passed straight through to the underlying Kafka consumer client.
# MAGIC `failOnDataLoss=false` is appropriate for training/dev where topics may be trimmed by retention; in a
# MAGIC strict-correctness production pipeline you'd set this `true` and alert instead of silently skipping.

# COMMAND ----------

reader = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", bootstrap)
    .option("subscribe", topic)
    .option("startingOffsets", starting_offsets)
    .option("failOnDataLoss", "false")
    .option("maxOffsetsPerTrigger", 10000)
)

if auth_mode == "IAM":
    reader = (
        reader
        .option("kafka.security.protocol", "SASL_SSL")
        .option("kafka.sasl.mechanism", "AWS_MSK_IAM")
        .option("kafka.sasl.jaas.config", "software.amazon.msk.auth.iam.IAMLoginModule required;")
        .option("kafka.sasl.client.callback.handler.class", "software.amazon.msk.auth.iam.IAMClientCallbackHandler")
    )
elif auth_mode == "SASL_SCRAM":
    scram_username = dbutils.secrets.get(scope="retail-lakehouse", key="msk-scram-username")
    scram_password = dbutils.secrets.get(scope="retail-lakehouse", key="msk-scram-password")
    reader = (
        reader
        .option("kafka.security.protocol", "SASL_SSL")
        .option("kafka.sasl.mechanism", "SCRAM-SHA-512")
        .option("kafka.sasl.jaas.config",
                f'org.apache.kafka.common.security.scram.ScramLoginModule required username="{scram_username}" password="{scram_password}";')
    )
else:
    print("WARNING: PLAINTEXT_DEV_ONLY selected. Only use this against a local/dev broker, never production MSK.")

raw_kafka = reader.load()

# COMMAND ----------
# MAGIC %md
# MAGIC ## Parse and write to bronze
# MAGIC
# MAGIC `parse_kafka_value` (from `retail_lakehouse.transformations`) is unit-tested in
# MAGIC `tests/test_kafka_parsing.py` against a synthetic Kafka-shaped DataFrame, so this logic is verified in CI
# MAGIC without needing a live broker.

# COMMAND ----------

from retail_lakehouse.transformations import parse_kafka_value, add_ingest_metadata

parsed = parse_kafka_value(raw_kafka)
bronze_stream = add_ingest_metadata(parsed, "kafka_msk")

query = (
    bronze_stream.writeStream
    .format("delta")
    .option("checkpointLocation", cfg.checkpoint("kafka_bronze_clickstream"))
    .outputMode("append")
    .trigger(availableNow=True)
    .toTable(cfg.table("bronze_clickstream_kafka"))
)
query.awaitTermination()

print("Micro-batch(es) complete. Query progress:")
for p in query.recentProgress[-5:]:
    print({"timestamp": p["timestamp"], "numInputRows": p["numInputRows"], "durationMs": p["durationMs"]})

display(spark.table(cfg.table("bronze_clickstream_kafka")).orderBy(F.desc("_ingest_ts")).limit(20))

# COMMAND ----------
# MAGIC %md
# MAGIC ## Running this continuously in production
# MAGIC
# MAGIC For a scheduled job (this project's default — see `resources/jobs.yml`), `trigger(availableNow=True)`
# MAGIC processes everything currently in the topic then stops, and the Databricks Job schedule re-triggers it
# MAGIC (e.g. every 5 minutes). This avoids paying for an always-on cluster and gives you natural
# MAGIC restart/backoff/alerting via Databricks Jobs.
# MAGIC
# MAGIC For true low-latency (seconds), switch to `trigger(processingTime="30 seconds")` and run this notebook as
# MAGIC a continuous (non-`availableNow`) job task on an always-on job cluster; monitor with the metrics printed
# MAGIC above and the checks in `notebooks/07_observability_testing_performance.py`.
# MAGIC
# MAGIC ## Next
# MAGIC
# MAGIC `03_streaming_silver_gold_delta.py` reads `bronze_clickstream_kafka` as a stream and builds the
# MAGIC silver/gold layers with watermarking, de-duplication, and an idempotent upsert into gold.

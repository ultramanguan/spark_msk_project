# Databricks notebook source
# MAGIC %md
# MAGIC # Class 3 — Kafka and Amazon MSK
# MAGIC
# MAGIC This notebook explains **what Kafka is, why it exists, how Amazon MSK fits in, and how Spark Structured
# MAGIC Streaming reads/writes it**. It is conceptual first; a runnable local demo follows using a filesystem stand-in
# MAGIC (real MSK connectivity requires network/broker setup covered in `infra/terraform/` and the production
# MAGIC notebook `notebooks/02_kafka_msk_streaming_ingest.py`).
# MAGIC
# MAGIC ## 1. What problem Kafka solves
# MAGIC
# MAGIC Before event streaming platforms, systems integrated point-to-point: service A calls service B's API, or
# MAGIC writes directly to B's database. This doesn't scale past a handful of systems (N systems need up to
# MAGIC N*(N-1) integrations) and couples producers to consumers' availability.
# MAGIC
# MAGIC Kafka decouples producers from consumers with a **durable, ordered, replayable log**:
# MAGIC
# MAGIC ```text
# MAGIC  Producers                     Kafka topic (durable log)                 Consumers
# MAGIC  ---------                     --------------------------                ---------
# MAGIC  web app        -----\                                          /-----> Spark Structured Streaming
# MAGIC  mobile app     ------>  [ partition 0: msg0 msg1 msg2 msg3 ... ] ------> fraud detection service
# MAGIC  IoT device     -----/    [ partition 1: msg0 msg1 msg2 ...     ] \-----> analytics dashboard
# MAGIC                           [ partition 2: msg0 msg1 ...          ]
# MAGIC ```
# MAGIC
# MAGIC Producers write once; any number of independent consumers can read the same data, each at their own pace,
# MAGIC each remembering their own position (offset). Kafka retains data for a configured period (or forever, with
# MAGIC compaction) regardless of whether anyone has consumed it yet — consumers can be slow, offline, or replay
# MAGIC from the beginning without affecting producers or other consumers.
# MAGIC
# MAGIC ## 2. Core concepts
# MAGIC
# MAGIC - **Topic** — a named stream of events (e.g. `retail-clickstream`).
# MAGIC - **Partition** — a topic is split into ordered, append-only partitions. Order is guaranteed *within* a
# MAGIC   partition, not across partitions of the same topic. Partitions are the unit of parallelism for both
# MAGIC   producers and consumers.
# MAGIC - **Offset** — the position of a message within a partition. Consumers track "what offset have I processed
# MAGIC   up to" per partition.
# MAGIC - **Broker** — a Kafka server that stores partitions and serves reads/writes. A cluster has multiple
# MAGIC   brokers.
# MAGIC - **Replication factor** — each partition is copied to N brokers for durability; if the leader broker for a
# MAGIC   partition dies, a replica is promoted with no data loss (for acknowledged writes).
# MAGIC - **Consumer group** — a set of consumers that split a topic's partitions between them so the topic's
# MAGIC   total throughput is processed collectively (each partition is read by exactly one consumer within a
# MAGIC   given group at a time).
# MAGIC - **Key** — an optional per-message key. Messages with the same key always land in the same partition
# MAGIC   (via a hash of the key), which is how you get ordering guarantees *per entity* (e.g. all events for one
# MAGIC   `user_id` are ordered relative to each other).
# MAGIC
# MAGIC ## 3. Delivery/ordering guarantees that matter for Spark integration
# MAGIC
# MAGIC - Kafka guarantees **at-least-once** delivery to consumers by default; a consumer (or Spark) that fails
# MAGIC   after reading but before committing its offset will re-read those messages on restart.
# MAGIC - Spark Structured Streaming turns this into **effectively-exactly-once** end to end by (a) tracking
# MAGIC   consumed offsets in its own checkpoint (not relying on Kafka consumer group offset commits), and
# MAGIC   (b) writing to an idempotent/transactional sink (Delta Lake) keyed by checkpoint + batch id, so replaying
# MAGIC   the same offset range after a failure produces the same table state, not duplicates.
# MAGIC - Ordering is only guaranteed **within a partition**. If cross-event ordering matters for an entity (e.g.
# MAGIC   "process this user's events in order"), that entity's `user_id` must be the partition key.
# MAGIC
# MAGIC ## 4. Amazon MSK (Managed Streaming for Apache Kafka)
# MAGIC
# MAGIC MSK is AWS's managed Kafka service: AWS runs and patches the brokers, handles broker replacement, and
# MAGIC integrates with VPC networking, IAM, and CloudWatch. Two flavors matter here:
# MAGIC
# MAGIC - **MSK Provisioned** — you choose broker instance type/count/storage; you're billed per broker-hour.
# MAGIC   Predictable cost, more capacity planning.
# MAGIC - **MSK Serverless** — capacity scales automatically; billed per partition-hour and per GB
# MAGIC   in/out/retained. Simpler to provision for a training/demo project, which is why `infra/terraform/`
# MAGIC   defaults to it.
# MAGIC
# MAGIC What you provide either way:
# MAGIC
# MAGIC 1. **Networking** — MSK brokers live in your VPC/subnets; Databricks compute needs network reachability
# MAGIC    (VPC peering, PrivateLink, or same-VPC deployment) and the right security group rules on the broker
# MAGIC    port (9092 plaintext / 9094-9098 TLS/SASL/IAM depending on auth mode).
# MAGIC 2. **Authentication** — MSK supports IAM auth, SASL/SCRAM, mutual TLS, or plaintext (dev only). Production
# MAGIC    Databricks jobs should use IAM auth or SASL/SCRAM with credentials in Databricks secrets, never
# MAGIC    hardcoded in a notebook.
# MAGIC 3. **Topics** — created via the Kafka Admin API/CLI, Terraform (`infra/terraform/msk.tf` in this project),
# MAGIC    or auto-creation (not recommended in production — you lose control of partition count/replication).
# MAGIC
# MAGIC ## 5. Spark <-> Kafka: the actual read/write shape
# MAGIC
# MAGIC Regardless of Provisioned vs Serverless, Spark's `kafka` format always looks like this:
# MAGIC
# MAGIC ```python
# MAGIC raw = (
# MAGIC     spark.readStream.format("kafka")
# MAGIC     .option("kafka.bootstrap.servers", bootstrap_servers)   # broker addresses
# MAGIC     .option("subscribe", "retail-clickstream")               # topic(s)
# MAGIC     .option("startingOffsets", "earliest")                    # or "latest"
# MAGIC     .option("failOnDataLoss", "false")                        # tolerate retention-expired offsets in dev
# MAGIC     .load()
# MAGIC )
# MAGIC ```
# MAGIC
# MAGIC The resulting DataFrame always has this fixed schema — Kafka messages are opaque bytes, so *you* are
# MAGIC responsible for parsing `value`:
# MAGIC
# MAGIC | column | type | meaning |
# MAGIC |---|---|---|
# MAGIC | `key` | binary | producer-supplied partition key |
# MAGIC | `value` | binary | the actual message payload (commonly JSON or Avro) |
# MAGIC | `topic` | string | topic name |
# MAGIC | `partition` | int | partition number |
# MAGIC | `offset` | long | offset within the partition |
# MAGIC | `timestamp` | timestamp | broker-assigned or producer-assigned event timestamp |
# MAGIC | `timestampType` | int | 0 = create time, 1 = log append time |
# MAGIC
# MAGIC You cast `value` to string and parse it with `from_json(col, schema)` against a known schema (see
# MAGIC `src/retail_lakehouse/schemas.py`), exactly like the demo below.
# MAGIC
# MAGIC Writing to Kafka is symmetric: your DataFrame must have a `value` column (and optionally `key`/`topic`);
# MAGIC Spark serializes and produces it.
# MAGIC
# MAGIC ## 6. Runnable demo without live MSK
# MAGIC
# MAGIC A JSON-lines directory is a reasonable stand-in for teaching the *parsing and schema* half of Kafka
# MAGIC integration (the half that's actually project-specific code) without requiring a live broker. The
# MAGIC production notebook `notebooks/02_kafka_msk_streaming_ingest.py` uses the real `kafka` source against MSK;
# MAGIC this cell only demonstrates schema parsing and bronze-table semantics.

# COMMAND ----------

try:
    dbutils.widgets.text("catalog", "main", "Unity Catalog catalog")
    dbutils.widgets.text("schema", "retail_lakehouse", "Schema/database")
    dbutils.widgets.text("volume", "raw", "Unity Catalog volume")
    dbutils.widgets.text("base_path", "", "Optional override; blank uses /Volumes/<catalog>/<schema>/<volume>")
except Exception:
    pass

from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, IntegerType, DoubleType
import json
import random
from datetime import datetime, timedelta, timezone

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

# COMMAND ----------
# MAGIC %md
# MAGIC ## Step 1 — Simulate Kafka's on-wire shape
# MAGIC
# MAGIC We write JSON files that mimic Kafka `value` payloads (a real MSK payload would be the same JSON bytes,
# MAGIC just delivered via the `kafka` source instead of files).

# COMMAND ----------

click_event_schema = StructType([
    StructField("event_id", StringType(), False),
    StructField("event_ts", TimestampType(), False),
    StructField("user_id", StringType(), False),
    StructField("product_id", StringType(), True),
    StructField("event_type", StringType(), False),
    StructField("quantity", IntegerType(), True),
    StructField("price", DoubleType(), True),
])

random.seed(3)
start = datetime.now(timezone.utc) - timedelta(minutes=30)
rows = []
for i in range(500):
    event_type = random.choice(["view", "add_to_cart", "purchase", "search", "checkout"])
    qty = random.randint(1, 3) if event_type in ("purchase", "checkout") else None
    price = round(random.uniform(5, 200), 2) if qty else None
    rows.append({
        "event_id": f"kevt-{i:06d}",
        "event_ts": (start + timedelta(seconds=random.randint(0, 1800))).isoformat(),
        "user_id": f"u{random.randint(1, 200):05d}",
        "product_id": f"p{random.randint(1, 50):04d}",
        "event_type": event_type,
        "quantity": qty,
        "price": price,
    })

sim_topic_path = path("kafka_sim", "retail-clickstream")
dbutils.fs.mkdirs(sim_topic_path)
with open(f"{sim_topic_path}/batch1.jsonl", "w") as f:
    for r in rows:
        f.write(json.dumps(r) + "\n")
print("Wrote simulated topic files to", sim_topic_path)

# COMMAND ----------
# MAGIC %md
# MAGIC ## Step 2 — Parse the payload against a known schema (the part that's identical for real Kafka)
# MAGIC
# MAGIC `spark.readStream.format("json")` here stands in for `spark.readStream.format("kafka")`. Everything from
# MAGIC `from_json` onward is **exactly** what the real MSK notebook does to the `value` column.

# COMMAND ----------

raw_stream = (
    spark.readStream.format("json")
    .schema(StructType([StructField("json_payload", StringType())]))  # placeholder; real files are raw JSON lines
    .load(sim_topic_path)
) if False else (
    spark.readStream.format("text").load(sim_topic_path)
    .withColumnRenamed("value", "json_payload")
)

parsed = (
    raw_stream
    .withColumn("event", F.from_json(F.col("json_payload"), click_event_schema))
    .select("json_payload", "event.*")
    .withColumn("_ingest_ts", F.current_timestamp())
    .withColumn("_source", F.lit("kafka_msk_simulated"))
)

query = (
    parsed.writeStream
    .format("delta")
    .option("checkpointLocation", checkpoint("class_kafka_sim_bronze"))
    .outputMode("append")
    .trigger(availableNow=True)
    .toTable(table("class_kafka_sim_bronze"))
)
query.awaitTermination()

display(spark.table(table("class_kafka_sim_bronze")).orderBy(F.desc("event_ts")).limit(10))

# COMMAND ----------
# MAGIC %md
# MAGIC ## What differs when this points at real MSK
# MAGIC
# MAGIC Only the source block changes — the parsing/business logic is identical:
# MAGIC
# MAGIC ```python
# MAGIC raw_stream = (
# MAGIC     spark.readStream.format("kafka")
# MAGIC     .option("kafka.bootstrap.servers", bootstrap_servers)
# MAGIC     .option("subscribe", "retail-clickstream")
# MAGIC     .option("startingOffsets", "earliest")
# MAGIC     .option("kafka.security.protocol", "SASL_SSL")             # or SSL for mTLS, or omit for IAM+PLAINTEXT within VPC
# MAGIC     .option("kafka.sasl.mechanism", "AWS_MSK_IAM")
# MAGIC     .option("kafka.sasl.jaas.config",
# MAGIC             "software.amazon.msk.auth.iam.IAMLoginModule required;")
# MAGIC     .option("kafka.sasl.client.callback.handler.class",
# MAGIC             "software.amazon.msk.auth.iam.IAMClientCallbackHandler")
# MAGIC     .load()
# MAGIC     .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING) AS json_payload", "timestamp AS kafka_ts")
# MAGIC )
# MAGIC ```
# MAGIC
# MAGIC This requires the `aws-msk-iam-auth` library on the cluster (or the SASL/SCRAM equivalent with credentials
# MAGIC from a Databricks secret scope), and network reachability from the Databricks VPC to the MSK brokers'
# MAGIC subnets/security groups — see `infra/terraform/msk.tf` and `RUNBOOK.md` step 2.
# MAGIC
# MAGIC ## What's next
# MAGIC
# MAGIC `class/04_data_lakehouse_delta_s3.py` covers what happens on the *write* side once events are flowing:
# MAGIC why raw files on S3 aren't enough, and what Delta Lake adds.

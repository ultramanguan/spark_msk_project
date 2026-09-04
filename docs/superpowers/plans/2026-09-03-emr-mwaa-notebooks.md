# EMR Interactive Learning Notebooks (Plan 3 of 5) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Create `emr-notebooks/` — real Jupyter `.ipynb` files mirroring all 8 `notebooks/*.py` Databricks notebooks, for interactive step-by-step learning on Plan 1's persistent EMR JupyterHub cluster. `notebooks/` (Databricks) stays completely untouched.

**Architecture:** Each `.ipynb` is a direct port of the matching `notebooks/*.py`: no `dbutils.widgets` (plain variables in the first code cell), no wheel-install bootstrap cell (Plan 1's EMR bootstrap action already installed `retail_lakehouse`), no Unity Catalog `CREATE CATALOG/VOLUME` (`CREATE DATABASE ... LOCATION ...` against Glue, using the same S3-location pattern Plan 2's review established is required — Plan 2 hit and fixed a real bug where omitting this destroys every table when the cluster stops, and this plan must not repeat it), `.show()`/`.toPandas()` instead of Databricks' `display()`, and (for notebook `02`) plain `kafka.security.protocol=SASL_SSL`/`AWS_MSK_IAM` config instead of `databricks.serviceCredential` — EMR is classic EC2, so MSK IAM auth comes for free from the instance profile Plan 1's `iam.tf` already attaches, no service-credential indirection needed.

**Tech Stack:** Jupyter notebook format (`nbformat` 4), PySpark via EMR's Sparkmagic/Livy-backed `pysparkkernel`, `retail_lakehouse` package (unchanged).

## Global Constraints

- `notebooks/`, `class/`, `src/retail_lakehouse/*.py` are NOT touched by this plan.
- **Every `CREATE DATABASE` must include a `LOCATION` clause pointing at `{base_path}/tables`, and every `CREATE TABLE ... USING DELTA` for a table not created via `saveAsTable`/`toTable` must include an explicit `LOCATION` too** — this is the exact bug Plan 2's final review caught and fixed (managed tables with no location get destroyed when the cluster stops). Do not omit this.
- No `dbutils`, no `%md`/`# MAGIC` Databricks magic syntax, no Unity Catalog `catalog` concept (Glue is two-level: `schema`/database + table). `PipelineConfig` is constructed as `PipelineConfig(schema=..., base_path=...)` — never pass `catalog=`.
- Output format is real Jupyter `.ipynb` JSON (`nbformat: 4`, `nbformat_minor: 5`), not a Databricks-style `.py` file with `# COMMAND ----------` markers — EMR JupyterHub renders real notebooks, not Databricks' proprietary cell-marker format.
- Kernel metadata: `{"kernelspec": {"display_name": "PySpark", "language": "python", "name": "pysparkkernel"}}` — matches Plan 1's `emr_learning.tf`, which enables `Livy` alongside `JupyterHub`/`Spark`, giving Sparkmagic's `pysparkkernel` an ambient `spark` session (same convenience Databricks notebooks have) with no user-side `SparkSession.builder` call needed.
- **Building the `.ipynb` files**: hand-typing raw notebook JSON is error-prone. Use this exact builder, saved as a temporary local script (e.g. `/tmp/build_nb.py`) — run it once per notebook to generate the file, then delete the temporary script. Do not commit the builder script; it's a one-off generation tool, not a project artifact:

```python
import json

def nb(cells):
    return {
        "cells": cells,
        "metadata": {"kernelspec": {"display_name": "PySpark", "language": "python", "name": "pysparkkernel"}},
        "nbformat": 4,
        "nbformat_minor": 5,
    }

def md(text):
    return {"cell_type": "markdown", "metadata": {}, "source": text.splitlines(keepends=True)}

def code(text):
    return {"cell_type": "code", "execution_count": None, "metadata": {}, "outputs": [], "source": text.splitlines(keepends=True)}
```

  Build each notebook as `json.dump(nb([md("..."), code("..."), ...]), open("emr-notebooks/<name>.ipynb", "w"), indent=1)`. After generating, always validate with `python3 -c "import json; json.load(open('emr-notebooks/<name>.ipynb'))"` (must not raise) — this is your only mechanical correctness check, since there's no PySpark/EMR environment available to actually execute these notebooks in this session.

---

### Task 1: `00_environment_setup.ipynb`, `01_batch_lakehouse_bronze_silver_gold.ipynb`, `02_kafka_msk_streaming_ingest.ipynb`

**Files:**
- Create: `emr-notebooks/00_environment_setup.ipynb`
- Create: `emr-notebooks/01_batch_lakehouse_bronze_silver_gold.ipynb`
- Create: `emr-notebooks/02_kafka_msk_streaming_ingest.ipynb`

**Interfaces:**
- All three notebooks share the same first-cell pattern: plain variables (`schema = "retail_lakehouse"`, `base_path = "s3://<your-bucket>/data"`) instead of widgets, then `from retail_lakehouse.config import PipelineConfig; cfg = PipelineConfig(schema=schema, base_path=base_path)`.
- `00`'s `CREATE DATABASE` (with `LOCATION`) is the one place the Glue database actually gets created — `01` and `02` (and every later notebook) assume it already exists and just `USE` it.

- [ ] **Step 1: Build `emr-notebooks/00_environment_setup.ipynb`**

Cells, in order (markdown cells are `md(...)`, code cells are `code(...)`, per the Global Constraints builder):

```
md("# 00 — Environment setup (EMR)\n\nCreate the Glue database and generate seed source files (products, customers, clickstream events). Port of `notebooks/00_environment_setup.py` — no `dbutils`, no wheel-install cell (the persistent EMR cluster's bootstrap action already installed `retail_lakehouse`), Glue database instead of Unity Catalog catalog/schema/volume.")

code("schema = \"retail_lakehouse\"\nbase_path = \"s3://<your-lakehouse-bucket>/data\"  # from `terraform output lakehouse_bucket_name`\n\nfrom retail_lakehouse.config import PipelineConfig\ncfg = PipelineConfig(schema=schema, base_path=base_path)\n\nspark.sql(f\"CREATE DATABASE IF NOT EXISTS `{cfg.schema}` LOCATION '{cfg.path(\"tables\")}'\")\nspark.sql(f\"USE `{cfg.schema}`\")\nspark.conf.set(\"spark.sql.shuffle.partitions\", \"8\")\nprint(\"Spark version:\", spark.version)")

md("## Generate seed data\n\nProducts and customers are batch dimension data (as if loaded from an OLTP export). Events simulate what would otherwise arrive continuously from MSK in `02_kafka_msk_streaming_ingest.ipynb` — having a batch copy also lets `01` demonstrate the pure-batch path for comparison.")

code("from retail_lakehouse.generate import synthetic_products, synthetic_customers, synthetic_events\nimport random\nrandom.seed(42)\n\nproduct_rows = synthetic_products(50)\ncustomer_rows = synthetic_customers(200)\nevent_rows = list(synthetic_events(2000))\n\nproduct_df = spark.createDataFrame(product_rows)\ncustomer_df = spark.createDataFrame(customer_rows)\nevent_df = spark.createDataFrame(event_rows)\n\nproduct_df.write.mode(\"overwrite\").format(\"delta\").saveAsTable(cfg.table(\"dim_product_seed\"))\ncustomer_df.write.mode(\"overwrite\").format(\"delta\").saveAsTable(cfg.table(\"dim_customer_seed\"))\nevent_df.write.mode(\"overwrite\").json(cfg.path(\"source\", \"events_json\"))\nproduct_df.write.mode(\"overwrite\").option(\"header\", True).csv(cfg.path(\"source\", \"products_csv\"))\ncustomer_df.write.mode(\"overwrite\").option(\"header\", True).csv(cfg.path(\"source\", \"customers_csv\"))\n\nprint(\"Created seed tables and files\")\nevent_df.limit(10).toPandas()")

md("## Tables created so far")

code("spark.sql(f\"SHOW TABLES IN `{cfg.schema}`\").show()")

md("## Next steps\n\n- `01_batch_lakehouse_bronze_silver_gold.ipynb` — batch path from the seed files.\n- `02_kafka_msk_streaming_ingest.ipynb` — real MSK ingestion (requires `infra/terraform` provisioned; see `RUNBOOK.md`).")
```

Note the nested-quote handling in the `CREATE DATABASE` f-string above (`cfg.path("tables")` inside an f-string that's itself inside a double-quoted Python string being embedded in JSON) — when you actually write the Python source for the code cell, use this exact line (no escaping needed once it's real Python source, not a shell-quoted string):
```python
spark.sql(f"CREATE DATABASE IF NOT EXISTS `{cfg.schema}` LOCATION '{cfg.path('tables')}'")
```
(single quotes inside the f-string's inner call, matching how `emr_jobs/00_environment_setup.py` from Plan 2 already does this — copy that exact pattern.)

- [ ] **Step 2: Build `emr-notebooks/01_batch_lakehouse_bronze_silver_gold.ipynb`**

```
md("# 01 — Batch lakehouse: bronze / silver / gold (EMR)\n\nBuild the medallion pipeline from the batch seed files created in `00`, using the shared `retail_lakehouse` transformation functions — the same functions the streaming path in `03` reuses for silver enrichment. This is the baseline to compare the streaming path against.")

code("schema = \"retail_lakehouse\"\nbase_path = \"s3://<your-lakehouse-bucket>/data\"\n\nfrom retail_lakehouse.config import PipelineConfig\ncfg = PipelineConfig(schema=schema, base_path=base_path)\nspark.sql(f\"USE `{cfg.schema}`\")")

md("## Read the seed files and dimensions")

code("from retail_lakehouse.transformations import (\n    normalize_click_events, filter_valid_events, deduplicate_events,\n    enrich_with_product_customer, revenue_by_hour, add_ingest_metadata,\n)\nfrom pyspark.sql import functions as F\n\nraw_events = spark.read.json(cfg.path(\"source\", \"events_json\"))\nproducts = spark.table(cfg.table(\"dim_product_seed\"))\ncustomers = spark.table(cfg.table(\"dim_customer_seed\"))\nprint(\"Raw event rows:\", raw_events.count())")

md("## Bronze — minimally transformed, ingestion metadata only")

code("bronze = add_ingest_metadata(raw_events, \"batch_file_seed\")\nbronze.write.mode(\"overwrite\").format(\"delta\").partitionBy(\"_ingest_date\").saveAsTable(cfg.table(\"bronze_clickstream_batch\"))\nspark.table(cfg.table(\"bronze_clickstream_batch\")).limit(5).toPandas()")

md("## Silver — normalized, validated, deduplicated")

code("silver = deduplicate_events(filter_valid_events(normalize_click_events(bronze)))\nsilver.write.mode(\"overwrite\").format(\"delta\").partitionBy(\"event_date\").saveAsTable(cfg.table(\"silver_clickstream_batch\"))\nprint(\"Silver rows:\", spark.table(cfg.table(\"silver_clickstream_batch\")).count())")

md("## Gold — enriched with dimensions, aggregated to a business metric")

code("enriched = enrich_with_product_customer(spark.table(cfg.table(\"silver_clickstream_batch\")), products, customers)\ngold = revenue_by_hour(enriched.withColumn(\n    \"is_purchase\", F.col(\"event_type\").isin(\"purchase\", \"checkout\")\n))\ngold.write.mode(\"overwrite\").format(\"delta\").saveAsTable(cfg.table(\"gold_revenue_by_hour_batch\"))\n\nspark.table(cfg.table(\"gold_revenue_by_hour_batch\")).orderBy(F.desc(\"revenue\")).toPandas()")

md("## Query plan check\n\nConfirm the product join broadcasts (small dimension) while the aggregation shuffles (expected for `groupBy`). See `class-emr/01_spark_batch_processing.ipynb` (added in a later plan) for the full explanation of this plan shape.")

code("enriched.explain(\"formatted\")")

md("## Next\n\n`02_kafka_msk_streaming_ingest.ipynb` ingests the same kind of event from a live MSK topic instead of a static file, and `03_streaming_silver_gold_delta.ipynb` reuses `normalize_click_events`/`enrich_with_product_customer` again — the transformation code doesn't care whether it's fed by batch or streaming, only the orchestration around it differs.")
```

- [ ] **Step 3: Build `emr-notebooks/02_kafka_msk_streaming_ingest.ipynb`**

This one differs from the Databricks original in its auth section: no `databricks.serviceCredential` — plain `kafka.security.protocol`/`kafka.sasl.mechanism`/`kafka.sasl.jaas.config`/`kafka.sasl.client.callback.handler.class` options instead, since EMR's EC2 instance profile (Plan 1's `iam.tf`) supplies AWS credentials ambiently to every process on the cluster:

```
md("# 02 — Kafka/MSK streaming ingest (EMR)\n\nRead clickstream events from a live Amazon MSK topic with Spark Structured Streaming and land them as an append-only bronze Delta table on S3.\n\n**Requires:**\n1. An MSK cluster reachable from the EMR cluster's VPC/subnets (see `infra/terraform/`).\n2. A topic (default `retail-clickstream`) — created via `scripts/create_msk_topics.sh` or manually.\n3. Producer traffic on that topic — run `scripts/seed_kafka_topic.py`, or point a real producer at it.\n4. The `aws-msk-iam-auth` library on the EMR cluster's classpath for IAM auth (EMR doesn't bundle it — attach it as a bootstrap action or `--jars` at kernel launch; this is a genuine setup step, unlike Databricks which handles this internally).\n\nIf MSK isn't provisioned yet, this notebook raises a clear error rather than silently doing nothing — use `01_batch_lakehouse_bronze_silver_gold.ipynb` or `07_file_rate_streaming_fallback.ipynb` to keep working while infra is being set up.")

code("schema = \"retail_lakehouse\"\nbase_path = \"s3://<your-lakehouse-bucket>/data\"\nkafka_bootstrap_servers = \"\"  # e.g. b-1.xxx.kafka.us-east-1.amazonaws.com:9098 -- from `terraform output -raw msk_bootstrap_brokers_command`\nkafka_topic = \"retail-clickstream\"\nstarting_offsets = \"earliest\"\n\nfrom retail_lakehouse.config import PipelineConfig\ncfg = PipelineConfig(schema=schema, base_path=base_path,\n                      kafka_bootstrap_servers=kafka_bootstrap_servers, kafka_topic=kafka_topic)\nspark.sql(f\"USE `{cfg.schema}`\")\n\nif not kafka_bootstrap_servers:\n    raise ValueError(\"Set kafka_bootstrap_servers before running this notebook.\")")

md("## Configure the Kafka source\n\nIAM auth (`AWS_MSK_IAM`) uses the EMR instance profile's credentials ambiently -- no service-credential indirection needed, unlike the Databricks-serverless version of this notebook.")

code("from pyspark.sql import functions as F\n\nreader = (\n    spark.readStream.format(\"kafka\")\n    .option(\"kafka.bootstrap.servers\", kafka_bootstrap_servers)\n    .option(\"subscribe\", kafka_topic)\n    .option(\"startingOffsets\", starting_offsets)\n    .option(\"failOnDataLoss\", \"false\")\n    .option(\"maxOffsetsPerTrigger\", 10000)\n    .option(\"kafka.security.protocol\", \"SASL_SSL\")\n    .option(\"kafka.sasl.mechanism\", \"AWS_MSK_IAM\")\n    .option(\"kafka.sasl.jaas.config\", \"software.amazon.msk.auth.iam.IAMLoginModule required;\")\n    .option(\"kafka.sasl.client.callback.handler.class\", \"software.amazon.msk.auth.iam.IAMClientCallbackHandler\")\n)\n\nraw_kafka = reader.load()")

md("## Parse and write to bronze\n\n`parse_kafka_value` (from `retail_lakehouse.transformations`) is unit-tested in `tests/test_kafka_parsing.py` against a synthetic Kafka-shaped DataFrame, so this logic is verified in CI without needing a live broker.")

code("from retail_lakehouse.transformations import parse_kafka_value, add_ingest_metadata\n\nparsed = parse_kafka_value(raw_kafka)\nbronze_stream = add_ingest_metadata(parsed, \"kafka_msk\")\n\nquery = (\n    bronze_stream.writeStream\n    .format(\"delta\")\n    .option(\"checkpointLocation\", cfg.checkpoint(\"kafka_bronze_clickstream\"))\n    .outputMode(\"append\")\n    .trigger(availableNow=True)\n    .toTable(cfg.table(\"bronze_clickstream_kafka\"))\n)\nquery.awaitTermination()\n\nprint(\"Micro-batch(es) complete. Query progress:\")\nfor p in query.recentProgress[-5:]:\n    print({\"timestamp\": p[\"timestamp\"], \"numInputRows\": p[\"numInputRows\"], \"durationMs\": p[\"durationMs\"]})\n\nspark.table(cfg.table(\"bronze_clickstream_kafka\")).orderBy(F.desc(\"_ingest_ts\")).limit(20).toPandas()")

md("## Next\n\n`03_streaming_silver_gold_delta.ipynb` reads `bronze_clickstream_kafka` as a stream and builds the silver/gold layers with watermarking, de-duplication, and an idempotent upsert into gold.")
```

- [ ] **Step 4: Validate all three files are well-formed JSON**

Run: `python3 -c "import json; [json.load(open(f'emr-notebooks/{n}.ipynb')) for n in ['00_environment_setup','01_batch_lakehouse_bronze_silver_gold','02_kafka_msk_streaming_ingest']]"`
Expected: no output, exit code 0.

- [ ] **Step 5: Commit**

```bash
git add emr-notebooks/00_environment_setup.ipynb emr-notebooks/01_batch_lakehouse_bronze_silver_gold.ipynb emr-notebooks/02_kafka_msk_streaming_ingest.ipynb
git commit -m "Add emr-notebooks/ 00-02: environment setup, batch medallion, Kafka/MSK ingest"
```

---

### Task 2: `07_file_rate_streaming_fallback.ipynb`, `03_streaming_silver_gold_delta.ipynb`, `04_delta_production_patterns_iceberg.ipynb`

**Files:**
- Create: `emr-notebooks/07_file_rate_streaming_fallback.ipynb`
- Create: `emr-notebooks/03_streaming_silver_gold_delta.ipynb`
- Create: `emr-notebooks/04_delta_production_patterns_iceberg.ipynb`

**Interfaces:**
- Same first-cell pattern as Task 1 (plain variables, `PipelineConfig(schema=..., base_path=...)`, `USE` the already-created database — none of these three notebooks create the database themselves).
- `07` writes `bronze_clickstream_rate`; `03` reads it via a `bronze_table` variable (default `bronze_clickstream_kafka`, but the notebook's own markdown tells the reader to set it to `bronze_clickstream_rate` if running after `07` instead of `02`).

- [ ] **Step 1: Build `emr-notebooks/07_file_rate_streaming_fallback.ipynb`**

```
md("# 07 — File/rate streaming fallback (EMR, no MSK required)\n\nExercises the exact same streaming mechanics as `02_kafka_msk_streaming_ingest.ipynb` and `03_streaming_silver_gold_delta.ipynb`, without requiring a live MSK cluster. Use this while `infra/terraform` is being provisioned, or for a classroom/demo run with no AWS networking dependency.\n\nThis writes to `bronze_clickstream_rate`. Point `03`'s `bronze_table` variable at `bronze_clickstream_rate` to run the rest of the pipeline against this fallback source.")

code("schema = \"retail_lakehouse\"\nbase_path = \"s3://<your-lakehouse-bucket>/data\"\n\nfrom retail_lakehouse.config import PipelineConfig\ncfg = PipelineConfig(schema=schema, base_path=base_path)\nspark.sql(f\"USE `{cfg.schema}`\")")

code("from pyspark.sql import functions as F\n\nstream_events = (\n    spark.readStream.format(\"rate\")\n    .option(\"rowsPerSecond\", 50)\n    .option(\"numPartitions\", 2)\n    .load()\n    .select(\n        F.concat(F.lit(\"rate-\"), F.col(\"value\")).alias(\"event_id\"),\n        F.col(\"timestamp\").alias(\"event_ts\"),\n        F.concat(F.lit(\"u\"), F.lpad((F.col(\"value\") % 200).cast(\"string\"), 5, \"0\")).alias(\"user_id\"),\n        F.concat(F.lit(\"s\"), F.lpad((F.col(\"value\") % 50).cast(\"string\"), 5, \"0\")).alias(\"session_id\"),\n        F.concat(F.lit(\"p\"), F.lpad(((F.col(\"value\") % 50) + 1).cast(\"string\"), 4, \"0\")).alias(\"product_id\"),\n        F.when((F.col(\"value\") % 10) == 0, \"purchase\").when((F.col(\"value\") % 5) == 0, \"add_to_cart\").otherwise(\"view\").alias(\"event_type\"),\n        F.when((F.col(\"value\") % 10) == 0, 1).otherwise(0).cast(\"int\").alias(\"quantity\"),\n        F.when((F.col(\"value\") % 10) == 0, 19.99).otherwise(0.0).cast(\"double\").alias(\"price\"),\n        F.lit(\"product\").alias(\"page\"),\n        F.lit(\"rate_source\").alias(\"user_agent\"),\n    )\n)\n\nfrom retail_lakehouse.transformations import add_ingest_metadata\nbronze_stream = add_ingest_metadata(stream_events, \"rate_source_fallback\")\n\nquery = (\n    bronze_stream.writeStream\n    .format(\"delta\")\n    .option(\"checkpointLocation\", cfg.checkpoint(\"rate_bronze_clickstream\"))\n    .outputMode(\"append\")\n    .trigger(availableNow=True)\n    .toTable(cfg.table(\"bronze_clickstream_rate\"))\n)\n\nquery.awaitTermination()\nprint(\"AvailableNow fallback stream completed\")\nspark.table(cfg.table(\"bronze_clickstream_rate\")).orderBy(F.desc(\"event_ts\")).limit(20).toPandas()")

md("## Next\n\nRun `03_streaming_silver_gold_delta.ipynb` with its `bronze_table` variable set to `bronze_clickstream_rate` to build silver/gold from this fallback source using the same package code that will run against real MSK data.")
```

- [ ] **Step 2: Build `emr-notebooks/03_streaming_silver_gold_delta.ipynb`**

Note the `CREATE TABLE` for the gold table needs a `LOCATION` clause (per Global Constraints — this is the exact table Plan 2's review caught missing a location on):

```
md("# 03 — Streaming silver/gold with watermarking, de-dup, and idempotent upsert (EMR)\n\nConsume a bronze table as a stream, produce a watermarked/deduplicated silver stream, enrich with dimensions, and upsert windowed revenue into gold using `foreachBatch` + `MERGE` for idempotent, replay-safe writes.\n\nIf you ran `07_file_rate_streaming_fallback.ipynb` instead of `02_kafka_msk_streaming_ingest.ipynb`, set `bronze_table` below to `bronze_clickstream_rate`.")

code("schema = \"retail_lakehouse\"\nbase_path = \"s3://<your-lakehouse-bucket>/data\"\nbronze_table = \"bronze_clickstream_kafka\"  # or \"bronze_clickstream_rate\" if you ran 07 instead of 02\n\nfrom retail_lakehouse.config import PipelineConfig\ncfg = PipelineConfig(schema=schema, base_path=base_path)\nspark.sql(f\"USE `{cfg.schema}`\")\n\nif not spark.catalog.tableExists(f\"{cfg.schema}.{bronze_table}\"):\n    raise ValueError(\n        f\"{bronze_table} does not exist yet. Run 02_kafka_msk_streaming_ingest.ipynb (or the \"\n        \"07_file_rate_streaming_fallback.ipynb fallback and set bronze_table to its output table) first.\"\n    )")

md("## Silver: watermark, de-duplicate, normalize, enrich")

code("from retail_lakehouse.streaming import deduplicate_stream\nfrom retail_lakehouse.transformations import normalize_click_events, enrich_with_product_customer\n\nbronze_stream = spark.readStream.table(cfg.table(bronze_table))\n\ndeduped_stream = deduplicate_stream(bronze_stream, watermark_col=\"event_ts\", watermark_delay=\"10 minutes\")\nnormalized_stream = normalize_click_events(deduped_stream)\n\nproducts = spark.table(cfg.table(\"dim_product_seed\"))\ncustomers = spark.table(cfg.table(\"dim_customer_seed\"))\nsilver_stream = enrich_with_product_customer(normalized_stream, products, customers)\n\nsilver_query = (\n    silver_stream.writeStream\n    .format(\"delta\")\n    .option(\"checkpointLocation\", cfg.checkpoint(\"silver_clickstream_streaming\"))\n    .outputMode(\"append\")\n    .trigger(availableNow=True)\n    .toTable(cfg.table(\"silver_clickstream_streaming\"))\n)\nsilver_query.awaitTermination()\nprint(\"Silver rows:\", spark.table(cfg.table(\"silver_clickstream_streaming\")).count())")

md("## Gold: windowed revenue via idempotent MERGE upsert\n\n`make_gold_upsert` (from `retail_lakehouse.streaming`) builds a `foreachBatch` function that MERGEs each micro-batch's aggregated rows by `(window_start, category)`. Replaying a micro-batch after a retry updates the same gold rows instead of double-counting revenue -- this is what \"effectively-exactly-once\" means in practice for a streaming aggregation sink.")

code("from pyspark.sql import functions as F\nfrom retail_lakehouse.streaming import windowed_revenue, make_gold_upsert\n\ngold_table = cfg.table(\"gold_revenue_windows_streaming\")\nspark.sql(f\"\"\"\nCREATE TABLE IF NOT EXISTS {gold_table} (\n  window STRUCT<start: TIMESTAMP, end: TIMESTAMP>,\n  category STRING,\n  orders LONG,\n  revenue DOUBLE\n) USING DELTA\nLOCATION '{cfg.path(\"tables\", \"gold_revenue_windows_streaming\")}'\n\"\"\")\n\nsilver_for_gold = (\n    spark.readStream.table(cfg.table(\"silver_clickstream_streaming\"))\n    .withWatermark(\"event_ts\", \"10 minutes\")\n)\nwindowed = windowed_revenue(silver_for_gold, window_duration=\"5 minutes\", watermark_delay=\"10 minutes\")\n\nupsert_fn = make_gold_upsert(\n    target_table=gold_table,\n    merge_keys=[\"window\", \"category\"],\n    update_cols=[\"orders\", \"revenue\"],\n)\n\ngold_query = (\n    windowed.writeStream\n    .foreachBatch(upsert_fn)\n    .option(\"checkpointLocation\", cfg.checkpoint(\"gold_revenue_windows_streaming\"))\n    .trigger(availableNow=True)\n    .start()\n)\ngold_query.awaitTermination()\n\nspark.table(gold_table).orderBy(F.desc(\"window.start\"), F.desc(\"revenue\")).limit(20).toPandas()")

md("## Verify idempotency\n\nRe-running the gold query against already-processed offsets should not change row counts or revenue totals, because MERGE keys on `(window, category)`. Try re-running the previous cell now -- row count and revenue sums should be identical.")

code("before = spark.table(gold_table).agg(F.sum(\"revenue\")).first()[0]\nprint(\"Total gold revenue (should stay identical across reruns of the previous cell):\", before)")

md("## Next\n\n`04_delta_production_patterns_iceberg.ipynb` covers Delta production operations (MERGE, time travel, OPTIMIZE, VACUUM) and a managed-Iceberg comparison; `05_observability_testing_performance.ipynb` covers monitoring these queries in production.")
```

- [ ] **Step 3: Build `emr-notebooks/04_delta_production_patterns_iceberg.ipynb`**

The `OPTIMIZE`/`ZORDER` and managed-Iceberg cells already defensively `try`/`except` and explain the fallback in both the Databricks original and here — keep that pattern (it now covers "not supported on this delta-spark version" or "Iceberg catalog not configured on this EMR cluster" just as well as it covered "not supported on this Databricks Runtime"):

```
md("# 04 — Delta Lake production patterns and Iceberg comparison (EMR)\n\nDemonstrate `DESCRIBE HISTORY`, `MERGE`, time travel, `OPTIMIZE`/`ZORDER`, `VACUUM`, and a managed Iceberg table for comparison, against the tables built in `01`-`03`.")

code("schema = \"retail_lakehouse\"\nbase_path = \"s3://<your-lakehouse-bucket>/data\"\n\nfrom retail_lakehouse.config import PipelineConfig\ncfg = PipelineConfig(schema=schema, base_path=base_path)\nspark.sql(f\"USE `{cfg.schema}`\")\n\nfrom delta.tables import DeltaTable")

md("## Table history")

code("spark.sql(f\"DESCRIBE HISTORY {cfg.table('silver_clickstream_batch')}\").toPandas()")

md("## MERGE upsert into a customer dimension (CDC-style update)")

code("spark.table(cfg.table(\"dim_customer_seed\")).write.mode(\"overwrite\").format(\"delta\").saveAsTable(cfg.table(\"dim_customer_current\"))\n\nupdates = spark.createDataFrame([\n    (\"u00001\", \"loyal\", \"west\", 1704067200),\n    (\"u99999\", \"new\", \"central\", 1735689600),\n], \"user_id string, segment string, region string, signup_epoch long\")\n\ndelta_t = DeltaTable.forName(spark, cfg.table(\"dim_customer_current\"))\n(delta_t.alias(\"t\")\n .merge(updates.alias(\"s\"), \"t.user_id = s.user_id\")\n .whenMatchedUpdateAll()\n .whenNotMatchedInsertAll()\n .execute())\n\nspark.table(cfg.table(\"dim_customer_current\")).filter(\"user_id in ('u00001','u99999')\").toPandas()")

md("## Time travel")

code("history = spark.sql(f\"DESCRIBE HISTORY {cfg.table('dim_customer_current')}\")\nversions = [r.version for r in history.select(\"version\").collect()]\nif len(versions) >= 2:\n    earliest = min(versions)\n    print(\"Reading version\", earliest, \"(before the MERGE):\")\n    spark.read.option(\"versionAsOf\", earliest).table(cfg.table(\"dim_customer_current\")).limit(10).toPandas()")

md("## OPTIMIZE + ZORDER and VACUUM\n\n`OPTIMIZE`/`ZORDER` require a delta-spark version that supports them (2.0+ for OPTIMIZE, later for ZORDER) -- this cell degrades gracefully if not. `VACUUM` physically deletes files no longer referenced by the log and older than the retention threshold (default 7 days) -- run it on a schedule, not ad hoc, and never with a retention below 7 days on a table with concurrent readers/time-travel users.")

code("try:\n    spark.sql(f\"OPTIMIZE {cfg.table('silver_clickstream_batch')} ZORDER BY (user_id, product_id)\")\nexcept Exception as e:\n    print(\"OPTIMIZE/ZORDER not supported by this delta-spark version:\", str(e)[:300])\n\nspark.sql(f\"DESCRIBE DETAIL {cfg.table('silver_clickstream_batch')}\").toPandas()")

md("## Managed Iceberg table for comparison\n\nFalls back to a Delta comparison table if this EMR cluster's Spark session isn't configured with an Iceberg catalog (this project's EMR clusters configure Glue as a Hive-compatible metastore for Delta, not an Iceberg catalog -- so this fallback path is expected to run by default here, exactly as it would on a Databricks workspace without managed Iceberg enabled).")

code("iceberg_table = cfg.table(\"iceberg_clickstream_sample\")\nfallback_delta = cfg.table(\"delta_iceberg_comparison_sample\")\nsource = spark.table(cfg.table(\"silver_clickstream_batch\")).limit(200)\n\ntry:\n    spark.sql(f\"DROP TABLE IF EXISTS {iceberg_table}\")\n    spark.sql(f\"\"\"\n    CREATE TABLE {iceberg_table} (\n      event_id STRING, event_ts TIMESTAMP, user_id STRING, product_id STRING,\n      event_type STRING, event_date DATE\n    ) USING ICEBERG\n    \"\"\")\n    (source.select(\"event_id\", \"event_ts\", \"user_id\", \"product_id\", \"event_type\", \"event_date\")\n          .write.mode(\"append\").saveAsTable(iceberg_table))\n    print(\"Created managed Iceberg table:\", iceberg_table)\nexcept Exception as e:\n    print(\"Managed Iceberg not available on this cluster, falling back to Delta:\", str(e)[:300])\n    (source.select(\"event_id\", \"event_ts\", \"user_id\", \"product_id\", \"event_type\", \"event_date\")\n          .write.mode(\"overwrite\").format(\"delta\").saveAsTable(fallback_delta))")

md("## Next\n\n`05_observability_testing_performance.ipynb` covers monitoring these tables and streaming queries in production, and `06_capstone_end_to_end.ipynb` ties the whole pipeline together in one flow.")
```

- [ ] **Step 4: Validate all three files are well-formed JSON**

Run: `python3 -c "import json; [json.load(open(f'emr-notebooks/{n}.ipynb')) for n in ['07_file_rate_streaming_fallback','03_streaming_silver_gold_delta','04_delta_production_patterns_iceberg']]"`
Expected: no output, exit code 0.

- [ ] **Step 5: Commit**

```bash
git add emr-notebooks/07_file_rate_streaming_fallback.ipynb emr-notebooks/03_streaming_silver_gold_delta.ipynb emr-notebooks/04_delta_production_patterns_iceberg.ipynb
git commit -m "Add emr-notebooks/ 07,03,04: streaming fallback, silver/gold, Delta/Iceberg patterns"
```

---

### Task 3: `05_observability_testing_performance.ipynb`, `06_capstone_end_to_end.ipynb`

**Files:**
- Create: `emr-notebooks/05_observability_testing_performance.ipynb`
- Create: `emr-notebooks/06_capstone_end_to_end.ipynb`

**Interfaces:** Same first-cell pattern as Tasks 1-2.

- [ ] **Step 1: Build `emr-notebooks/05_observability_testing_performance.ipynb`**

```
md("# 05 — Observability, testing, and performance tuning (EMR)\n\nOperational practices a production on-call engineer actually uses: streaming query metrics, table history/detail, query plans, and data quality checks.")

code("schema = \"retail_lakehouse\"\nbase_path = \"s3://<your-lakehouse-bucket>/data\"\n\nfrom retail_lakehouse.config import PipelineConfig\ncfg = PipelineConfig(schema=schema, base_path=base_path)\nspark.sql(f\"USE `{cfg.schema}`\")")

md("## Streaming query metrics\n\nEvery `StreamingQuery` exposes `.lastProgress`/`.recentProgress` with metrics you should alert on:\n\n- `numInputRows`/`inputRowsPerSecond` -- throughput; a sudden drop can mean an upstream (MSK) problem.\n- `durationMs` per phase (`addBatch`, `getBatch`, `queryPlanning`) -- where time is actually spent.\n- `sources[].endOffset` vs the topic's actual latest offset -- this delta is **consumer lag**.\n- `stateOperators[].numRowsTotal` -- state store size; unbounded growth usually means a missing/too-long watermark or a `dropDuplicates` key that never converges.\n\nIn production, ship these via a `StreamingQueryListener` to CloudWatch/Datadog/Prometheus rather than reading them interactively:\n\n```python\nfrom pyspark.sql.streaming import StreamingQueryListener\n\nclass MetricsListener(StreamingQueryListener):\n    def onQueryProgress(self, event):\n        p = event.progress\n        # push p.numInputRows, p.inputRowsPerSecond, p.durationMs.get(\"addBatch\") to your metrics sink\n        pass\n    def onQueryStarted(self, event): pass\n    def onQueryTerminated(self, event): pass\n\nspark.streams.addListener(MetricsListener())\n```")

md("## Table history and storage detail")

code("for tbl in [\"bronze_clickstream_kafka\", \"silver_clickstream_streaming\", \"gold_revenue_windows_streaming\"]:\n    try:\n        print(\"==\", tbl, \"history ==\")\n        spark.sql(f\"DESCRIBE HISTORY {cfg.table(tbl)}\").show()\n        print(\"==\", tbl, \"detail ==\")\n        spark.sql(f\"DESCRIBE DETAIL {cfg.table(tbl)}\").show()\n    except Exception as e:\n        print(tbl, \"not available yet:\", str(e)[:200])")

md("## Query plan and tuning levers\n\nLook for scan filters (`PushedFilters`), broadcast exchanges, sort-merge joins, shuffle exchanges, and skew hints in `explain(\"formatted\")` output.")

code("if spark.catalog.tableExists(f\"{cfg.schema}.silver_clickstream_batch\"):\n    q = spark.table(cfg.table(\"silver_clickstream_batch\")).groupBy(\"event_type\").count()\n    q.explain(\"formatted\")\n    q.toPandas()")

md("## Data quality summary")

code("from retail_lakehouse.quality import quality_summary, assert_no_duplicate_keys\n\nif spark.catalog.tableExists(f\"{cfg.schema}.silver_clickstream_batch\"):\n    quality_summary(spark.table(cfg.table(\"silver_clickstream_batch\"))).show()\n    assert_no_duplicate_keys(spark.table(cfg.table(\"silver_clickstream_batch\")), [\"event_id\"])\n    print(\"No duplicate event_id in silver_clickstream_batch\")")

md("## Production tuning and operations checklist\n\n- Start with a correct data model and partition strategy; fix the model before tuning code.\n- Keep file sizes healthy (target ~128MB-1GB per file); `OPTIMIZE` after ingestion bursts, not every micro-batch.\n- Use broadcast joins only for genuinely small dimensions; check `explain()` to confirm the plan Spark actually picked, don't assume.\n- Trust AQE for partition coalescing and skew joins; only hand-tune `spark.sql.shuffle.partitions` when AQE's defaults are measurably wrong for your workload.\n- Cache only reused intermediate data, and `unpersist()` when done.\n- Size streaming state and watermarks deliberately -- measure actual event lateness in your data before picking a watermark delay.\n- Alert on consumer lag and state store size growth, not just job failure/success.\n- Never delete a production streaming checkpoint casually -- it is the source of truth for \"what has been processed.\"\n\n## Next\n\n`06_capstone_end_to_end.ipynb` runs the full pipeline as a single validated flow -- this is also what the MWAA DAG (`airflow/dags/retail_lakehouse_pipeline.py`) orchestrates as a scheduled run.")
```

- [ ] **Step 2: Build `emr-notebooks/06_capstone_end_to_end.ipynb`**

```
md("# 06 — Capstone: end-to-end lakehouse pipeline (EMR)\n\nRun the full batch + streaming medallion pipeline as a single flow and validate outputs. This mirrors what the MWAA DAG (`airflow/dags/retail_lakehouse_pipeline.py`) runs as a scheduled pipeline in production.")

code("schema = \"retail_lakehouse\"\nbase_path = \"s3://<your-lakehouse-bucket>/data\"\n\nfrom retail_lakehouse.config import PipelineConfig\ncfg = PipelineConfig(schema=schema, base_path=base_path)\nspark.sql(f\"USE `{cfg.schema}`\")")

md("## Seed + batch bronze/silver/gold")

code("from retail_lakehouse.generate import synthetic_products, synthetic_customers, synthetic_events\nfrom retail_lakehouse.transformations import (\n    normalize_click_events, filter_valid_events, deduplicate_events,\n    enrich_with_product_customer, revenue_by_hour, add_ingest_metadata,\n)\nfrom pyspark.sql import functions as F\nimport random\nrandom.seed(11)\n\nproducts = spark.createDataFrame(synthetic_products(50))\ncustomers = spark.createDataFrame(synthetic_customers(200))\nevents = spark.createDataFrame(list(synthetic_events(5000)))\n\nproducts.write.mode(\"overwrite\").format(\"delta\").saveAsTable(cfg.table(\"dim_product_seed\"))\ncustomers.write.mode(\"overwrite\").format(\"delta\").saveAsTable(cfg.table(\"dim_customer_seed\"))\n\nbronze = add_ingest_metadata(events, \"capstone_batch\")\nsilver = deduplicate_events(filter_valid_events(normalize_click_events(bronze)))\nsilver.write.mode(\"overwrite\").format(\"delta\").partitionBy(\"event_date\").saveAsTable(cfg.table(\"capstone_silver_events\"))\n\nenriched = enrich_with_product_customer(silver, products, customers)\ngold = revenue_by_hour(enriched.withColumn(\"is_purchase\", F.col(\"event_type\").isin(\"purchase\", \"checkout\")))\ngold.write.mode(\"overwrite\").format(\"delta\").saveAsTable(cfg.table(\"capstone_gold_revenue\"))")

md("## Validate outputs\n\nA real production run would fail the pipeline (non-zero exit) on a failed assertion -- exactly what `emr_jobs/06_capstone_end_to_end.py` does, which is what the MWAA DAG actually schedules. This notebook version just raises, which is the right behavior interactively too.")

code("from retail_lakehouse.quality import assert_no_duplicate_keys\n\nassert spark.table(cfg.table(\"capstone_silver_events\")).count() > 0, \"silver produced no rows\"\nassert spark.table(cfg.table(\"capstone_gold_revenue\")).count() > 0, \"gold produced no rows\"\nassert_no_duplicate_keys(spark.table(cfg.table(\"capstone_silver_events\")), [\"event_id\"])\n\nspark.table(cfg.table(\"capstone_gold_revenue\")).orderBy(F.desc(\"revenue\")).toPandas()\nprint(\"Capstone pipeline validated successfully.\")")

md("## Discussion questions\n\n1. Which parts of this pipeline should move from `availableNow` scheduled streaming to a true always-on continuous stream, and what would that cost in cluster spend vs latency improvement?\n2. Where should MSK credentials live, and how would you rotate them without downtime?\n3. Which tables should be governed as Delta vs Iceberg if a second (non-EMR) query engine joins this platform?\n4. How would you test a schema change to the clickstream event contract without breaking consumers?\n5. What SLOs would you define for streaming lag and gold table freshness, and how would you alert on them?\n6. If MSK ingestion falls behind for an hour then catches up, what do you need to verify about the gold table afterward?")
```

- [ ] **Step 3: Validate both files are well-formed JSON**

Run: `python3 -c "import json; [json.load(open(f'emr-notebooks/{n}.ipynb')) for n in ['05_observability_testing_performance','06_capstone_end_to_end']]"`
Expected: no output, exit code 0.

- [ ] **Step 4: Commit**

```bash
git add emr-notebooks/05_observability_testing_performance.ipynb emr-notebooks/06_capstone_end_to_end.ipynb
git commit -m "Add emr-notebooks/ 05,06: observability and capstone end-to-end"
```

---

## Verifying the whole plan

1. `python3 -c "import json,glob; [json.load(open(f)) for f in glob.glob('emr-notebooks/*.ipynb')]"` — all 8 files parse as valid JSON.
2. `ls emr-notebooks/` — exactly 8 `.ipynb` files, no leftover builder scripts.
3. `grep -rn "dbutils\|databricks.serviceCredential\|CREATE CATALOG\|CREATE VOLUME" emr-notebooks/` — returns nothing (no Databricks-specific syntax leaked in).
4. `grep -rln "CREATE DATABASE\|CREATE TABLE" emr-notebooks/*.ipynb` then manually confirm each matched notebook's `CREATE DATABASE`/non-`saveAsTable` `CREATE TABLE` includes a `LOCATION` clause — this is the one correctness property with no automated check available in this environment (no real Spark/EMR session), so it needs a deliberate human-eyeball pass per the Global Constraints' repeated warning.
5. `git diff --stat main` — only `emr-notebooks/*.ipynb` files added; `notebooks/`, `class/`, `src/` untouched.

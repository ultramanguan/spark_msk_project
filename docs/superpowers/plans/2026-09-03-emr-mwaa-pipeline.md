# EMR + MWAA Production Pipeline (Plan 2 of 5) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace `resources/jobs.yml` (Databricks Asset Bundle job) with plain Python Spark scripts (`emr_jobs/`) plus one MWAA Airflow DAG that creates an ephemeral EMR cluster, runs those scripts as EMR Steps in the same order `resources/jobs.yml` uses today, and always terminates the cluster afterward.

**Architecture:** Each `emr_jobs/*.py` script is a direct, Databricks-syntax-free port of the matching `notebooks/*.py` — `argparse` instead of `dbutils.widgets`, `SparkSession.builder` instead of Databricks' ambient `spark`, plain `CREATE DATABASE` (routed to Glue automatically once Glue Catalog is the configured metastore, from Plan 1's `emr_learning.tf`/DAG job-flow config) instead of Unity Catalog's `CREATE CATALOG/SCHEMA/VOLUME`. The DAG mirrors `resources/jobs.yml`'s current task graph exactly: `00 → 01 → 07 (fallback, since MSK isn't provisioned yet) → 03 → 06`, using `EmrCreateJobFlowOperator → EmrAddStepsOperator → EmrStepSensor (one per step) → EmrTerminateJobFlowOperator` (the last with `trigger_rule=ALL_DONE` so the cluster always terminates).

**Tech Stack:** PySpark (via `spark-submit`), `retail_lakehouse` package (unchanged), Apache Airflow (`apache-airflow-providers-amazon` operators), AWS EMR.

## Global Constraints

- `src/retail_lakehouse/*.py` do not change in this plan — these scripts only *consume* the package, exactly as the notebooks did.
- `notebooks/`, `class/`, `resources/jobs.yml`, `databricks.yml` are NOT touched by this plan (removing the Databricks bundle files is Plan 5's job).
- Job graph order must match `resources/jobs.yml`'s current state exactly: `00 → 01 → 07 → 03 → 06` (fallback in place of `02`, since MSK isn't provisioned — matches the comment already in `resources/jobs.yml`).
- EMR is NOT provisioned by Terraform per-run (Plan 1 already established this: ephemeral clusters are created dynamically by the DAG via the EMR API, not by Terraform).
- Reuses Plan 1's artifacts: the bootstrap script (`infra/terraform/bootstrap/install_retail_lakehouse.sh`) and its S3 key convention (`s3://<bucket>/artifacts/retail_lakehouse-latest.whl`), the EMR instance profile/service role names, and the `emr_msk_client` security group — all already exist from Plan 1's merged Terraform, but populating the actual account-specific values (bucket name, subnet ID, etc.) into Airflow Variables is a manual/deploy-time step, not something this plan's DAG code can read from Terraform state directly.
- EMR needs Delta Lake explicitly enabled per Spark job (unlike Databricks, which ships it built-in) — every `spark-submit` step must include the `io.delta:delta-spark_2.12:3.1.0` package plus the two `spark.sql.extensions`/`spark.sql.catalog.spark_catalog` configs, matching the pattern `tests/conftest.py` already established via `configure_spark_with_delta_pip`.
- Keep this small: 2 EMR core nodes, matching Plan 1's `var.emr_instance_count` default.

---

### Task 1: `emr_jobs/` — five production Spark scripts

**Files:**
- Create: `emr_jobs/00_environment_setup.py`
- Create: `emr_jobs/01_batch_lakehouse_bronze_silver_gold.py`
- Create: `emr_jobs/07_file_rate_streaming_fallback.py`
- Create: `emr_jobs/03_streaming_silver_gold_delta.py`
- Create: `emr_jobs/06_capstone_end_to_end.py`

**Interfaces:**
- Each script's CLI contract (consumed by Task 2's DAG, which invokes them via `spark-submit ... script.py --schema ... --base-path ...`):
  - All five accept `--schema` (default `retail_lakehouse`) and `--base-path` (required).
  - `03_streaming_silver_gold_delta.py` additionally accepts `--bronze-table` (default `bronze_clickstream_kafka`) — the DAG passes `--bronze-table bronze_clickstream_rate` since it runs after the `07` fallback, not the real Kafka ingest (matching `resources/jobs.yml`'s current `streaming_silver_gold` task).
  - Each script's `main()` raises/exits non-zero on failure (uncaught exception, or `06`'s explicit `sys.exit(1)` on a failed assertion) — this is what `ActionOnFailure: TERMINATE_CLUSTER` in Task 2's DAG relies on to detect a failed pipeline stage.
- Produces: no new `retail_lakehouse` package interfaces — these scripts only call existing, already-tested functions (`retail_lakehouse.config.PipelineConfig`, `.generate.*`, `.transformations.*`, `.streaming.*`, `.quality.assert_no_duplicate_keys`).

- [ ] **Step 1: Create `emr_jobs/00_environment_setup.py`**

Direct port of `notebooks/00_environment_setup.py`'s logic (skip its Databricks-only cells: the wheel-install bootstrap cell and the widget-fallback ceremony — this script's environment already has the wheel installed via Plan 1's EMR bootstrap action, and takes its parameters as plain CLI args instead of widgets). `CREATE CATALOG`/`CREATE VOLUME` calls are dropped entirely — Glue has no catalog/volume concept, only databases, and the job-flow's Glue Catalog configuration (Task 2) makes `CREATE DATABASE` route there automatically:

```python
"""EMR Step: environment setup — create the Glue database and seed source files.

Production port of notebooks/00_environment_setup.py, adapted for spark-submit on EMR
(argparse instead of dbutils widgets, Glue Data Catalog instead of Unity Catalog).
"""
import argparse
import random

from pyspark.sql import SparkSession

from retail_lakehouse.config import PipelineConfig
from retail_lakehouse.generate import synthetic_products, synthetic_customers, synthetic_events


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--schema", default="retail_lakehouse")
    parser.add_argument("--base-path", required=True)
    return parser.parse_args()


def main():
    args = parse_args()
    spark = SparkSession.builder.appName("00_environment_setup").getOrCreate()
    cfg = PipelineConfig(schema=args.schema, base_path=args.base_path)

    spark.sql(f"CREATE DATABASE IF NOT EXISTS `{cfg.schema}` LOCATION '{cfg.path('tables')}'")
    spark.sql(f"USE `{cfg.schema}`")
    spark.conf.set("spark.sql.shuffle.partitions", "8")
    print("Spark version:", spark.version)

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
    spark.sql(f"SHOW TABLES IN `{cfg.schema}`").show()


if __name__ == "__main__":
    main()
```

- [ ] **Step 2: Create `emr_jobs/01_batch_lakehouse_bronze_silver_gold.py`**

Port of `notebooks/01_batch_lakehouse_bronze_silver_gold.py`'s bronze/silver/gold logic (drop the `.explain("formatted")` query-plan cell — that's a notebook-only teaching aid, not part of the pipeline's actual work):

```python
"""EMR Step: batch medallion pipeline (bronze/silver/gold) from the seed files 00_environment_setup.py produces."""
import argparse

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from retail_lakehouse.config import PipelineConfig
from retail_lakehouse.transformations import (
    add_ingest_metadata, normalize_click_events, filter_valid_events,
    deduplicate_events, enrich_with_product_customer, revenue_by_hour,
)


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--schema", default="retail_lakehouse")
    parser.add_argument("--base-path", required=True)
    return parser.parse_args()


def main():
    args = parse_args()
    spark = SparkSession.builder.appName("01_batch_lakehouse_bronze_silver_gold").getOrCreate()
    cfg = PipelineConfig(schema=args.schema, base_path=args.base_path)
    spark.sql(f"USE `{cfg.schema}`")

    raw_events = spark.read.json(cfg.path("source", "events_json"))
    products = spark.table(cfg.table("dim_product_seed"))
    customers = spark.table(cfg.table("dim_customer_seed"))
    print("Raw event rows:", raw_events.count())

    bronze = add_ingest_metadata(raw_events, "batch_file_seed")
    bronze.write.mode("overwrite").format("delta").partitionBy("_ingest_date").saveAsTable(cfg.table("bronze_clickstream_batch"))

    silver = deduplicate_events(filter_valid_events(normalize_click_events(bronze)))
    silver.write.mode("overwrite").format("delta").partitionBy("event_date").saveAsTable(cfg.table("silver_clickstream_batch"))
    print("Silver rows:", spark.table(cfg.table("silver_clickstream_batch")).count())

    enriched = enrich_with_product_customer(spark.table(cfg.table("silver_clickstream_batch")), products, customers)
    gold = revenue_by_hour(enriched.withColumn(
        "is_purchase", F.col("event_type").isin("purchase", "checkout")
    ))
    gold.write.mode("overwrite").format("delta").saveAsTable(cfg.table("gold_revenue_by_hour_batch"))

    print("Batch medallion pipeline complete")


if __name__ == "__main__":
    main()
```

- [ ] **Step 3: Create `emr_jobs/07_file_rate_streaming_fallback.py`**

Port of `notebooks/07_file_rate_streaming_fallback.py` — this is the step that runs in place of the real Kafka ingest until MSK is provisioned, matching `resources/jobs.yml`'s current `streaming_ingest_fallback` task:

```python
"""EMR Step: file/rate streaming fallback (no MSK required) — same mechanics as the real Kafka ingest."""
import argparse

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from retail_lakehouse.config import PipelineConfig
from retail_lakehouse.transformations import add_ingest_metadata


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--schema", default="retail_lakehouse")
    parser.add_argument("--base-path", required=True)
    return parser.parse_args()


def main():
    args = parse_args()
    spark = SparkSession.builder.appName("07_file_rate_streaming_fallback").getOrCreate()
    cfg = PipelineConfig(schema=args.schema, base_path=args.base_path)
    spark.sql(f"USE `{cfg.schema}`")

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


if __name__ == "__main__":
    main()
```

- [ ] **Step 4: Create `emr_jobs/03_streaming_silver_gold_delta.py`**

Port of `notebooks/03_streaming_silver_gold_delta.py` — watermarked dedup, enrichment, and the idempotent gold MERGE upsert:

```python
"""EMR Step: streaming silver/gold with watermarking, de-dup, and idempotent upsert."""
import argparse

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from retail_lakehouse.config import PipelineConfig
from retail_lakehouse.streaming import deduplicate_stream, windowed_revenue, make_gold_upsert
from retail_lakehouse.transformations import normalize_click_events, enrich_with_product_customer


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--schema", default="retail_lakehouse")
    parser.add_argument("--base-path", required=True)
    parser.add_argument("--bronze-table", default="bronze_clickstream_kafka")
    return parser.parse_args()


def main():
    args = parse_args()
    spark = SparkSession.builder.appName("03_streaming_silver_gold_delta").getOrCreate()
    cfg = PipelineConfig(schema=args.schema, base_path=args.base_path)
    spark.sql(f"USE `{cfg.schema}`")

    if not spark.catalog.tableExists(f"{cfg.schema}.{args.bronze_table}"):
        raise ValueError(
            f"{args.bronze_table} does not exist yet. Run the Kafka ingest step (or the "
            "file/rate fallback step and pass --bronze-table bronze_clickstream_rate) first."
        )

    bronze_stream = spark.readStream.table(cfg.table(args.bronze_table))
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

    gold_table = cfg.table("gold_revenue_windows_streaming")
    spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {gold_table} (
      window STRUCT<start: TIMESTAMP, end: TIMESTAMP>,
      category STRING,
      orders LONG,
      revenue DOUBLE
    ) USING DELTA
    LOCATION '{cfg.path("tables", "gold_revenue_windows_streaming")}'
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
    print("Streaming silver/gold pipeline complete")


if __name__ == "__main__":
    main()
```

- [ ] **Step 5: Create `emr_jobs/06_capstone_end_to_end.py`**

Port of `notebooks/06_capstone_end_to_end.py` — note this script must `sys.exit(1)` on a failed validation (unlike the notebook, which just raises inside a Databricks Job task), since that non-zero exit is what makes `ActionOnFailure: TERMINATE_CLUSTER` (Task 2) actually detect the failure:

```python
"""EMR Step: capstone end-to-end validation — fails the pipeline (non-zero exit) on a bad result."""
import argparse
import random
import sys

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from retail_lakehouse.config import PipelineConfig
from retail_lakehouse.generate import synthetic_products, synthetic_customers, synthetic_events
from retail_lakehouse.transformations import (
    normalize_click_events, filter_valid_events, deduplicate_events,
    enrich_with_product_customer, revenue_by_hour, add_ingest_metadata,
)
from retail_lakehouse.quality import assert_no_duplicate_keys


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--schema", default="retail_lakehouse")
    parser.add_argument("--base-path", required=True)
    return parser.parse_args()


def main():
    args = parse_args()
    spark = SparkSession.builder.appName("06_capstone_end_to_end").getOrCreate()
    cfg = PipelineConfig(schema=args.schema, base_path=args.base_path)
    spark.sql(f"USE `{cfg.schema}`")
    random.seed(11)

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

    try:
        assert spark.table(cfg.table("capstone_silver_events")).count() > 0, "silver produced no rows"
        assert spark.table(cfg.table("capstone_gold_revenue")).count() > 0, "gold produced no rows"
        assert_no_duplicate_keys(spark.table(cfg.table("capstone_silver_events")), ["event_id"])
    except (AssertionError, ValueError) as exc:
        print(f"Capstone validation FAILED: {exc}", file=sys.stderr)
        sys.exit(1)

    print("Capstone pipeline validated successfully.")


if __name__ == "__main__":
    main()
```

- [ ] **Step 6: Lint all five files**

Run: `ruff check emr_jobs/` (use `/tmp/rl_venv/bin/ruff` if a bare `ruff` isn't on PATH). Fix anything it flags — these are new files, they should start lint-clean. Note `ruff.toml`/`pyproject.toml`'s `[tool.ruff] src = ["src", "tests"]` doesn't include `emr_jobs/`; that's fine for this task (an import-sorting default may differ slightly) — just make sure there are no real errors (undefined names, unused imports).

- [ ] **Step 7: Commit**

```bash
git add emr_jobs/
git commit -m "Add emr_jobs/ production Spark scripts, ported from Databricks notebooks"
```

---

### Task 2: `airflow/dags/retail_lakehouse_pipeline.py` — the MWAA DAG

**Files:**
- Create: `airflow/dags/retail_lakehouse_pipeline.py`

**Interfaces:**
- Consumes: the five `emr_jobs/*.py` scripts from Task 1 (invoked by filename via `spark-submit`), Plan 1's Terraform outputs (`emr_instance_profile_name`, `emr_service_role_name`, `emr_msk_client_security_group_id`) — read at DAG-parse time from Airflow Variables (populated manually or by a later deploy step, not from Terraform state directly), and the bootstrap script/wheel S3 key convention Plan 1 established (`s3://<bucket>/bootstrap/install_retail_lakehouse.sh`, `s3://<bucket>/artifacts/retail_lakehouse-latest.whl`).
- Produces: one Airflow DAG, `dag_id="retail_lakehouse_pipeline"`, with tasks `create_emr_cluster → add_steps → wait_for_<step> (one per step, chained) → terminate_emr_cluster`.

- [ ] **Step 1: Create `airflow/dags/retail_lakehouse_pipeline.py`**

```python
"""Production pipeline DAG: creates an ephemeral EMR cluster, runs the emr_jobs/ scripts as EMR
Steps in the same order resources/jobs.yml (the now-retired Databricks Job) used, then always
terminates the cluster.

Account-specific values come from Airflow Variables rather than Terraform state directly, since
this file is parsed by the Airflow scheduler independently of any `terraform apply`. Set these once
per environment (Airflow UI -> Admin -> Variables, or `airflow variables set`):
  - retail_lakehouse_bucket            (from `terraform output lakehouse_bucket_name`)
  - retail_lakehouse_subnet_id         (one of the subnet_ids passed to Terraform)
  - retail_lakehouse_base_path         (e.g. s3://<bucket>/data)
  - retail_lakehouse_emr_msk_client_sg (from `terraform output emr_msk_client_security_group_id`)
Optional, with defaults matching Plan 1's Terraform variable defaults:
  - retail_lakehouse_emr_instance_profile (default: retail-lakehouse-dev-emr-instance-profile)
  - retail_lakehouse_emr_service_role     (default: retail-lakehouse-dev-emr-service-role)
  - retail_lakehouse_emr_release_label    (default: emr-7.5.0)
  - retail_lakehouse_emr_instance_type    (default: m5.xlarge)
  - retail_lakehouse_schema               (default: retail_lakehouse)
"""
from datetime import datetime

from airflow import DAG
from airflow.models import Variable
from airflow.providers.amazon.aws.operators.emr import (
    EmrAddStepsOperator,
    EmrCreateJobFlowOperator,
    EmrTerminateJobFlowOperator,
)
from airflow.providers.amazon.aws.sensors.emr import EmrStepSensor
from airflow.utils.trigger_rule import TriggerRule

LAKEHOUSE_BUCKET = Variable.get("retail_lakehouse_bucket")
SUBNET_ID = Variable.get("retail_lakehouse_subnet_id")
BASE_PATH = Variable.get("retail_lakehouse_base_path")
EMR_MSK_CLIENT_SG = Variable.get("retail_lakehouse_emr_msk_client_sg")
EMR_INSTANCE_PROFILE = Variable.get(
    "retail_lakehouse_emr_instance_profile", default_var="retail-lakehouse-dev-emr-instance-profile"
)
EMR_SERVICE_ROLE = Variable.get(
    "retail_lakehouse_emr_service_role", default_var="retail-lakehouse-dev-emr-service-role"
)
EMR_RELEASE_LABEL = Variable.get("retail_lakehouse_emr_release_label", default_var="emr-7.5.0")
EMR_INSTANCE_TYPE = Variable.get("retail_lakehouse_emr_instance_type", default_var="m5.xlarge")
SCHEMA = Variable.get("retail_lakehouse_schema", default_var="retail_lakehouse")

WHEEL_S3_URI = f"s3://{LAKEHOUSE_BUCKET}/artifacts/retail_lakehouse-latest.whl"
BOOTSTRAP_S3_URI = f"s3://{LAKEHOUSE_BUCKET}/bootstrap/install_retail_lakehouse.sh"
SCRIPTS_S3_PREFIX = f"s3://{LAKEHOUSE_BUCKET}/emr_jobs"

# EMR doesn't ship Delta Lake built-in (unlike Databricks) -- every step needs this explicitly.
DELTA_SPARK_CONF = [
    "--packages", "io.delta:delta-spark_2.12:3.1.0",
    "--conf", "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension",
    "--conf", "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog",
    "--conf", f"spark.sql.warehouse.dir={BASE_PATH}/tables",
]


def spark_step(name, script, extra_args=None):
    args = (
        ["spark-submit", "--deploy-mode", "cluster"]
        + DELTA_SPARK_CONF
        + [f"{SCRIPTS_S3_PREFIX}/{script}", "--schema", SCHEMA, "--base-path", BASE_PATH]
        + (extra_args or [])
    )
    return {
        "Name": name,
        "ActionOnFailure": "TERMINATE_CLUSTER",
        "HadoopJarStep": {"Jar": "command-runner.jar", "Args": args},
    }


JOB_FLOW_OVERRIDES = {
    "Name": "retail-lakehouse-pipeline",
    "ReleaseLabel": EMR_RELEASE_LABEL,
    "Applications": [{"Name": "Spark"}, {"Name": "Hadoop"}],
    "Configurations": [
        {
            "Classification": "hive-site",
            "Properties": {
                "hive.metastore.client.factory.class": "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory"
            },
        },
        {
            "Classification": "spark-hive-site",
            "Properties": {
                "hive.metastore.client.factory.class": "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory"
            },
        },
    ],
    "Instances": {
        "InstanceGroups": [
            {"Name": "Master", "InstanceRole": "MASTER", "InstanceType": EMR_INSTANCE_TYPE, "InstanceCount": 1},
            {"Name": "Core", "InstanceRole": "CORE", "InstanceType": EMR_INSTANCE_TYPE, "InstanceCount": 2},
        ],
        "Ec2SubnetId": SUBNET_ID,
        "AdditionalMasterSecurityGroups": [EMR_MSK_CLIENT_SG],
        "AdditionalSlaveSecurityGroups": [EMR_MSK_CLIENT_SG],
        "KeepJobFlowAliveWhenNoSteps": True,
        "TerminationProtected": False,
    },
    "BootstrapActions": [
        {"Name": "install-retail-lakehouse", "ScriptBootstrapAction": {"Path": BOOTSTRAP_S3_URI, "Args": [WHEEL_S3_URI]}},
    ],
    "JobFlowRole": EMR_INSTANCE_PROFILE,
    "ServiceRole": EMR_SERVICE_ROLE,
    "VisibleToAllUsers": True,
}

# Same order as resources/jobs.yml's task graph: 00 -> 01 -> 07 (fallback) -> 03 -> 06.
STEPS = [
    spark_step("00_environment_setup", "00_environment_setup.py"),
    spark_step("01_batch_medallion", "01_batch_lakehouse_bronze_silver_gold.py"),
    spark_step("02_streaming_ingest_fallback", "07_file_rate_streaming_fallback.py"),
    spark_step(
        "03_streaming_silver_gold", "03_streaming_silver_gold_delta.py",
        extra_args=["--bronze-table", "bronze_clickstream_rate"],
    ),
    spark_step("04_capstone", "06_capstone_end_to_end.py"),
]

with DAG(
    dag_id="retail_lakehouse_pipeline",
    description="Retail lakehouse production pipeline on ephemeral EMR, orchestrated by MWAA",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["retail-lakehouse"],
) as dag:
    create_cluster = EmrCreateJobFlowOperator(
        task_id="create_emr_cluster",
        job_flow_overrides=JOB_FLOW_OVERRIDES,
    )

    add_steps = EmrAddStepsOperator(
        task_id="add_steps",
        job_flow_id=create_cluster.output,
        steps=STEPS,
    )

    step_sensors = [
        EmrStepSensor(
            task_id=f"wait_for_{step['Name']}",
            job_flow_id=create_cluster.output,
            step_id=add_steps.output[i],
        )
        for i, step in enumerate(STEPS)
    ]

    terminate_cluster = EmrTerminateJobFlowOperator(
        task_id="terminate_emr_cluster",
        job_flow_id=create_cluster.output,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    create_cluster >> add_steps >> step_sensors[0]
    for upstream, downstream in zip(step_sensors, step_sensors[1:]):
        upstream >> downstream
    step_sensors[-1] >> terminate_cluster
```

- [ ] **Step 2: Verify the DAG file is at least syntactically valid Python**

Run: `python3 -m py_compile airflow/dags/retail_lakehouse_pipeline.py`
Expected: no output, exit code 0. (A full `airflow dags list-import-errors`-style check needs the `apache-airflow` package installed, which isn't part of this project's own dependencies — `py_compile` is the syntax-level check available without adding that dependency here. If `apache-airflow-providers-amazon` happens to be installed in your environment, `python3 -c "import airflow.providers.amazon.aws.operators.emr"` is worth trying too, but don't add it as a new project dependency for this check alone.)

- [ ] **Step 3: Lint the file**

Run: `ruff check airflow/`. Fix anything it flags.

- [ ] **Step 4: Commit**

```bash
git add airflow/
git commit -m "Add MWAA Airflow DAG orchestrating the production pipeline on ephemeral EMR"
```

---

## Verifying the whole plan

1. `ruff check emr_jobs/ airflow/` — clean.
2. `python3 -m py_compile airflow/dags/retail_lakehouse_pipeline.py` and `python3 -m py_compile emr_jobs/*.py` — all exit 0.
3. `pytest -q` from the repo root — unaffected by this plan (no changes to `src/retail_lakehouse/` or `tests/`); same pre-existing PySpark/JVM-gateway sandbox limitation as before, not a regression.
4. Confirm no file outside `emr_jobs/` and `airflow/` was touched: `git diff --stat main` should show only new files under those two directories.

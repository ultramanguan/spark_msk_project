# Migrate production pipeline from Databricks to EMR + MWAA + MSK (AWS-only)

## Context

The production pipeline currently runs on Databricks (Asset Bundle jobs, Unity Catalog, serverless
compute). Getting MSK IAM auth working on Databricks serverless compute turned out to require
workarounds (Unity Catalog service credentials, a two-phase self-assuming IAM trust policy, uncertain
serverless networking) that add real operational complexity. This project replaces the Databricks
production path with an all-AWS stack: EMR for compute, MWAA (Airflow) for orchestration, MSK for
streaming (unchanged), S3 for storage (unchanged).

Two separate concerns are being kept explicitly apart:

- **Production execution** — must be manageable and run by MWAA. Plain Spark scripts, not notebooks.
- **Teaching/learning** — interactive Jupyter notebooks on EMR, for step-by-step exploration only. Not
  part of the production execution path.

The existing Databricks material (`class/`, `notebooks/`, `resources/jobs.yml`, `databricks.yml`,
`.github/workflows/deploy_databricks.yml`) is a working reference and is **not deleted**, except for the
Databricks deploy plumbing that is specifically superseded by the new production path (see "Removed").
`class/` in particular stays completely untouched.

## Goals

- Production pipeline runs entirely on AWS: EMR (ephemeral, per-run clusters) orchestrated by MWAA,
  reading/writing MSK and S3, with table metadata in AWS Glue Data Catalog.
- `src/retail_lakehouse/` (schemas, transformations, streaming, quality, generate) stays the shared,
  tested package for both the production scripts and the new interactive notebooks — no logic fork.
- A new interactive notebook track (`emr-notebooks/`) mirrors today's `notebooks/00`-`07` for
  step-by-step learning/dev on an EMR JupyterHub cluster.
- A new teaching-concepts track (`class-emr/`) mirrors today's `class/01`-`04` Databricks concept
  notebooks, adapted for plain Jupyter/PySpark on EMR.
- MSK auth reverts to a plain EC2 instance profile (no service credentials, no serverless networking
  workarounds) since EMR is classic EC2-based.

## Non-goals

- No changes to `class/` or `notebooks/` (Databricks) — they remain as-is, a working reference.
- No scheduled/automated path for the new interactive notebooks — they are manually run, like the
  Databricks notebooks have been all along.
- No Iceberg/Hudi migration — table format stays Delta Lake (`delta-spark`), since it already works on
  EMR and this avoids rewriting `src/retail_lakehouse`'s schemas/streaming/MERGE logic.
- No change to `tests/` or CI's lint/test/build steps — the package under test doesn't change.

## Architecture

### Infrastructure (`infra/terraform/`)

- **`s3.tf`** — unchanged.
- **`msk.tf`** — small but necessary change: the ingress rule currently allows traffic from
  `var.databricks_security_group_id`, an externally-supplied SG belonging to a Databricks cluster this
  project no longer has. Replaced with a Terraform-managed `aws_security_group.emr_msk_client` — a
  no-rules "marker" SG that any EMR cluster needing MSK access attaches as an additional security group
  (the persistent learning cluster in `emr_learning.tf`, and the ephemeral production clusters MWAA
  creates later). Its ID is exposed as a new output (`emr_msk_client_security_group_id`) for the Airflow
  DAG (Plan 2) to reference. `var.databricks_security_group_id` is removed from `variables.tf` and
  `terraform.tfvars.example`.
- **`iam.tf`** — replaced. Drops the Unity Catalog service-credential role (self-assuming trust
  policy, External ID dance) entirely. Replaced with:
  - `aws_iam_role` for EMR's EC2 instance profile (trust policy: `ec2.amazonaws.com`)
  - `aws_iam_instance_profile` wrapping that role
  - `aws_iam_role_policy` granting the same `kafka-cluster:*` actions as before (Connect, topic
    read/write, consumer group), attached directly to the instance-profile role
  - Additional policy statements for S3 (data bucket read/write) and Glue Data Catalog
    (`glue:GetDatabase`, `glue:GetTable`, `glue:CreateTable`, etc.) — needed because EMR uses this same
    instance profile for all AWS access, unlike Databricks which separated storage/service credentials.
- **New: `emr_learning.tf`** — one `aws_emr_cluster` resource for interactive Jupyter:
  - `applications = ["Spark", "JupyterHub", "Livy"]`
  - Glue Data Catalog enabled via `configurations_json` (`hive.metastore.client.factory.class` = Glue's
    `AWSGlueDataCatalogHiveClientFactory`)
  - Bootstrap action: a script (checked into `infra/terraform/bootstrap/install_retail_lakehouse.sh`)
    that `pip3 install`s the wheel from a fixed S3 key (`s3://<bucket>/artifacts/retail_lakehouse-latest.whl`)
  - EC2 instance profile from `iam.tf`
  - Tagged/named consistently with existing `project_name`/`environment` conventions; same "this costs
    money, destroy when done" pattern as MSK, documented in `infra/terraform/README.md`
- **New: `mwaa.tf`** — `aws_mwaa_environment` plus its required S3 prefix structure
  (`dags/`, `plugins/`, `requirements.txt`) in the existing lakehouse bucket (or a new small MWAA-only
  bucket — see Open question 1), an MWAA execution role (permissions: `elasticmapreduce:*` for
  create/add-steps/terminate/describe, `iam:PassRole` for the EMR instance profile role, S3 access to
  the DAGs/artifacts bucket, CloudWatch Logs).
- **Removed**: nothing in `infra/terraform/` is removed — this is additive. (The Databricks Asset
  Bundle files removed are outside `infra/terraform/`, see below.)

### Production pipeline (MWAA + EMR Steps)

- **New: `emr_jobs/`** — plain Python Spark scripts, direct ports of the logic in
  `notebooks/00`-`07`, Databricks-specific code stripped:
  - `00_environment_setup.py`, `01_batch_lakehouse_bronze_silver_gold.py`,
    `02_kafka_msk_streaming_ingest.py`, `03_streaming_silver_gold_delta.py`,
    `06_capstone_end_to_end.py`, `07_file_rate_streaming_fallback.py` (mirrors the current default job
    graph, which uses `07`'s fallback in place of `02` until MSK is provisioned — see
    `resources/jobs.yml`'s current state)
  - Each script takes CLI args via `argparse` (`--catalog` dropped, `--schema`, `--base-path`,
    `--kafka-bootstrap-servers`, `--kafka-topic`, `--bronze-table` as needed) instead of Databricks
    widgets, matching the parameters `resources/jobs.yml` used to pass as `base_parameters`
  - Each script calls `spark = SparkSession.builder.appName(...).getOrCreate()` directly (no ambient
    `spark`/`dbutils` the way Databricks notebooks have)
  - Unity Catalog `CREATE CATALOG/SCHEMA/VOLUME` calls become `spark.sql("CREATE DATABASE IF NOT EXISTS
    ...")` (routed to Glue automatically once Glue Catalog is the configured metastore) plus plain S3
    paths from `PipelineConfig` (see package changes below)
- **New: `airflow/dags/retail_lakehouse_pipeline.py`** — one MWAA DAG:
  1. `EmrCreateJobFlowOperator` — ephemeral cluster, same instance sizing intent as the old Databricks
     job cluster (`i3.xlarge` x2 equivalent), Glue Catalog enabled, bootstrap action installs the wheel
     from S3, EC2 instance profile from `iam.tf`
  2. `EmrAddStepsOperator` — submits `00 → 01 → 07 (fallback) → 03 → 06` as sequential
     `spark-submit` steps, same dependency order `resources/jobs.yml` used
  3. `EmrStepSensor` per step, failing the DAG on step failure
  4. `EmrTerminateJobFlowOperator` with `trigger_rule=TriggerRule.ALL_DONE` so the cluster always
     terminates, success or failure
  - `kafka_bootstrap_servers`/`kafka_topic` are DAG-level Airflow Variables (or `params`), matching
     today's fail-fast-if-unset behavior in notebook `02` — the DAG task raises before creating the EMR
     cluster if unset and the step graph still targets `02` instead of `07`.

### Package changes (`src/retail_lakehouse/`)

- **`config.py`** — `PipelineConfig.catalog` field removed (Glue has no catalog level); `schema` becomes
  the Glue database name directly. Table/checkpoint path helpers (`table()`, `checkpoint()`, `path()`)
  updated accordingly. This is the only package file that needs to change — `transformations.py`,
  `streaming.py`, `quality.py`, `schemas.py`, `generate.py` are pure PySpark/Delta and untouched.
- **`tests/`** — updated only where they reference `PipelineConfig(catalog=...)`; no test logic changes
  since the underlying transform/streaming functions don't change.

### Interactive learning notebooks (`emr-notebooks/`)

New directory, parallel to (not replacing) `notebooks/`. Same 8-stage structure and underlying
`retail_lakehouse` calls, adapted for EMR JupyterHub:

- No bootstrap-install cell — the persistent Jupyter cluster's bootstrap action already installs the
  wheel, so notebooks `import retail_lakehouse` directly.
- No `dbutils.widgets` — plain variables in the first cell instead.
- No Unity Catalog SQL — `spark.sql("CREATE DATABASE IF NOT EXISTS ...")` against Glue.
- `spark`/`sc` already in scope via EMR's Jupyter/Sparkmagic kernel, same convenience as Databricks.
- Kafka auth reverts to plain `kafka.security.protocol=SASL_SSL` /
  `kafka.sasl.mechanism=AWS_MSK_IAM` config — no service-credential option needed, since the instance
  profile supplies credentials ambiently to every process on the cluster.

### New teaching-concepts notebooks (`class-emr/`)

New directory, parallel to (not replacing) `class/`. Mirrors `class/01`-`04` topics (Spark batch, Spark
Structured Streaming, Kafka/MSK, Delta+S3 lakehouse) with the same concept-first, self-contained,
runnable style, rewritten for plain Jupyter/PySpark on EMR: plain Markdown cells instead of `%md` magic,
no Unity Catalog Volumes references, no `dbutils`.

### CI/CD

- **`ci.yml`** — unchanged (lints/tests/builds the same package).
- **Removed**: `.github/workflows/deploy_databricks.yml`, `databricks.yml`, `resources/jobs.yml` — the
  Databricks Asset Bundle deploy path is fully superseded by MWAA + EMR for production. `class/` and
  `notebooks/` (Databricks) are unaffected by this removal since they don't depend on the Asset Bundle
  to run interactively.
- **New: `.github/workflows/deploy_aws.yml`** — on push to main (or manual dispatch): build the wheel
  (already done in `ci.yml`'s pattern), upload it to the fixed S3 artifact key the EMR bootstrap actions
  reference, and sync `airflow/dags/` to MWAA's DAGs S3 prefix. Does **not** run `terraform apply` — see
  "Decisions on infra details" below.

## Decisions on infra details

- **MWAA's S3 layout**: reuses the existing lakehouse bucket rather than a new bucket, under an
  `airflow/` prefix (`airflow/dags/`, `airflow/plugins/`, `airflow/requirements.txt`), alongside the
  existing `data/tables`, `data/checkpoints`, `data/source` prefixes documented in `s3.tf`. Avoids
  another bucket to manage/tear down.
- **`terraform apply` stays manual**, not run automatically by `deploy_aws.yml`. Every piece of infra in
  this project (S3, MSK, and now EMR/MWAA) has been treated as a manual, reviewed, real-money action
  throughout this project's `RUNBOOK.md` — `deploy_aws.yml` only builds/uploads the wheel and syncs
  `airflow/dags/`, matching the boundary the Databricks deploy workflow drew between build/deploy and
  infra provisioning.
- **EMR release/sizing**: EMR 7.x (latest stable at apply time), `m5.xlarge` x2 for both the ephemeral
  production cluster and the persistent learning cluster — comparable cost/capacity to the `i3.xlarge` x2
  Databricks job cluster this replaces. Exposed as Terraform variables (`emr_release_label`,
  `emr_instance_type`, `emr_instance_count`) so this is adjustable without editing resource definitions.

## Testing

No changes to `tests/` logic — `PipelineConfig` call sites in tests get updated for the removed
`catalog` field, and the same `pytest`/`ruff`/`delta-spark`+`configure_spark_with_delta_pip` local test
setup applies to `emr_jobs/` and `emr-notebooks/`'s use of `src/retail_lakehouse` (nothing new to test
there beyond what's already covered — the new scripts are thin CLI/argument-parsing wrappers around the
same tested transform functions).

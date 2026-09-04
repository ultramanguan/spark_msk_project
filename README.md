# Project 1 — Retail Lakehouse: Batch, Streaming, Kafka/MSK, Delta

This project has two parallel tracks, teaching the same concepts on two different platforms:

1. **Databricks track** (legacy/reference — kept working, not actively deployed):
   - `class/` — concept-first teaching notebooks (Spark batch, Structured Streaming, Kafka/MSK, the data lakehouse).
   - `notebooks/` — the production retail clickstream pipeline, orchestrated as a Databricks Job.
   - See `RUNBOOK.md` for the exact Databricks setup steps.
2. **AWS-native track** (current — this is what actually gets deployed):
   - `class-emr/` — the same teaching notebooks, ported for EMR JupyterHub.
   - `emr-notebooks/` — the same production pipeline, ported for interactive step-by-step learning on EMR JupyterHub.
   - `emr_jobs/` + `airflow/dags/` — the actual production pipeline: plain Spark scripts run as EMR Steps, orchestrated by an MWAA (managed Airflow) DAG on an ephemeral EMR cluster.
   - `infra/terraform/` — S3, MSK Serverless, EMR (a persistent learning cluster + the IAM/security groups ephemeral clusters attach), and the MWAA environment.

Both tracks share the same underlying package (`src/retail_lakehouse/`) and the same scenario:

- Stream clickstream events from **Amazon MSK** into a **bronze** Delta table on **S3**.
- Clean, deduplicate, and enrich into a **silver** Delta table (streaming, with watermarking).
- Aggregate into **gold** business tables using `foreachBatch`/batch `MERGE` for idempotent upserts.
- Also demonstrate the equivalent **batch** path (files → bronze/silver/gold) for direct comparison.

## Why this split

Notebooks are great for teaching and exploration but bad for testing, review, and reuse. The `class*/` notebooks intentionally contain a lot of inline logic and commentary — they're meant to be read. The production code in `src/retail_lakehouse/` contains none of that: plain, tested, importable PySpark functions. `notebooks/`/`emr-notebooks/` orchestrate the package interactively; `emr_jobs/` + the MWAA DAG orchestrate it as the actual scheduled production pipeline. None of them reimplement the package's logic.

## Repository layout

```text
project1/
├── class/                       # Databricks teaching notebooks (legacy/reference)
├── class-emr/                   # Same teaching content, ported for EMR JupyterHub
├── notebooks/                   # Databricks production pipeline notebooks (legacy/reference)
├── emr-notebooks/                # Same production pipeline, ported for interactive EMR JupyterHub use
├── emr_jobs/                    # Plain Spark scripts run as EMR Steps -- the actual production pipeline
├── airflow/dags/                 # MWAA DAG orchestrating emr_jobs/ on an ephemeral EMR cluster
├── src/retail_lakehouse/         # Installable Python package: schemas, transforms, streaming, quality
├── tests/                       # pytest unit tests for the package (local PySpark)
├── infra/terraform/               # AWS S3 + MSK + EMR + MWAA + IAM as code
├── scripts/                     # Local dev, deploy, and teardown helpers
├── .github/workflows/            # CI (test/build/lint) and CD (deploy_aws.yml)
├── pyproject.toml                # Package build/test config
└── RUNBOOK.md                    # Step-by-step run order for the Databricks track
```

## Scenario

A retail company ingests clickstream and order events. Product and customer dimensions arrive as batch files. The pipeline streams from MSK into bronze, cleans/dedupes/enriches into silver, and aggregates into gold — demonstrated on both platforms, with the AWS-native track (EMR + MWAA) being what's actually deployed day to day.

## Prerequisites

- AWS account with permission to create S3 buckets, an MSK cluster, EMR clusters, an MWAA environment, and IAM roles (see `infra/terraform/`).
- Python 3.10+ locally for packaging/tests.
- AWS CLI configured locally if you want to run `terraform apply`/`deploy_aws.yml`'s steps by hand.
- (Databricks track only) An AWS Databricks workspace with Unity Catalog enabled, Runtime 15.4 LTS+, and the Databricks CLI configured — see `RUNBOOK.md`.

## Where to start

1. **New to the concepts?** Read `class-emr/01_spark_batch_processing.ipynb` through `04_data_lakehouse_delta_s3.ipynb` on an EMR JupyterHub cluster (see `infra/terraform/emr_learning.tf`), or the Databricks-flavored `class/` if you're on that platform instead.
2. **Want the production pipeline running?** `infra/terraform/` provisions everything (see `infra/terraform/README.md` — this costs real money, read it before applying). Once applied, `deploy_aws.yml` (or its manual equivalent) publishes the wheel and syncs `emr_jobs/`/`airflow/dags/`, and the MWAA DAG `retail_lakehouse_pipeline` runs the pipeline end to end.
3. **Want to step through the pipeline interactively instead of watching it run as a scheduled job?** `emr-notebooks/00_environment_setup.ipynb` through `06_capstone_end_to_end.ipynb`, in order, on the same EMR JupyterHub cluster.
4. **Working on the Databricks track specifically?** `RUNBOOK.md` has the exact Databricks setup steps; it predates the AWS-native track and stays accurate for that platform.

# Project 1 — AWS Databricks Lakehouse: Batch, Streaming, Kafka/MSK, Delta

This project has two parts:

1. **`class/`** — concept-first teaching notebooks. Each one explains a subject (Spark batch, Spark Structured Streaming, Kafka/MSK, the data lakehouse) from first principles, with diagrams-in-text, runnable examples, and "why it exists" narrative. Read/run these before touching the production project if the concepts are new to you.
2. **Production project** (`notebooks/`, `src/`, `infra/`, `resources/`, `.github/`) — a real, step-by-step retail clickstream lakehouse pipeline built for **AWS Databricks + Amazon MSK (Kafka) + Amazon S3 + Delta Lake**, using **Spark Structured Streaming** end to end. This mirrors how a production data engineering team would actually structure the repo: reusable PySpark logic packaged as a wheel, notebooks that orchestrate it, Terraform for the AWS/MSK/S3 footprint, Databricks Asset Bundles for deployment, and CI/CD.

## Why this split

Notebooks are great for teaching and exploration but bad for testing, review, and reuse. Concept notebooks in `class/` intentionally contain a lot of inline logic and commentary. The production code in `src/` contains none of that — it's plain, tested, importable PySpark functions. The `notebooks/` folder orchestrates the package; it doesn't reimplement it.

## Repository layout

```text
project1/
├── class/                      # Concept-teaching notebooks (Databricks-importable .py)
│   ├── 01_spark_batch_processing.py
│   ├── 02_spark_structured_streaming.py
│   ├── 03_kafka_and_msk.py
│   └── 04_data_lakehouse_delta_s3.py
├── notebooks/                  # Production pipeline notebooks, run in order 00-09
├── src/retail_lakehouse/       # Installable Python package: schemas, transforms, streaming, quality
├── tests/                      # pytest unit tests for the package (local PySpark)
├── infra/terraform/            # AWS S3 + MSK + IAM as code
├── resources/jobs.yml          # Databricks Asset Bundle job graph
├── databricks.yml              # Bundle definition (dev/prod targets)
├── scripts/                    # Local dev, deploy, and teardown helpers
├── .github/workflows/          # CI (test/build) and CD (bundle deploy)
├── pyproject.toml              # Package build/test config
└── RUNBOOK.md                  # Step-by-step run order and talking points
```

## Scenario

A retail company ingests clickstream and order events. Product and customer dimensions arrive as batch files. The pipeline:

- Streams clickstream events from **Amazon MSK** with Spark Structured Streaming into a **bronze** Delta table on **S3**.
- Cleans, deduplicates, and enriches into a **silver** Delta table (streaming, with watermarking).
- Aggregates into **gold** business tables (revenue by hour/category/region) using `foreachBatch` + `MERGE` for idempotent upserts.
- Also demonstrates the equivalent **batch** path (files → bronze/silver/gold) so the two paradigms can be compared directly.
- Ships as a wheel, deployed via Databricks Asset Bundles, tested and built in CI.

## Prerequisites

- AWS account with permission to create S3 buckets, an MSK cluster (or MSK Serverless), and IAM roles (see `infra/terraform/`).
- AWS Databricks workspace with Unity Catalog enabled, Runtime 15.4 LTS+.
- Python 3.10+ locally for packaging/tests.
- Databricks CLI configured (`databricks configure`) for bundle deploys.

## Where to start

1. Read `class/01_spark_batch_processing.py` through `class/04_data_lakehouse_delta_s3.py` (or import them into Databricks and run cell by cell) if you want the concepts first.
2. Read `RUNBOOK.md` for the exact step-by-step to provision infra and run the production notebooks in order.
3. See `infra/terraform/README.md` before creating any real AWS resources — it costs money (MSK in particular).

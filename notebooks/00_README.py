# Databricks notebook source
# MAGIC %md
# MAGIC # Retail Lakehouse Notebooks
# MAGIC
# MAGIC Run these in order. `02` requires MSK to be provisioned (`infra/terraform/`); use `07` as a
# MAGIC no-infra fallback while working through the rest of the pipeline.
# MAGIC
# MAGIC 1. `00_environment_setup` — create schema, generate seed data.
# MAGIC 2. `01_batch_lakehouse_bronze_silver_gold` — batch medallion pipeline from seed files.
# MAGIC 3. `02_kafka_msk_streaming_ingest` — real MSK -> Spark Structured Streaming -> bronze Delta.
# MAGIC 4. `03_streaming_silver_gold_delta` — watermarking, de-dup, enrichment, idempotent MERGE upsert to gold.
# MAGIC 5. `04_delta_production_patterns_iceberg` — MERGE, time travel, OPTIMIZE/VACUUM, Iceberg comparison.
# MAGIC 6. `05_observability_testing_performance` — streaming metrics, table history, query plans, data quality.
# MAGIC 7. `06_capstone_end_to_end` — full pipeline run + validation (what the scheduled Databricks Job runs).
# MAGIC 8. `07_file_rate_streaming_fallback` — no-MSK-required streaming source for demos/infra-not-ready.
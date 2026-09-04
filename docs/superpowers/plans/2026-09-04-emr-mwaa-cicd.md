# AWS-Only CI/CD (Plan 5 of 5) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Finish the Databricks-to-AWS migration by removing the now-superseded Databricks Asset Bundle deploy path and adding an AWS-native deploy workflow that publishes the wheel and syncs `emr_jobs/`/`airflow/dags/` to S3 — closing the gap Plan 2's review flagged (nothing was syncing `emr_jobs/*.py` to the S3 prefix its own DAG expects). Update the repo's top-level docs to describe both tracks (Databricks legacy, AWS-native current) instead of only the Databricks one.

**Architecture:** `databricks.yml`, `resources/jobs.yml`, and `.github/workflows/deploy_databricks.yml` are deleted outright — this repo no longer deploys anything to Databricks. `ci.yml` (lint/test/build) is widened to cover `emr_jobs/`/`airflow/` alongside `src`/`tests`, but is otherwise unchanged — the package under test doesn't change. A new `deploy_aws.yml` builds the wheel, uploads it to the fixed S3 artifact key Plan 1/2 already established, and syncs `emr_jobs/` and `airflow/dags/` to their expected S3 prefixes — mirroring `deploy_databricks.yml`'s trigger shape (push to `main` + manual `workflow_dispatch`) but for AWS credentials/targets instead of Databricks ones. `terraform apply` stays a manual step, matching this project's established convention throughout — `deploy_aws.yml` only builds/uploads artifacts.

**Tech Stack:** GitHub Actions, AWS CLI (`aws s3 cp`/`sync`), the existing Python packaging toolchain (unchanged).

## Global Constraints

- `class/`, `notebooks/` (Databricks tracks) stay completely untouched — they remain valid reference material, just no longer the actively-deployed path.
- `src/retail_lakehouse/`, `tests/` are NOT touched — the package and its tests don't change in this plan.
- `terraform apply` stays manual, never run by CI — this has been the rule since `infra/terraform/README.md`'s very first version and every plan since; `deploy_aws.yml` must not violate it.
- `RUNBOOK.md` documents the Databricks track's exact manual steps and stays valid for that track — do not delete it or rewrite its Databricks-specific content wholesale; add a clear pointer at the top instead, so a reader lands on the right track for their situation.
- The S3 key conventions already established by earlier plans must be reused exactly, not reinvented: wheel at `s3://<bucket>/artifacts/retail_lakehouse-latest.whl` (Plan 1's `emr_learning.tf` bootstrap action reads this), `emr_jobs/*.py` synced to `s3://<bucket>/emr_jobs/` (Plan 2's DAG's `SCRIPTS_S3_PREFIX` reads this), `airflow/dags/*.py` synced to `s3://<bucket>/airflow/dags/` (Plan 1's `mwaa.tf`'s `dag_s3_path` reads this).

---

### Task 1: Remove the Databricks Asset Bundle deploy path

**Files:**
- Delete: `databricks.yml`
- Delete: `resources/jobs.yml`
- Delete: `.github/workflows/deploy_databricks.yml`

**Interfaces:** None produced — this is pure removal. `resources/` will be empty after deleting `jobs.yml`; git doesn't track empty directories, so no further action is needed for that directory to "disappear."

- [ ] **Step 1: Delete the three files**

```bash
git rm databricks.yml resources/jobs.yml .github/workflows/deploy_databricks.yml
```

- [ ] **Step 2: Confirm nothing else references these files**

Run: `grep -rln "databricks.yml\|resources/jobs.yml\|deploy_databricks" --include="*.md" --include="*.yml" --include="*.py" . 2>/dev/null | grep -v "^\./docs/superpowers/"`

Expected: this will likely show hits in `RUNBOOK.md` and possibly `README.md` — that's expected and handled by Task 3, not this task. Just confirm there's nothing in `.github/workflows/ci.yml`, `pyproject.toml`, or any `src/`/`emr_jobs/`/`airflow/` file (there shouldn't be — none of those ever referenced the Databricks bundle).

- [ ] **Step 3: Commit**

```bash
git commit --no-gpg-sign -m "Remove Databricks Asset Bundle deploy path (databricks.yml, resources/jobs.yml, deploy_databricks.yml)

Production deploys now go through deploy_aws.yml (EMR + MWAA) instead.
The Databricks notebooks/ and class/ tracks are unaffected -- they don't
depend on the Asset Bundle to run interactively."
```

(Note the `--no-gpg-sign` flag — this repo has `commit.gpgsign=true` and the gpg-agent may hang indefinitely waiting for a passphrase prompt it can't receive in this environment; the user has explicitly approved skipping signing for every commit this session.)

---

### Task 2: Add `deploy_aws.yml`, widen `ci.yml`

**Files:**
- Create: `.github/workflows/deploy_aws.yml`
- Modify: `.github/workflows/ci.yml`

**Interfaces:**
- `deploy_aws.yml` consumes: a GitHub Environment named `aws` (mirroring the existing `environment: databricks` pattern the now-deleted `deploy_databricks.yml` used) with secrets `AWS_ACCESS_KEY_ID`/`AWS_SECRET_ACCESS_KEY`, and a repo/environment variable `vars.LAKEHOUSE_BUCKET` (the S3 bucket name — not secret, from `terraform output lakehouse_bucket_name`). It produces the exact S3 layout Plans 1/2 already assume (see Global Constraints).

- [ ] **Step 1: Create `.github/workflows/deploy_aws.yml`**

```yaml
name: Deploy AWS Artifacts

on:
  workflow_dispatch:
  push:
    branches: [ main ]

jobs:
  deploy:
    runs-on: ubuntu-latest
    environment: aws
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with:
          python-version: '3.10'
      - name: Build wheel
        run: |
          python -m pip install --upgrade pip
          pip install -e .[dev]
          python -m build
      - name: Configure AWS credentials
        uses: aws-actions/configure-aws-credentials@v4
        with:
          aws-access-key-id: ${{ secrets.AWS_ACCESS_KEY_ID }}
          aws-secret-access-key: ${{ secrets.AWS_SECRET_ACCESS_KEY }}
          aws-region: ${{ vars.AWS_REGION || 'us-east-1' }}
      - name: Upload wheel artifact
        run: aws s3 cp dist/retail_lakehouse-*.whl "s3://${{ vars.LAKEHOUSE_BUCKET }}/artifacts/retail_lakehouse-latest.whl"
      - name: Sync production Spark scripts
        run: aws s3 sync emr_jobs/ "s3://${{ vars.LAKEHOUSE_BUCKET }}/emr_jobs/" --delete
      - name: Sync MWAA DAGs
        run: aws s3 sync airflow/dags/ "s3://${{ vars.LAKEHOUSE_BUCKET }}/airflow/dags/" --delete
      # Deliberately does NOT run `terraform apply` -- infra changes stay a manual, reviewed step
      # (see infra/terraform/README.md). This workflow only publishes application artifacts.
```

- [ ] **Step 2: Widen `ci.yml`'s lint step and add a compile-check step**

Replace:
```yaml
      - name: Lint
        run: ruff check src tests
      - name: Unit tests
        run: pytest -q
      - name: Build wheel
        run: python -m build
```
with:
```yaml
      - name: Lint
        run: ruff check src tests emr_jobs airflow
      - name: Compile-check EMR scripts and DAG
        run: python -m compileall emr_jobs airflow/dags
      - name: Unit tests
        run: pytest -q
      - name: Build wheel
        run: python -m build
```
(`emr_jobs/`/`airflow/` have no dedicated test suite by design — Plans 2's review noted these are thin wrappers around already-tested `retail_lakehouse` functions — so lint + compile-check is the cheapest meaningful substitute for coverage, catching syntax errors and import mistakes on every PR instead of only when someone happens to run them.)

- [ ] **Step 3: Verify locally**

Run: `ruff check src tests emr_jobs airflow` (use `/tmp/rl_venv/bin/ruff` if a bare `ruff` isn't on PATH) — must pass clean (it already did as of Plan 4's merge; this step just confirms the widened scope doesn't surface anything new).
Run: `python3 -m compileall emr_jobs airflow/dags` — must report no syntax errors.

- [ ] **Step 4: Commit**

```bash
git add .github/workflows/deploy_aws.yml .github/workflows/ci.yml
git commit --no-gpg-sign -m "Add deploy_aws.yml (wheel + emr_jobs + MWAA DAG sync to S3), widen CI to cover emr_jobs/airflow"
```

---

### Task 3: Update `README.md` and add a track-pointer banner to `RUNBOOK.md`

**Files:**
- Modify: `README.md` (full rewrite)
- Modify: `RUNBOOK.md` (banner only — the rest of the file stays as-is, documenting the Databricks track)

**Interfaces:** None — documentation only.

- [ ] **Step 1: Replace the full contents of `README.md`**

```markdown
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
```

- [ ] **Step 2: Add a banner to the top of `RUNBOOK.md`**

Insert this immediately after `RUNBOOK.md`'s existing first line (its title heading) — do not modify anything else in the file:

```markdown

> **This runbook documents the Databricks track** (`class/`, `notebooks/`, Databricks Asset Bundle deploys).
> That track is no longer the actively-deployed path -- production now runs on AWS-native EMR + MWAA
> (`class-emr/`, `emr-notebooks/`, `emr_jobs/`, `airflow/dags/`, provisioned by `infra/terraform/`).
> This runbook remains accurate for the Databricks track specifically; see the top-level `README.md`
> for how the two tracks relate and where to start for the AWS-native one.
```

- [ ] **Step 3: Verify**

Run: `grep -c "AWS-native\|class-emr\|emr-notebooks" README.md` — should be several (confirms the new content landed). Run: `head -5 RUNBOOK.md` — should show the title line followed by the new banner blockquote.

- [ ] **Step 4: Commit**

```bash
git add README.md RUNBOOK.md
git commit --no-gpg-sign -m "Document the AWS-native track in README.md; point RUNBOOK.md at it"
```

---

## Verifying the whole plan

1. `ls databricks.yml resources/jobs.yml .github/workflows/deploy_databricks.yml 2>&1` — all three report "No such file or directory".
2. `ruff check src tests emr_jobs airflow` — clean.
3. `python3 -m compileall emr_jobs airflow/dags` — no errors.
4. `pytest -q` — unaffected by this plan (no `src`/`tests` changes); same pre-existing PySpark/JVM-gateway sandbox limitation as every prior plan, not a regression.
5. `python3 -c "import yaml; yaml.safe_load(open('.github/workflows/deploy_aws.yml')); yaml.safe_load(open('.github/workflows/ci.yml'))"` — both parse as valid YAML.
6. `git diff --stat main` — shows the three deletions plus `deploy_aws.yml` (new), `ci.yml`/`README.md`/`RUNBOOK.md` (modified). Nothing under `src/`, `tests/`, `class/`, `notebooks/`, `class-emr/`, `emr-notebooks/`, `emr_jobs/`, `airflow/`, `infra/` touched.

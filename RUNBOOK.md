# Runbook: Retail Lakehouse on AWS Databricks, MSK, S3, and Delta Lake

This is the step-by-step to go from zero to a running end-to-end pipeline. Read `README.md` first for the
overall structure.

## 0. If you just want to learn the concepts (no AWS needed)

Import `class/` into a Databricks workspace (any workspace with Unity Catalog and Runtime 15.4 LTS+ works —
no MSK, no Terraform, no S3 setup) and run `01` through `04` in order. Each is self-contained and generates
its own small dataset in-notebook.

## 1. Prerequisites for the production pipeline

- An AWS account with permission to create S3 buckets, an MSK Serverless cluster, and IAM policies.
- An AWS Databricks workspace with Unity Catalog enabled, Runtime 15.4 LTS or newer.
- Network connectivity between the Databricks workspace's compute plane VPC and the VPC you'll deploy MSK
  into (same VPC, peering, or Transit Gateway). This is an organizational/networking decision — Terraform
  cannot set this up for you without knowing your Databricks deployment's VPC layout.
- Python 3.10+ and the Databricks CLI configured locally (`databricks configure --host ...`) for packaging
  and bundle deploys.

## 2. Provision AWS infrastructure (S3 + MSK)

```bash
cd infra/terraform
cp terraform.tfvars.example terraform.tfvars
# edit terraform.tfvars with your vpc_id, subnet_ids, databricks_security_group_id
terraform init
terraform apply
terraform output next_steps
```

Follow the printed steps to:
1. Fetch the MSK bootstrap broker string.
2. Create the `retail-clickstream` topic (`scripts/create_msk_topics.sh <bootstrap-brokers>`).
3. Attach the generated IAM policy (`databricks_msk_access_policy_arn` output) to your Databricks cluster's
   instance profile role, and set that instance profile on the cluster you'll run notebooks on.

**MSK Serverless bills continuously. Run `terraform destroy` (see `infra/terraform/README.md`) when you're
done for the day.**

## 3. Install the Kafka connector library

Databricks Runtime includes the core Spark-Kafka connector, but MSK IAM auth needs the
`aws-msk-iam-auth` Maven library available too: `software.amazon.msk:aws-msk-iam-auth:2.2.0`.

- **Classic cluster:** attach via cluster UI -> Libraries -> Install New -> Maven -> paste the coordinates above.
- **Serverless compute** (this repo's `resources/jobs.yml` runs on serverless — workspaces that only
  allow serverless reject classic `new_cluster` job definitions outright): the wheel is installed
  automatically via the job's `environments.pipeline_env.spec.dependencies` block, but Maven/JVM library
  support on serverless environments is still evolving — check your workspace's current Databricks docs
  for whether it can be added there. If not, notebook `02`'s MSK ingest task may need to run on a classic
  job cluster (a mixed compute job, with only that one task pinned to `new_cluster`) while the rest of the
  pipeline stays serverless.

## 4. Package and run tests locally

```bash
python -m venv .venv
source .venv/bin/activate
pip install -e .[dev]
pytest -q
python -m build
```

All transformation, streaming-aggregation, and Kafka-payload-parsing logic in `src/retail_lakehouse/` is
unit tested against a local PySpark session — no live MSK/S3/Databricks needed to validate the code itself.

## 5. Run the notebooks in order

Import `notebooks/` into your Databricks workspace (or work against a Git-backed Repo). Set widgets:

```text
catalog = main
schema = retail_lakehouse
base_path = s3://<your-lakehouse-bucket>/data   (from `terraform output lakehouse_bucket_name`)
kafka_bootstrap_servers = <from step 2>          (only needed for notebook 02)
kafka_topic = retail-clickstream
```

Run in order: `00` -> `01` -> `02` -> `03` -> `04` -> `05` -> `06`. If MSK isn't provisioned yet, run `07`
instead of `02`, and point notebook `03`'s `bronze_table` widget at `bronze_clickstream_rate`.

Seed live traffic into the MSK topic before running notebook `02`:

```bash
pip install -e .[dev,kafka]
python scripts/seed_kafka_topic.py --bootstrap-servers <brokers> --auth iam --count 5000
```

## 6. Deploy as a scheduled Databricks Job (Asset Bundle)

```bash
databricks bundle validate -t dev
databricks bundle deploy -t dev
databricks bundle run retail_lakehouse_pipeline -t dev
```

`resources/jobs.yml` chains `00` -> `01` -> `streaming_ingest_fallback` -> `03` -> `06` as a single job on
serverless compute, using the built wheel (attached via the job's `environments` block, not a cluster
library). By default the streaming-ingest task runs notebook `07`'s file/rate fallback (no MSK required) —
once `infra/terraform` is applied and you have a real bootstrap broker string, edit that task in
`resources/jobs.yml` to point at `notebooks/02_kafka_msk_streaming_ingest.py` with
`kafka_bootstrap_servers`/`kafka_topic` base_parameters instead, and update the downstream `bronze_table`
base_parameter on the `streaming_silver_gold` task back to `bronze_clickstream_kafka`.

Add a schedule block to the job resource, or trigger it from your orchestrator of choice, for a recurring run.

## 7. CI/CD

- `.github/workflows/ci.yml` — lints, runs `pytest`, and builds the wheel on every PR/push to `main`.
- `.github/workflows/deploy_databricks.yml` — validates and deploys the bundle to the `dev` target (or a
  chosen target via `workflow_dispatch`) using `DATABRICKS_HOST`/`DATABRICKS_TOKEN` repo secrets.

## 8. Cleanup

```bash
# Drop pipeline tables
databricks bundle run ... # or run scripts/delete_pipeline_tables.sql via a SQL editor/notebook

# Tear down AWS infra
cd infra/terraform
aws s3 rm s3://$(terraform output -raw lakehouse_bucket_name) --recursive
terraform destroy
```

## Troubleshooting

**`Cannot import retail_lakehouse`** — In an interactive Repos-backed notebook without the wheel installed,
run `%pip install -e .` from the repo root in a notebook cell, or attach the built wheel
(`dist/retail_lakehouse-*.whl`) to the cluster as a library. Bundle-deployed jobs install the wheel
automatically per `resources/jobs.yml`.

**`Kafka source not found` / class not found errors** — Ensure the `aws-msk-iam-auth` Maven library (step 3)
is attached to the cluster, not just present in your local pip environment — it's a JVM-side dependency.

**`kafka_bootstrap_servers` widget raises immediately** — expected: notebook 02 fails fast rather than
silently no-op'ing when MSK isn't configured. Use notebook 07's fallback, or complete step 2.

**Streaming query seems to run forever in `processingTime` mode** — expected for a true always-on stream; on serverless use AvailableNow instead.
All production notebooks in this project use `trigger(availableNow=True)` specifically so they terminate and
are safe to run as scheduled Databricks Job tasks; see `class/02_spark_structured_streaming.py` section 3
for the tradeoff.

**`OPTIMIZE`/managed Iceberg table creation fails** — requires a genuine Databricks Runtime cluster (not
local PySpark) and, for Iceberg, Unity Catalog with the feature enabled in your workspace/region. Notebook
`04_delta_production_patterns_iceberg.py` catches this and falls back to a Delta comparison table so the
rest of the notebook still runs.

**Terraform `iam.tf` ARNs look wrong** — see the note in `infra/terraform/iam.tf`; MSK's IAM ARN format for
topics/consumer groups isn't reliably derivable by string substitution from the cluster ARN across all AWS
partitions. Verify in the AWS console after `apply` and hand-correct if needed.

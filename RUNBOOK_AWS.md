# Runbook: Retail Lakehouse on AWS-Native EMR + MWAA, MSK, S3, and Delta Lake

This is the step-by-step to go from zero to a running end-to-end pipeline on the AWS-native track (EMR +
MWAA, no Databricks). Read `README.md` first for how this track relates to the Databricks one
(`RUNBOOK.md`).

**Claude Code did not run any of the steps below against real AWS** — every file in this repo (Terraform,
the DAG, `deploy_aws.yml`, the notebooks) was authored locally without AWS credentials. Everything from
`terraform apply` onward requires *you* to have AWS credentials configured in your own shell/CI.

## 0. If you just want to learn the concepts (minimal infra)

`class-emr/01_spark_batch_processing.ipynb` through `04_data_lakehouse_delta_s3.ipynb` are self-contained
(they generate their own data in-notebook, no MSK/production tables needed) — but unlike the Databricks
`class/` track, they still need a real Spark session, so you need at least step 2 below applied (S3 +
the persistent EMR learning cluster) before opening them. No MWAA, no production pipeline required for
this path.

## 1. Prerequisites

- An AWS account with permission to create S3 buckets, an MSK Serverless cluster, EMR clusters, an MWAA
  environment, and IAM roles.
- AWS credentials configured locally (`aws configure` or equivalent env vars) and Terraform installed.
- An existing VPC with at least 2 private subnets in different AZs, each with a route to a NAT gateway
  (or S3 gateway endpoint) for outbound internet/S3 access — required by MWAA and by the EMR bootstrap
  action. This is the one step Terraform can't automate for you; see `infra/terraform/README.md`.
- Python 3.10+ locally for packaging/tests.

## 2. Provision AWS infrastructure

```bash
cd infra/terraform
cp terraform.tfvars.example terraform.tfvars
# edit terraform.tfvars: vpc_id, subnet_ids

terraform init
terraform plan
terraform apply

terraform output next_steps
```

**This costs real money continuously** — MSK Serverless, the EMR learning cluster, and MWAA all bill
while they exist, not just while in use. See "This costs real money" in `infra/terraform/README.md`, and
step 9 below for teardown.

## 3. Create the Kafka topic

```bash
terraform output -raw msk_bootstrap_brokers_command | bash   # prints the bootstrap broker string
../../scripts/create_msk_topics.sh <bootstrap-brokers>        # creates retail-clickstream, 6 partitions, RF 3
```

No credential registration step is needed — EMR clusters built from this Terraform (the persistent
learning cluster and the ephemeral production clusters MWAA creates) already have MSK access via the EC2
instance profile in `iam.tf`.

## 4. Package and run tests locally (optional, no AWS needed)

```bash
python -m venv .venv
source .venv/bin/activate
pip install -e .[dev]
pytest -q
python -m build
```

All transformation/streaming/quality logic in `src/retail_lakehouse/` is unit tested against a local
PySpark session — nothing here touches AWS.

## 5. Access EMR JupyterHub (the persistent learning cluster)

JupyterHub has no inbound security group rule opening it to the internet — reach it via SSM port
forwarding instead (no bastion, key pair, or open ports needed):

```bash
# Find the master instance ID:
aws emr list-instances --cluster-id <terraform output emr_learning_cluster_id> \
  --instance-group-types MASTER --query 'Instances[0].Ec2InstanceId' --output text

# Start the port-forward session:
aws ssm start-session --target <master-instance-id> \
  --document-name AWS-StartPortForwardingSession \
  --parameters '{"portNumber":["9443"],"localPortNumber":["9443"]}'
```

Then browse to `https://localhost:9443/` (self-signed cert — your browser will warn, that's expected).
Use the `PySpark` kernel for every notebook below.

## 6. Run the notebooks

In each notebook's first real code cell, set `base_path` to `s3://<bucket>/data` (from
`terraform output lakehouse_bucket_name`) — every notebook has a `s3://<your-lakehouse-bucket>/...`
placeholder that needs replacing.

- **Concepts**: `class-emr/01` → `02` → `03` → `04`, in order.
- **Production walkthrough**: `emr-notebooks/00` → `01` → `02` → `03` → `04` → `05` → `06`, in order. If
  MSK isn't provisioned yet (or you skipped straight here from step 0), run `07` instead of `02`, and set
  `03`'s `bronze_table` variable to `bronze_clickstream_rate`.

## 7. Deploy the production pipeline's artifacts

Either push to `main` (if `.github/workflows/deploy_aws.yml`'s GitHub Environment is configured — see
`README.md`'s "CI/CD" section for the exact secrets/variables it needs), or run it locally:

```bash
./scripts/deploy_aws.sh <lakehouse-bucket-name>
```

Either path builds the wheel and syncs `emr_jobs/*.py` + `airflow/dags/retail_lakehouse_pipeline.py` to
S3 — the exact files/locations the MWAA environment and its DAG expect.

## 8. Configure and trigger the MWAA DAG

Set these Airflow Variables once per environment (MWAA UI → Admin → Variables, or
`aws mwaa create-cli-token` + the Airflow CLI):

```text
retail_lakehouse_bucket            (from `terraform output lakehouse_bucket_name`)
retail_lakehouse_subnet_id         (one of the subnet_ids passed to Terraform)
retail_lakehouse_base_path         (e.g. s3://<bucket>/data)
retail_lakehouse_emr_msk_client_sg (from `terraform output emr_msk_client_security_group_id`)
```

Optional (defaults match `infra/terraform/variables.tf`): `retail_lakehouse_emr_instance_profile`,
`retail_lakehouse_emr_service_role`, `retail_lakehouse_emr_release_label`,
`retail_lakehouse_emr_instance_type`, `retail_lakehouse_schema`.

Then open the Airflow UI (`terraform output mwaa_webserver_url`), find the `retail_lakehouse_pipeline`
DAG, un-pause it, and trigger a run. It creates an ephemeral EMR cluster, runs `emr_jobs/00` → `01` →
`07` (fallback, until you provision real MSK traffic and swap this task back to `02`) → `03` → `06` as
EMR Steps, then always terminates the cluster.

## 9. Cleanup

```bash
# Drop pipeline tables -- run scripts/delete_pipeline_tables.sql via a SQL editor, or from a notebook

# Tear down AWS infra
cd infra/terraform
aws s3 rm s3://$(terraform output -raw lakehouse_bucket_name) --recursive
terraform destroy
```

## Troubleshooting

**Notebook cell fails with `Failed to find data source: delta`** — you skipped the `%%configure -f` cell
at the top of the notebook, or ran cells out of order. It must run before any other Spark code in that
session.

**`Cannot import retail_lakehouse`** — the EMR cluster's bootstrap action installs it automatically from
`s3://<bucket>/artifacts/retail_lakehouse-latest.whl`; if that key doesn't exist yet, run step 7 first,
then recreate the cluster (the bootstrap action only runs at cluster creation).

**MWAA DAG import error / DAG not showing up** — confirm `airflow/dags/retail_lakehouse_pipeline.py`
actually landed in `s3://<bucket>/airflow/dags/` (step 7), and that all four required Airflow Variables
from step 8 are set — the DAG raises at parse time if any are missing.

**EMR step fails at submit time with a Maven/Ivy resolution error** — the EMR subnet has no outbound
internet access (no NAT gateway/S3 endpoint); `--packages io.delta:...` needs to reach Maven Central.

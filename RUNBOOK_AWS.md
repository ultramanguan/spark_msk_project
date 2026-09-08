# Runbook: Retail Lakehouse on AWS-Native EMR + MSK + S3 + Delta Lake (MWAA optional)

This is the step-by-step to go from zero to a running interactive lakehouse pipeline on the AWS-native
track (EMR, no Databricks). Read `README.md` first for how this track relates to the Databricks one
(`RUNBOOK.md`).

**Claude Code did not run any of the steps below against real AWS** — every file in this repo (Terraform,
the DAG, the notebooks) was authored locally without AWS credentials. Everything from `terraform apply`
onward requires *you* to have AWS credentials configured in your own shell.

**MWAA is off by default** (`var.enable_mwaa = false`). The default path in this runbook — S3 + MSK +
the persistent EMR learning cluster + JupyterHub — doesn't need it at all. MWAA only matters if you want
the pipeline running as a *scheduled production job* rather than something you run interactively; see
"Optional: the scheduled production pipeline (MWAA)" near the end if that's what you want.

## 0. If you just want to learn the concepts (minimal infra)

`class-emr/01_spark_batch_processing.ipynb` through `04_data_lakehouse_delta_s3.ipynb` are self-contained
(they generate their own data in-notebook, no MSK/production tables needed) — but unlike the Databricks
`class/` track, they still need a real Spark session, so you need at least step 2 below applied before
opening them.

## 1. Prerequisites

- An AWS account with permission to create S3 buckets, an MSK Serverless cluster, EMR clusters, and IAM
  roles.
- AWS CLI installed and credentials configured locally:
  ```bash
  brew install awscli
  aws configure   # or `aws sso login` / AWS_* env vars
  ```
- Session Manager plugin, needed for `aws ssm start-session` to reach EMR's JupyterHub (step 5) — the
  base AWS CLI can't open an interactive session on its own:
  ```bash
  brew install --cask session-manager-plugin
  ```
- Terraform CLI installed. It's no longer in homebrew-core (HashiCorp pulled it after their license
  change), so install it from HashiCorp's own tap:
  ```bash
  brew tap hashicorp/tap
  brew install hashicorp/tap/terraform
  ```
- An existing VPC with **2 subnets** in different AZs (that's all MSK Serverless and EMR need — no NAT
  Gateway required for this default path; that's a separate, MWAA-only requirement covered in the
  optional MWAA section below).
- Python 3.10+ locally for packaging/tests.

## 2. Provision AWS infrastructure

The EMR learning cluster's bootstrap action installs the `retail_lakehouse` wheel automatically at
cluster creation — which means the wheel needs to already be in S3 *before* the cluster is created, and
the S3 bucket needs to exist before you can upload anything to it. Three small steps, in order:

```bash
cd infra/terraform
cp terraform.tfvars.example terraform.tfvars
# edit terraform.tfvars: vpc_id, subnet_ids (2 subnets, any AZ pairing)

terraform init
terraform plan
terraform apply -exclude=aws_emr_cluster.learning   # everything except the EMR cluster: S3, IAM, MSK, networking
```

```bash
cd ..
BUCKET=$(terraform -chdir=infra/terraform output -raw lakehouse_bucket_name)
scripts/deploy_aws.sh "$BUCKET"                     # builds the wheel, uploads it to the now-existing bucket
```

```bash
cd infra/terraform
terraform apply                                     # now creates the EMR cluster; its bootstrap succeeds
terraform output next_steps
```

**This costs real money continuously** — MSK Serverless and the EMR learning cluster both bill while they
exist, not just while in use. See "This costs real money" in `infra/terraform/README.md`, and step 8
below for teardown.

## 3. Create the Kafka topic

Nothing to do here manually — `emr-notebooks/02_kafka_msk_streaming_ingest.ipynb` (step 6 below) creates
the `retail-clickstream` topic itself, the first time you run it, via
`retail_lakehouse.kafka_admin.create_topics`. It's idempotent, so re-running the notebook later is safe
and won't error on an existing topic.

You do need the bootstrap brokers string first, to paste into that notebook's `kafka_bootstrap_servers`
variable:

```bash
terraform output -raw msk_bootstrap_brokers_command | bash
```

No credential registration step is needed — the EMR cluster already has MSK access via the EC2 instance
profile in `iam.tf`.

## 4. Package and run tests locally (optional, no AWS needed)

```bash
python -m venv .venv
source .venv/bin/activate
pip install -e ".[dev,kafka]"
pytest -q
python -m build
```

All transformation/streaming/quality logic in `src/retail_lakehouse/` is unit tested against a local
PySpark session — nothing here touches AWS.

## 5. Access EMR JupyterHub

This is the main way you'll interact with everything from here on. JupyterHub has no inbound security
group rule opening it to the internet — reach it via SSM port forwarding instead (no bastion, key pair,
or open ports needed):

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
Default login is `jovyan` / `jupyter`. Use the `PySpark` kernel for every notebook below.

## 6. Run the notebooks

`emr-notebooks/`/`class-emr/` land in JupyterHub's file browser automatically — `emr_learning.tf` uploads
every `*.ipynb` to S3 via Terraform and an EMR step (`sync_notebooks`, runs once the cluster and
JupyterHub are fully up) pulls them into `jovyan`'s home directory at cluster creation. Nothing to do here
on a fresh cluster — just open JupyterHub and they're there.

If you edit a notebook locally afterward and want that change reflected on an already-running cluster
(the step only runs once, at creation, so it won't pick up later edits on its own), re-sync manually: push
from your laptop with `aws s3 sync emr-notebooks/ "s3://$BUCKET/notebooks/emr-notebooks/"` (same for
`class-emr/`), then pull from a **terminal inside JupyterHub** (File → New → Terminal) with
`aws s3 sync "s3://$BUCKET/notebooks/emr-notebooks/" ~/emr-notebooks/`.

In each notebook's first real code cell, set `base_path` to `s3://<bucket>/data` (from
`terraform output lakehouse_bucket_name`) — every notebook has a `s3://<your-lakehouse-bucket>/...`
placeholder that needs replacing.

- **Concepts**: `class-emr/01` → `02` → `03` → `04`, in order.
- **Production walkthrough**: `emr-notebooks/00` → `01` → `02` → `03` → `04` → `05` → `06`, in order. If
  MSK isn't provisioned yet (or you skipped straight here from step 0), run `07` instead of `02`, and set
  `03`'s `bronze_table` variable to `bronze_clickstream_rate`.

## 7. Cleanup

```bash
# Drop pipeline tables -- run scripts/delete_pipeline_tables.sql via a SQL editor, or from a notebook

# Tear down AWS infra
cd infra/terraform
aws s3 rm s3://$(terraform output -raw lakehouse_bucket_name) --recursive
terraform destroy
```

Verify nothing's left billing after:

```bash
aws emr list-clusters --active
aws kafka list-clusters
aws mwaa list-environments
```

## Optional: the scheduled production pipeline (MWAA)

Only do this if you actually want the pipeline running as a scheduled Airflow DAG on ephemeral EMR
clusters, rather than something you run interactively from notebooks — it's meaningfully more cost and
setup than the default path above.

1. **Additional prerequisite**: one more existing **public** subnet, distinct from your 2 `subnet_ids`,
   to host a NAT Gateway. MWAA rejects subnets that route directly to an Internet Gateway; MSK/EMR don't
   care either way, which is why this isn't needed for the default path. If your 2 `subnet_ids` started
   life as public, also run
   `aws ec2 modify-subnet-attribute --subnet-id <id> --no-map-public-ip-on-launch` on both — MWAA checks
   this attribute independently of the route table, and Terraform doesn't manage it.
2. In `terraform.tfvars`, set:
   ```hcl
   enable_mwaa           = true
   nat_gateway_subnet_id = "subnet-xxxxx"   # the extra public subnet from step 1
   ```
3. `terraform apply` again — this creates the NAT Gateway, makes `subnet_ids` private, and provisions
   MWAA. Expect this to take 20-40 minutes; MWAA environment creation is just slow. Check real status in
   a second terminal instead of waiting on a blocked one:
   ```bash
   aws mwaa get-environment --name <project_name>-<environment> \
     --query 'Environment.{Status:Status,LastUpdate:LastUpdate}' --output json
   ```
4. Set these Airflow Variables once (MWAA UI → Admin → Variables, or `aws mwaa create-cli-token` + the
   Airflow CLI):
   ```text
   retail_lakehouse_bucket            (from `terraform output lakehouse_bucket_name`)
   retail_lakehouse_subnet_id         (one of the subnet_ids passed to Terraform)
   retail_lakehouse_base_path         (e.g. s3://<bucket>/data)
   retail_lakehouse_emr_msk_client_sg (from `terraform output emr_msk_client_security_group_id`)
   ```
   Optional (defaults match `infra/terraform/variables.tf`): `retail_lakehouse_emr_instance_profile`,
   `retail_lakehouse_emr_service_role`, `retail_lakehouse_emr_release_label`,
   `retail_lakehouse_emr_instance_type`, `retail_lakehouse_schema`.
5. Open the Airflow UI (`terraform output mwaa_webserver_url`), find the `retail_lakehouse_pipeline` DAG,
   un-pause it, and trigger a run. It creates an ephemeral EMR cluster, runs `emr_jobs/00` → `01` → `07`
   (fallback, until you provision real MSK traffic and swap this task back to `02`) → `03` → `06` as EMR
   Steps, then always terminates the cluster.

To turn MWAA back off later, set `enable_mwaa = false` and `terraform apply` — this destroys the MWAA
environment, its IAM role/security group, the NAT Gateway, and the private route table, leaving
`subnet_ids` however they were before.

## Troubleshooting

**Notebook cell fails with `Failed to find data source: delta`** — you skipped the `%%configure -f` cell
at the top of the notebook, or ran cells out of order. It must run before any other Spark code in that
session.

**`Cannot import retail_lakehouse`** — the EMR cluster's bootstrap action installs it automatically from
`s3://<bucket>/artifacts/retail_lakehouse-latest.whl`. If you applied `terraform apply` (creating the EMR
cluster) before running `scripts/deploy_aws.sh`, that key won't exist yet — run the deploy script, then
recreate the cluster (`terraform apply -replace=aws_emr_cluster.learning`) so its bootstrap action can
pick up the wheel.

**Step 3's bootstrap-brokers command prints `None` instead of a broker string** — the MSK Serverless
cluster isn't `ACTIVE` yet. Check with `aws kafka list-clusters-v2 --query 'ClusterInfoList[].State'` and
try again once it says `ACTIVE` — this usually takes a few minutes after `terraform apply` finishes.

**EMR step fails at submit time with a Maven/Ivy resolution error** — the EMR subnet has no outbound
internet access. Confirm the subnet has a route to an Internet Gateway (default path) or NAT Gateway
(MWAA path) — `--packages io.delta:...` needs to reach Maven Central either way.

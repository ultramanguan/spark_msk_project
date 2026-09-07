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
- AWS CLI installed and credentials configured locally:
  ```bash
  brew install awscli
  aws configure   # or `aws sso login` / AWS_* env vars
  ```
- Session Manager plugin, needed for `aws ssm start-session` to reach EMR's JupyterHub (step 5/6) — the
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
- An existing VPC with 3 subnets: 2 that Terraform will make private (via a NAT Gateway + dedicated route
  table in `infra/terraform/networking.tf`), plus 1 separate existing **public** subnet to host that NAT
  Gateway — a NAT Gateway can't live inside the private subnets it serves. MWAA rejects subnets that
  route directly to an Internet Gateway, so this is required; see `infra/terraform/README.md`. If the 2
  subnets were originally public, also run
  `aws ec2 modify-subnet-attribute --subnet-id <id> --no-map-public-ip-on-launch` on both — MWAA checks
  this attribute independently of the route table, and Terraform doesn't manage it.
- Python 3.10+ locally for packaging/tests.

## 2. Provision AWS infrastructure

```bash
cd infra/terraform
cp terraform.tfvars.example terraform.tfvars
# edit terraform.tfvars: vpc_id, subnet_ids (2 subnets to make private), nat_gateway_subnet_id (1 existing
# public subnet, distinct from subnet_ids, to host the NAT Gateway)

terraform init
terraform plan
terraform apply

terraform output next_steps
```

**This costs real money continuously** — MSK Serverless, the EMR learning cluster, and MWAA all bill
while they exist, not just while in use. See "This costs real money" in `infra/terraform/README.md`, and
step 9 below for teardown.

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

`terraform destroy` also removes the NAT Gateway, its Elastic IP, and the private route table
`infra/terraform/networking.tf` created -- nothing NAT-related is left behind to bill after this.

Verify nothing's left billing after:

```bash
aws emr list-clusters --active
aws kafka list-clusters
aws mwaa list-environments
```

## Troubleshooting

**Notebook cell fails with `Failed to find data source: delta`** — you skipped the `%%configure -f` cell
at the top of the notebook, or ran cells out of order. It must run before any other Spark code in that
session.

**`Cannot import retail_lakehouse`** — the EMR cluster's bootstrap action installs it automatically from
`s3://<bucket>/artifacts/retail_lakehouse-latest.whl`; if that key doesn't exist yet, run step 7 first,
then recreate the cluster (the bootstrap action only runs at cluster creation).

**EMR `BOOTSTRAP_FAILURE` with `ERROR: retail_lakehouse-latest.whl is not a valid wheel filename` in the
bootstrap action's `stderr.gz`** (find it at
`s3://<bucket>/emr-logs/learning/<cluster-id>/node/<instance-id>/bootstrap-actions/1/stderr.gz`) — this is
a real bug, not an environment issue: `pip install` requires a wheel filename shaped like
`name-version-pythontag-abitag-platformtag.whl` (at least 4 hyphen-separated segments) before it will even
open the file, and the fixed `-latest` alias name used for the S3 object (`retail_lakehouse-latest.whl`,
only 2 segments) fails that check unconditionally — it was never going to work, regardless of the wheel's
actual contents. `infra/terraform/bootstrap/install_retail_lakehouse.sh` fixes this by giving the
downloaded copy a PEP 427-compliant local filename (`/tmp/retail_lakehouse-0.0.0-py3-none-any.whl`) before
installing it; the placeholder version number doesn't matter since pip reads the real name/version from
the wheel's actual metadata, not the filename, once the filename parses.

**EMR `BOOTSTRAP_FAILURE` with `ERROR: Package 'retail-lakehouse' requires a different Python: 3.9.25 not
in '>=3.10'`** — EMR release `emr-7.5.0`'s system `python3` is 3.9.25, but `pyproject.toml` declared
`requires-python = ">=3.10"`. pip enforces that constraint against the *installing* interpreter, not
whatever Python built the wheel — building/testing locally or in CI on 3.10+ (see `.github/workflows/*`)
is unaffected, since the wheel itself is a pure-Python `py3-none-any` build with no version-specific
bytecode. The fix was lowering `requires-python` to `>=3.9` in `pyproject.toml` to match what EMR actually
ships, after confirming nothing in `src/` uses 3.10-only syntax (`match` statements, etc.). If a future
change genuinely needs 3.10+, the EMR release label would need to change too, not just the constraint.

**Step 3's bootstrap-brokers command prints `None` instead of a broker string** — two different possible
causes, check in this order: (1) the MSK Serverless cluster may not be `ACTIVE` yet
(`aws kafka list-clusters-v2 --query 'ClusterInfoList[].State'`) — bootstrap brokers aren't populated
before that. (2) Even once active, `--query bootstrapBrokerStringSaslIam` (lowercase `b`) silently returns
null forever, because JMESPath queries are case-sensitive and the real API field is
`BootstrapBrokerStringSaslIam` (capital `B`) — confirm the exact casing by running
`aws kafka get-bootstrap-brokers --cluster-arn <arn>` with no `--query` at all and reading the raw JSON.
This lowercase/uppercase mismatch was an actual bug in `msk_bootstrap_brokers_command`'s output definition
in `infra/terraform/msk.tf`, not a transient cluster-readiness issue — it would have returned `None`
forever, even once the cluster was active, until the casing was fixed.

**MWAA DAG import error / DAG not showing up** — confirm `airflow/dags/retail_lakehouse_pipeline.py`
actually landed in `s3://<bucket>/airflow/dags/` (step 7), and that all four required Airflow Variables
from step 8 are set — the DAG raises at parse time if any are missing.

**EMR step fails at submit time with a Maven/Ivy resolution error** — the EMR subnet has no outbound
internet access (no NAT gateway/S3 endpoint); `--packages io.delta:...` needs to reach Maven Central.

**MWAA `CreateEnvironment` fails with `ValidationException: The subnets must be private`, even though the
subnets' route table correctly points `0.0.0.0/0` at a NAT Gateway** — MWAA also checks the EC2 subnet
attribute `MapPublicIpOnLaunch`, independent of the route table. Subnets that started life as public
(most default-VPC subnets do) keep this set to `true` even after you repoint their routing — Terraform's
`aws_route_table`/`aws_route_table_association` never touches this attribute, so it has to be turned off
separately:
```bash
aws ec2 modify-subnet-attribute --subnet-id <subnet-id> --no-map-public-ip-on-launch
```
Do this for both subnets in `var.subnet_ids`, then retry. Routing alone will look correct in
`aws ec2 describe-route-tables` while this is still the actual blocker — don't stop checking at the route
table.

**A NAT Gateway needs its own subnet, separate from the private subnets it serves** — a NAT Gateway must
sit in a subnet with its own direct route to an Internet Gateway, so it can't live inside the private
subnets it's making private for MWAA (that would be circular). If you're converting existing public
subnets into `var.subnet_ids`, you need one *additional* existing public subnet on hand purely to host the
NAT Gateway (`var.nat_gateway_subnet_id`) — budget for 3 subnets total, not 2.

**EMR `RunJobFlow` fails with `Instance type 'X' is not supported`** — not every size in an instance
family is valid for EMR, and it depends on the release label + application set. `m5.large` was rejected
outright for release `emr-7.5.0` running Spark+JupyterHub+Livy+Hadoop together; `m5.xlarge` was the
smallest size that worked. Don't assume the smallest instance in a family will be accepted — if cost is
the goal, drop `emr_instance_count` instead of downsizing below whatever instance type is proven to work.

**MWAA `CreateEnvironment` fails with `... is not authorized to perform: s3:GetAccountPublicAccessBlock`
or `s3:GetBucketPublicAccessBlock`** — these are pre-flight checks MWAA's own validation runs against your
execution role before it will create the environment; they aren't part of a typical hand-written S3 access
policy. Add both to the execution role's policy: the account-level action needs `Resource = "*"` (no
bucket ARN applies to an account-level check), the bucket-level one needs the bucket's ARN. See
`aws_iam_role_policy.mwaa_execution_policy` in `infra/terraform/mwaa.tf` for the exact statements.

**Terraform creates/updates resources in the "wrong" order even though the config looks fine** — Terraform
only infers ordering from resource-*attribute* references (`aws_x.y.arn`, `.id`, etc). If two resources
are tied together only by both reading the same input *variable* (e.g. `aws_mwaa_environment.this` and
`aws_route_table_association.private` both use `var.subnet_ids`, but neither references the other's
attributes), Terraform sees no dependency between them and may create them in parallel — which is exactly
how MWAA validation raced ahead of a NAT Gateway/route table that hadn't finished being created yet in an
earlier version of this config. The fix is an explicit `depends_on` (see `mwaa.tf` and `emr_learning.tf`)
wherever a resource's *real* correctness depends on another resource that config structure alone doesn't
expose.

**`terraform apply -target=X` doesn't pick up a change you just made** — `-target` only pulls in resources
`X` depends on *by attribute reference*. `aws_mwaa_environment.this` references
`aws_iam_role.mwaa_execution.arn` (the role), not `aws_iam_role_policy.mwaa_execution_policy` (the inline
policy attached to that role) — so `-target=aws_mwaa_environment.this` alone will retry environment
creation using whatever policy is *already live in AWS*, silently ignoring a policy edit still sitting
only in your local config. Target both explicitly
(`-target=aws_iam_role_policy.mwaa_execution_policy -target=aws_mwaa_environment.this`), or just run a
plain `terraform apply` once you're done narrowing down a specific failure — `-target` is for isolating
one error at a time, not a substitute for a normal apply.

**`terraform apply` seems stuck on the MWAA environment for 30+ minutes** — this isn't necessarily stuck;
AWS documents MWAA environment creation as commonly taking 20-40 minutes. Don't just wait on a blocked
terminal — open a second one and check real status directly:
```bash
aws mwaa get-environment --name <project_name>-<environment> \
  --query 'Environment.{Status:Status,LastUpdate:LastUpdate}' --output json
```
`CREATING` means it's genuinely still working. `CREATE_FAILED` shows the real error immediately in
`LastUpdate.Error.ErrorMessage` — you can `Ctrl+C` the stuck `terraform apply` and act on it right away
instead of waiting for Terraform's own polling to notice and time out.


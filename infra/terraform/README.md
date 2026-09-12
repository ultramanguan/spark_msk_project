# Terraform: S3 + MSK Serverless + EMR (+ optional MWAA) for the Retail Lakehouse project

This provisions the AWS infrastructure the lakehouse pipeline needs, entirely within AWS (no Databricks
dependency for this track):

- An **S3 bucket** for Delta table data, streaming checkpoints, source files, and (if MWAA is enabled)
  its DAGs/requirements.
- An **MSK Serverless cluster** (IAM-authenticated) for the clickstream Kafka topic.
- A **persistent EMR cluster** with JupyterHub, for interactive/teaching notebooks.
- Optionally, an **MWAA (managed Airflow) environment** to orchestrate the pipeline as a scheduled job on
  ephemeral EMR clusters — off by default (`var.enable_mwaa = false`). The interactive path above doesn't
  need it at all.

## This costs real money

MSK Serverless bills per partition-hour and per GB in/out/retained even when idle. The EMR learning
cluster bills continuously while running (multiple `m5.xlarge` nodes by default). If you enable MWAA, it
has no free/idle tier either, and it also brings in a NAT Gateway (hourly + per-GB). **Destroy what
you're not using (`terraform destroy`) when you're done with the demo/training session.** Don't leave any
of this running.

## Prerequisites

1. An AWS account and credentials configured for Terraform (`aws configure` or equivalent env vars). If the
   AWS CLI isn't installed yet:
   ```bash
   brew install awscli
   aws configure   # or `aws sso login` / AWS_* env vars
   ```
2. Terraform CLI installed. It's no longer in homebrew-core (HashiCorp pulled it after their license
   change), so install it from HashiCorp's own tap:
   ```bash
   brew tap hashicorp/tap
   brew install hashicorp/tap/terraform
   ```
3. An existing VPC with **2 subnets**, any AZ pairing — that's all MSK Serverless and EMR need. Neither
   requires the subnets to be private; a plain route to an Internet Gateway is fine.

   **Only if you set `enable_mwaa = true`** (see Usage below), you additionally need one more existing
   **public** subnet, distinct from the 2 above, to host a NAT Gateway — MWAA (unlike MSK/EMR) rejects
   subnets that route directly to an Internet Gateway. `networking.tf` then moves the 2 subnets onto a
   dedicated route table through that NAT Gateway. If those 2 subnets started life as public, also
   disable their "auto-assign public IPv4" attribute — MWAA checks this independently of the route table
   and will reject them with `The subnets must be private` even after `networking.tf` fixes their
   routing:
   ```bash
   aws ec2 modify-subnet-attribute --subnet-id <subnet-id> --no-map-public-ip-on-launch
   ```
   (repeat for both). Terraform doesn't manage this attribute, so it has to be done by hand.

## Usage

The EMR learning cluster's bootstrap action needs the `retail_lakehouse` wheel already in S3 at creation
time, which means the S3 bucket needs to exist first. Provisioning happens in three phases: create just
the S3 bucket, upload the wheel to it, then apply everything else — there's no need to exclude the EMR
cluster from that last apply, since the wheel is already in place by the time it runs.

```bash
cd infra/terraform
cp terraform.tfvars.example terraform.tfvars
# edit terraform.tfvars: vpc_id, subnet_ids (2 subnets). Leave enable_mwaa unset (defaults to false).

terraform init
terraform apply -target=aws_s3_bucket.lakehouse -target=aws_s3_bucket_versioning.lakehouse \
  -target=aws_s3_bucket_server_side_encryption_configuration.lakehouse \
  -target=aws_s3_bucket_public_access_block.lakehouse \
  -target=aws_s3_bucket_lifecycle_configuration.lakehouse
```

```bash
cd ..
BUCKET=$(terraform -chdir=infra/terraform output -raw lakehouse_bucket_name)
scripts/deploy_aws.sh "$BUCKET"                     # builds + uploads the wheel to the now-existing bucket
```

Now apply everything else. Pick **one**:

**A. Without MWAA** (default — S3 + MSK + the persistent EMR learning cluster + JupyterHub):

```bash
cd infra/terraform
terraform plan
terraform apply
terraform output next_steps
```

**B. With MWAA** (scheduled production pipeline — see Prerequisites above for the extra subnet this
needs, and `RUNBOOK_AWS.md`'s "Optional: the scheduled production pipeline (MWAA)" section for the
post-apply Airflow setup):

```bash
cd infra/terraform
# in terraform.tfvars, set:
#   enable_mwaa           = true
#   nat_gateway_subnet_id = "subnet-xxxxx"   # extra public subnet, distinct from subnet_ids
terraform plan
terraform apply
terraform output next_steps
```

Follow the printed `next_steps` output — it walks through fetching bootstrap brokers and creating the
Kafka topic. No credential registration step is needed: EMR authenticates to MSK via the EC2 instance
profile in `iam.tf`, attached automatically.

You can switch between paths A and B later by flipping `enable_mwaa` in `terraform.tfvars` and re-running
`terraform apply`.

## Files

- `versions.tf` — Terraform/provider version pins.
- `variables.tf` — all inputs; see `terraform.tfvars.example` for a starting point.
- `s3.tf` — the lakehouse bucket, versioning, encryption, public access block, checkpoint lifecycle rule.
- `networking.tf` — only creates anything when `var.enable_mwaa = true`: a shared NAT Gateway (hosted in
  `var.nat_gateway_subnet_id`, which must stay public), an Elastic IP for it, and a dedicated route table
  pointing `var.subnet_ids` at it — MWAA rejects subnets that route directly to an Internet Gateway; MSK
  and EMR don't care either way. Always creates a free S3 gateway VPC endpoint (regardless of
  `enable_mwaa`) attached to every route table in `var.vpc_id`, so S3 traffic doesn't route through (and
  get billed by) the NAT Gateway when it exists.
- `msk.tf` — MSK Serverless cluster + its security group. Ingress is allowed from a Terraform-managed
  `emr_msk_client` marker security group, which any EMR cluster (persistent or ephemeral) attaches to get
  MSK access — see the comment on that resource for why.
- `iam.tf` — the EC2 instance profile role EMR clusters use to access MSK, S3, and Glue (plus the EMR
  service role). Classic instance-profile pattern — EMR is real EC2, so there's no service-credential
  indirection needed. **Verify the rendered MSK topic/group ARNs in the AWS console after apply** — MSK
  IAM ARN shapes for topics/consumer groups aren't simply derivable from the cluster ARN by string
  substitution across all AWS partitions, so double-check before relying on this in a real account.
- `emr_learning.tf` — a persistent EMR cluster with JupyterHub, for interactive/teaching notebooks (see
  `emr-notebooks/` and `class-emr/`). Bills continuously while running. Always created, regardless of
  `enable_mwaa`.
- `mwaa.tf` — the MWAA (managed Airflow) environment that orchestrates the production pipeline (see
  `airflow/dags/`). Every resource in this file is gated behind `var.enable_mwaa` (default `false`) —
  none of it exists unless you opt in. The webserver is set to `PUBLIC_ONLY` for simplicity in this
  demo/training context — MWAA still enforces IAM/console-login auth on top of that, but it's a step down
  from the SSM-only access pattern `emr_learning.tf` uses for JupyterHub. Switch to `PRIVATE_ONLY` plus a
  VPC-internal access path if that tradeoff doesn't fit your environment.
- `outputs.tf` — the `next_steps` runbook text printed after apply.

## Teardown

```bash
terraform destroy
```

Note the S3 bucket has versioning enabled; `terraform destroy` will fail to delete a non-empty bucket. Empty
it first if you want a full teardown:

```bash
aws s3 rm s3://$(terraform output -raw lakehouse_bucket_name) --recursive
terraform destroy
```

This tears down everything this Terraform manages -- the MSK Serverless cluster, the persistent EMR
learning cluster, the S3 bucket, the S3 gateway endpoint, all IAM roles/security groups, and (if
`enable_mwaa` was ever `true`) the MWAA environment, the NAT Gateway + Elastic IP + private route table.

Verify nothing's left billing after:

```bash
aws emr list-clusters --active
aws kafka list-clusters
aws mwaa list-environments
```

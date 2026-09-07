# Terraform: S3 + MSK Serverless + EMR + MWAA for the Retail Lakehouse project

This provisions the AWS infrastructure the production pipeline needs, entirely within AWS (no
Databricks dependency for this track):

- An **S3 bucket** for Delta table data, streaming checkpoints, source files, and MWAA's DAGs/requirements.
- An **MSK Serverless cluster** (IAM-authenticated) for the clickstream Kafka topic.
- A **persistent EMR cluster** with JupyterHub, for interactive/teaching notebooks.
- An **MWAA (managed Airflow) environment** to orchestrate the production pipeline on ephemeral EMR clusters.

## This costs real money

MSK Serverless bills per partition-hour and per GB in/out/retained even when idle. The EMR learning
cluster bills continuously while running (multiple `m5.xlarge` nodes by default). MWAA has no free/idle
tier either. **Destroy what you're not using (`terraform destroy`) when you're done with the demo/training
session.** Don't leave any of this running.

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
3. An existing VPC with exactly 2 private subnets in different AZs (that's the minimum both MSK Serverless
   and MWAA require — a 3rd buys nothing), each with a route to outbound internet for MWAA and the EMR
   bootstrap action (which downloads the `retail_lakehouse` wheel from S3 on cluster startup). For a
   training/demo account, route both subnets to a single shared NAT Gateway rather than one per AZ — this
   workload doesn't need per-AZ NAT redundancy, and it roughly halves the NAT Gateway hourly cost.
   `networking.tf` adds a free S3 gateway endpoint on top of that, so the S3 traffic this project generates
   (Delta tables, checkpoints, the bootstrap wheel, DAG sync) doesn't hit the NAT Gateway's per-GB charge
   at all.

## Usage

```bash
cd infra/terraform
cp terraform.tfvars.example terraform.tfvars
# edit terraform.tfvars: vpc_id, subnet_ids

terraform init
terraform plan
terraform apply

terraform output next_steps
```

Follow the printed `next_steps` output — it walks through fetching bootstrap brokers and creating the
Kafka topic. No credential registration step is needed: EMR authenticates to MSK via the EC2 instance
profile in `iam.tf`, attached automatically to any cluster built from this Terraform.

## Files

- `versions.tf` — Terraform/provider version pins.
- `variables.tf` — all inputs; see `terraform.tfvars.example` for a starting point.
- `s3.tf` — the lakehouse bucket, versioning, encryption, public access block, checkpoint lifecycle rule.
- `networking.tf` — a free S3 gateway VPC endpoint attached to every route table in `var.vpc_id`, so S3
  traffic doesn't route through (and get billed by) the NAT Gateway.
- `msk.tf` — MSK Serverless cluster + its security group. Ingress is allowed from a Terraform-managed
  `emr_msk_client` marker security group, which any EMR cluster (persistent or ephemeral) attaches to get
  MSK access — see the comment on that resource for why.
- `iam.tf` — the EC2 instance profile role EMR clusters use to access MSK, S3, and Glue (plus the EMR
  service role). Classic instance-profile pattern — EMR is real EC2, so there's no service-credential
  indirection needed. **Verify the rendered MSK topic/group ARNs in the AWS console after apply** — MSK
  IAM ARN shapes for topics/consumer groups aren't simply derivable from the cluster ARN by string
  substitution across all AWS partitions, so double-check before relying on this in a real account.
- `emr_learning.tf` — a persistent EMR cluster with JupyterHub, for interactive/teaching notebooks (see
  `emr-notebooks/` and `class-emr/`). Bills continuously while running.
- `mwaa.tf` — the MWAA (managed Airflow) environment that orchestrates the production pipeline (see
  `airflow/dags/`). The webserver is set to `PUBLIC_ONLY` for simplicity in this
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

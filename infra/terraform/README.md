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

1. An AWS account and credentials configured for Terraform (`aws configure` or equivalent env vars).
2. An existing VPC with at least 2 private subnets in different AZs, each with a route to a NAT gateway
   (or S3 gateway endpoint) for outbound internet/S3 access — required by MWAA and by the EMR bootstrap
   action, which downloads the `retail_lakehouse` wheel from S3 on cluster startup.

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
- `msk.tf` — MSK Serverless cluster + its security group. Ingress is allowed from a Terraform-managed
  `emr_msk_client` marker security group, which any EMR cluster (persistent or ephemeral) attaches to get
  MSK access — see the comment on that resource for why.
- `iam.tf` — the EC2 instance profile role EMR clusters use to access MSK, S3, and Glue (plus the EMR
  service role). Classic instance-profile pattern — EMR is real EC2, so there's no service-credential
  indirection needed. **Verify the rendered MSK topic/group ARNs in the AWS console after apply** — MSK
  IAM ARN shapes for topics/consumer groups aren't simply derivable from the cluster ARN by string
  substitution across all AWS partitions, so double-check before relying on this in a real account.
- `emr_learning.tf` — a persistent EMR cluster with JupyterHub, for interactive/teaching notebooks (see
  `emr-notebooks/` and `class-emr/`, added in later plans). Bills continuously while running.
- `mwaa.tf` — the MWAA (managed Airflow) environment that orchestrates the production pipeline (see
  `airflow/dags/`, added in a later plan). The webserver is set to `PUBLIC_ONLY` for simplicity in this
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

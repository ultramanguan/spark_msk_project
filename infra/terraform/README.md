# Terraform: S3 + MSK Serverless for the Retail Lakehouse project

This provisions the two pieces of real AWS infrastructure the pipeline needs beyond Databricks itself:

- An **S3 bucket** for Delta table data, streaming checkpoints, and source files.
- An **MSK Serverless cluster** (IAM-authenticated) for the clickstream Kafka topic.

## This costs real money

MSK Serverless bills per partition-hour and per GB in/out/retained even when idle. **Destroy it
(`terraform destroy`) when you're done with the demo/training session.** Don't leave this running.

## Prerequisites

1. An AWS account and credentials configured for Terraform (`aws configure` or equivalent env vars).
2. An existing VPC with at least 2 private subnets in different AZs, reachable from your Databricks
   workspace's compute plane. If your Databricks workspace uses AWS-managed VPC (the default,
   non-customer-managed), you'll need VPC peering or Transit Gateway between that VPC and this one —
   ask your platform/network team, or deploy Databricks with a customer-managed VPC that already contains
   (or can be peered cheaply with) these subnets. This is genuinely the most involved manual step; there is
   no way to fully automate it without knowing your organization's networking setup.
3. The security group ID attached to your Databricks cluster's ENIs (find it in the AWS VPC console, in the
   VPC associated with your Databricks workspace).

## Usage

```bash
cd infra/terraform
cp terraform.tfvars.example terraform.tfvars
# edit terraform.tfvars: vpc_id, subnet_ids, databricks_security_group_id

terraform init
terraform plan
terraform apply

terraform output next_steps
```

Follow the printed `next_steps` output — it walks through fetching bootstrap brokers and creating the
Kafka topic. No credential registration step is needed anymore: EMR authenticates to MSK via the EC2
instance profile in `iam.tf`, attached automatically to any cluster built from this Terraform.

## Files

- `versions.tf` — Terraform/provider version pins.
- `variables.tf` — all inputs; see `terraform.tfvars.example` for a starting point.
- `s3.tf` — the lakehouse bucket, versioning, encryption, public access block, checkpoint lifecycle rule.
- `msk.tf` — MSK Serverless cluster + its security group (IAM auth, port 9098 from the Databricks SG only).
- `iam.tf` — the EC2 instance profile role EMR clusters use to access MSK, S3, and Glue (plus the EMR
  service role). Classic instance-profile pattern — EMR is real EC2, so there's no service-credential
  indirection needed. **Verify the rendered MSK topic/group ARNs in the AWS console after apply** — MSK
  IAM ARN shapes for topics/consumer groups aren't simply derivable from the cluster ARN by string
  substitution across all AWS partitions, so double-check before relying on this in a real account.
- `emr_learning.tf` — a persistent EMR cluster with JupyterHub, for interactive/teaching notebooks (see
  `emr-notebooks/` and `class-emr/`, added in later plans). Bills continuously while running.
- `mwaa.tf` — the MWAA (managed Airflow) environment that orchestrates the production pipeline (see
  `airflow/dags/`, added in a later plan).
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

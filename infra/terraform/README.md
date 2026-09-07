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
3. An existing VPC with 3 subnets: 2 that will become private (any AZ pairing works — `networking.tf`
   moves them onto a dedicated route table through a shared NAT Gateway, which is what both MSK Serverless
   and MWAA require), plus 1 separate **public** subnet (route to an Internet Gateway) to host that NAT
   Gateway — a NAT Gateway can't sit inside the private subnets it serves. `networking.tf` also adds a
   free S3 gateway endpoint, so the S3 traffic this project generates (Delta tables, checkpoints, the
   bootstrap wheel, DAG sync) doesn't hit the NAT Gateway's per-GB charge at all.

   If the 2 subnets you're repurposing were originally public, also disable their
   "auto-assign public IPv4" attribute — MWAA checks this independently of the route table and will
   reject them with `The subnets must be private` even after `networking.tf` fixes their routing:
   ```bash
   aws ec2 modify-subnet-attribute --subnet-id <subnet-id> --no-map-public-ip-on-launch
   ```
   (repeat for both). Terraform doesn't manage this attribute, so it has to be done by hand.

## Usage

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

Follow the printed `next_steps` output — it walks through fetching bootstrap brokers and creating the
Kafka topic. No credential registration step is needed: EMR authenticates to MSK via the EC2 instance
profile in `iam.tf`, attached automatically to any cluster built from this Terraform.

## Files

- `versions.tf` — Terraform/provider version pins.
- `variables.tf` — all inputs; see `terraform.tfvars.example` for a starting point.
- `s3.tf` — the lakehouse bucket, versioning, encryption, public access block, checkpoint lifecycle rule.
- `networking.tf` — makes `var.subnet_ids` private: a shared NAT Gateway (hosted in `var.nat_gateway_subnet_id`,
  which must stay public), an Elastic IP for it, and a dedicated route table pointing `var.subnet_ids` at
  it — required because MWAA rejects subnets that route directly to an Internet Gateway. Also adds a free
  S3 gateway VPC endpoint attached to every route table in `var.vpc_id`, so S3 traffic doesn't route
  through (and get billed by) the NAT Gateway.
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

This tears down everything this Terraform manages -- the MSK Serverless cluster, the persistent EMR
learning cluster, the MWAA environment, the S3 bucket, the NAT Gateway + Elastic IP + private route table,
the S3 gateway endpoint, and all IAM roles/security groups. `var.subnet_ids` revert to whatever route
table they'd use by default once their explicit association here is removed; `var.nat_gateway_subnet_id`
was never modified, since it was already public.

Verify nothing's left billing after:

```bash
aws emr list-clusters --active
aws kafka list-clusters
aws mwaa list-environments
```


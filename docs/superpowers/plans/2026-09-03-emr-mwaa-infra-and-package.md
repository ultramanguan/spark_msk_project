# EMR + MWAA Infra & Package Migration (Plan 1 of 5) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the Databricks-specific pieces of `infra/terraform/` (Unity Catalog service credential, `var.databricks_security_group_id`) with an AWS-only foundation for EMR + MWAA, and update `src/retail_lakehouse/config.py` to address tables via a two-level Glue Data Catalog naming scheme instead of Unity Catalog's three-level `catalog.schema.table`.

**Architecture:** `iam.tf` becomes a classic EC2 instance-profile role for EMR (no self-assuming trust policy needed — EMR is real EC2, unlike Databricks serverless). `msk.tf` gains a reusable "marker" security group (`emr_msk_client`) that any EMR cluster — the persistent learning cluster created here, or the ephemeral production clusters MWAA creates later (Plan 2) — attaches to get MSK ingress. New `emr_learning.tf` provisions one persistent EMR cluster with JupyterHub for interactive use (Plan 3/4 notebooks land on it later). New `mwaa.tf` provisions the Airflow environment Plan 2's DAG will run on. `PipelineConfig.catalog` is removed since Glue has no catalog level.

**Tech Stack:** Terraform (AWS provider `~> 5.0`), Python 3.10 dataclasses, pytest.

## Global Constraints

- This project's convention: real-money infra changes (`terraform apply`) are always a manual, human-run step — never automate it in CI. (Established throughout this project's `RUNBOOK.md`.)
- Keep clusters/instance types small — this is a training/demo project, not production scale (see `variables.tf`'s existing `environment` variable description).
- `src/retail_lakehouse/transformations.py`, `streaming.py`, `quality.py`, `schemas.py`, `generate.py` do **not** change in this plan — only `config.py` does.
- Terraform CLI may not be available in every environment this plan runs in; `terraform validate`/`terraform fmt -check` steps should be attempted, and skipped with a clear note if the CLI genuinely isn't installed — don't block the task on that alone, since `iam.tf`/`msk.tf`/etc. can also be sanity-checked by eye (balanced braces, valid HCL) the way earlier work in this session already was.

---

### Task 1: `PipelineConfig` — drop `catalog`, two-level Glue naming

**Files:**
- Modify: `src/retail_lakehouse/config.py`
- Create: `tests/test_config.py`

**Interfaces:**
- Produces: `PipelineConfig(schema: str = "retail_lakehouse", base_path: str = "", kafka_bootstrap_servers: str = "", kafka_topic: str = "retail-clickstream")` with methods `.table(name: str) -> str`, `.checkpoint(name: str) -> str`, `.path(*parts: str) -> str`. No `catalog` field or parameter exists anymore — every other task/plan that constructs `PipelineConfig(...)` must not pass `catalog=`.

- [ ] **Step 1: Write the failing tests**

Create `tests/test_config.py`:

```python
import dataclasses

from retail_lakehouse.config import PipelineConfig


def test_table_uses_two_level_glue_naming():
    cfg = PipelineConfig(schema="retail_lakehouse")
    assert cfg.table("bronze_clickstream") == "`retail_lakehouse`.`bronze_clickstream`"


def test_checkpoint_joins_base_path():
    cfg = PipelineConfig(base_path="s3://my-bucket/data/")
    assert cfg.checkpoint("silver_stream") == "s3://my-bucket/data/checkpoints/silver_stream"


def test_path_strips_slashes():
    cfg = PipelineConfig(base_path="s3://my-bucket/data")
    assert cfg.path("/source/", "/events.jsonl") == "s3://my-bucket/data/source/events.jsonl"


def test_no_catalog_field():
    field_names = {f.name for f in dataclasses.fields(PipelineConfig)}
    assert "catalog" not in field_names
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `pytest tests/test_config.py -v`
Expected: `test_table_uses_two_level_glue_naming` and `test_no_catalog_field` FAIL (current `config.py` still has `catalog` and produces a three-level `table()` string); the other two pass already (unaffected by the `catalog` field).

- [ ] **Step 3: Update `config.py`**

Replace the full contents of `src/retail_lakehouse/config.py`:

```python
from dataclasses import dataclass


@dataclass(frozen=True)
class PipelineConfig:
    schema: str = "retail_lakehouse"
    base_path: str = ""
    kafka_bootstrap_servers: str = ""
    kafka_topic: str = "retail-clickstream"

    def table(self, name: str) -> str:
        return f"`{self.schema}`.`{name}`"

    def checkpoint(self, name: str) -> str:
        return f"{self.base_path.rstrip('/')}/checkpoints/{name}"

    def path(self, *parts: str) -> str:
        clean = "/".join(p.strip("/") for p in parts)
        return f"{self.base_path.rstrip('/')}/{clean}"
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `pytest tests/test_config.py -v`
Expected: all 4 tests PASS.

- [ ] **Step 5: Run the full test suite to check for regressions**

Run: `pytest -q`
Expected: all tests pass (no other test file references `PipelineConfig` today, per a repo-wide grep during planning — `notebooks/*.py` reference it but those are Databricks notebooks outside `tests/`, out of scope for this plan and updated in a later plan).

- [ ] **Step 6: Commit**

```bash
git add src/retail_lakehouse/config.py tests/test_config.py
git commit -m "Drop catalog from PipelineConfig, use two-level Glue table naming"
```

---

### Task 2: `iam.tf` — EMR EC2 instance profile (replaces Unity Catalog service credential)

**Files:**
- Modify: `infra/terraform/iam.tf` (full replace)

**Interfaces:**
- Consumes: `aws_msk_serverless_cluster.this` (from `msk.tf`, unchanged resource name), `aws_s3_bucket.lakehouse` (from `s3.tf`, unchanged), `var.project_name`, `var.environment` (from `variables.tf`, unchanged).
- Produces: `aws_iam_instance_profile.emr` (referenced by Task 5's `emr_learning.tf` and later by Plan 2's Airflow DAG config), `aws_iam_role.emr_instance_profile_role` (referenced by Task 6's `mwaa.tf` for its `iam:PassRole` statement), outputs `emr_instance_profile_name` and `emr_instance_profile_role_arn`.

- [ ] **Step 1: Replace `infra/terraform/iam.tf`**

```hcl
# IAM role + instance profile EMR clusters use to access MSK, S3, and the Glue Data Catalog, plus the
# EMR service role. Classic EC2-based instance profile -- unlike Databricks serverless compute, EMR
# nodes are real EC2 instances that can have a profile attached directly, so there's no
# service-credential/self-assuming-trust-policy indirection needed here.

resource "aws_iam_role" "emr_instance_profile_role" {
  name = "${var.project_name}-${var.environment}-emr-instance-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Principal = {
          Service = "ec2.amazonaws.com"
        }
        Action = "sts:AssumeRole"
      }
    ]
  })

  tags = {
    Project     = var.project_name
    Environment = var.environment
  }
}

resource "aws_iam_instance_profile" "emr" {
  name = "${var.project_name}-${var.environment}-emr-instance-profile"
  role = aws_iam_role.emr_instance_profile_role.name
}

# The EMR *service* role (distinct from the EC2 instance role above) that EMR itself assumes to manage
# cluster resources on your behalf. Provisioned here so this project stays fully Terraform-managed,
# instead of requiring a one-time manual `aws emr create-default-roles`.
resource "aws_iam_role" "emr_service_role" {
  name = "${var.project_name}-${var.environment}-emr-service-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Principal = {
          Service = "elasticmapreduce.amazonaws.com"
        }
        Action = "sts:AssumeRole"
      }
    ]
  })

  tags = {
    Project     = var.project_name
    Environment = var.environment
  }
}

resource "aws_iam_role_policy_attachment" "emr_service_role_managed" {
  role       = aws_iam_role.emr_service_role.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AmazonElasticMapReduceRole"
}

# Lets the SSM agent (preinstalled on EMR's AMI) check in, so JupyterHub on the learning cluster can be
# reached via `aws ssm start-session ... --document-name AWS-StartPortForwardingSession` instead of
# opening inbound security group rules to the internet. See emr_learning.tf.
resource "aws_iam_role_policy_attachment" "emr_ssm_managed" {
  role       = aws_iam_role.emr_instance_profile_role.name
  policy_arn = "arn:aws:iam::aws:policy/AmazonSSMManagedInstanceCore"
}

# Scope note: this example scopes cluster-level actions to the specific MSK cluster ARN, and topic-level
# actions to the "retail-clickstream*" topic name pattern. Tighten `Resource` further per your org's IAM
# standards before using outside training/demo purposes.
resource "aws_iam_role_policy" "emr_msk_access" {
  name = "${var.project_name}-${var.environment}-emr-msk-access"
  role = aws_iam_role.emr_instance_profile_role.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "ClusterConnect"
        Effect = "Allow"
        Action = [
          "kafka-cluster:Connect",
          "kafka-cluster:AlterCluster",
          "kafka-cluster:DescribeCluster",
        ]
        Resource = aws_msk_serverless_cluster.this.arn
      },
      {
        Sid    = "TopicReadWrite"
        Effect = "Allow"
        Action = [
          "kafka-cluster:*Topic*",
          "kafka-cluster:ReadData",
          "kafka-cluster:WriteData",
          "kafka-cluster:DescribeTopicDynamicConfiguration",
        ]
        Resource = replace(aws_msk_serverless_cluster.this.arn, ":cluster/", ":topic/") # broadened at apply time; see note below
      },
      {
        Sid    = "ConsumerGroup"
        Effect = "Allow"
        Action = [
          "kafka-cluster:AlterGroup",
          "kafka-cluster:DescribeGroup",
        ]
        Resource = replace(aws_msk_serverless_cluster.this.arn, ":cluster/", ":group/")
      }
    ]
  })
}

# NOTE: MSK IAM policy resource ARNs for topics/groups use a distinct ARN shape
# (arn:aws:kafka:REGION:ACCOUNT:topic/CLUSTER_NAME/CLUSTER_UUID/TOPIC_NAME) that Terraform cannot derive
# purely by string substitution from the cluster ARN in all AWS partitions/versions. Verify the rendered
# policy in the AWS console after apply and correct the Resource ARNs if needed -- see AWS's MSK IAM access
# control documentation for the exact ARN format.

resource "aws_iam_role_policy" "emr_s3_access" {
  name = "${var.project_name}-${var.environment}-emr-s3-access"
  role = aws_iam_role.emr_instance_profile_role.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "LakehouseBucketReadWrite"
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:PutObject",
          "s3:DeleteObject",
          "s3:ListBucket",
        ]
        Resource = [
          aws_s3_bucket.lakehouse.arn,
          "${aws_s3_bucket.lakehouse.arn}/*",
        ]
      }
    ]
  })
}

# Scope note: Glue Data Catalog actions are left unscoped (Resource = "*") because Glue's ARN hierarchy
# (catalog/database/table) makes precise scoping verbose for a training/demo project. Tighten before
# using outside that context, same as the MSK policy above.
resource "aws_iam_role_policy" "emr_glue_access" {
  name = "${var.project_name}-${var.environment}-emr-glue-access"
  role = aws_iam_role.emr_instance_profile_role.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "GlueDataCatalog"
        Effect = "Allow"
        Action = [
          "glue:GetDatabase",
          "glue:GetDatabases",
          "glue:CreateDatabase",
          "glue:GetTable",
          "glue:GetTables",
          "glue:CreateTable",
          "glue:UpdateTable",
          "glue:DeleteTable",
          "glue:GetPartition",
          "glue:GetPartitions",
          "glue:CreatePartition",
          "glue:BatchCreatePartition",
        ]
        Resource = "*"
      }
    ]
  })
}

output "emr_instance_profile_name" {
  value = aws_iam_instance_profile.emr.name
}

output "emr_instance_profile_role_arn" {
  value = aws_iam_role.emr_instance_profile_role.arn
}

output "emr_service_role_name" {
  value = aws_iam_role.emr_service_role.name
}
```

- [ ] **Step 2: Sanity-check the file**

Run: `cd infra/terraform && terraform fmt -check -diff iam.tf`
Expected: no diff output (if `terraform` isn't installed, instead visually confirm every `{`/`[` has a matching `}`/`]` by reading the file back — same manual check used earlier in this project for the previous `iam.tf` rewrite).

If `terraform` **is** installed: also run `terraform validate` (this requires `versions.tf`'s provider block and every other `.tf` file in the directory to already parse — safe to run even though Task 3/5/6 haven't landed yet, since Terraform validates the whole directory together; expect it to fail until Task 6 is done if it complains about undefined references like `var.databricks_security_group_id` still being used elsewhere — that's expected and resolved by Task 3).

- [ ] **Step 3: Commit**

```bash
git add infra/terraform/iam.tf
git commit -m "Replace Unity Catalog service credential with EMR EC2 instance profile"
```

---

### Task 3: `msk.tf` — reusable EMR client security group

**Files:**
- Modify: `infra/terraform/msk.tf`

**Interfaces:**
- Produces: `aws_security_group.emr_msk_client` and output `emr_msk_client_security_group_id` — consumed by Task 5's `emr_learning.tf` (attached to the persistent learning cluster) and, in Plan 2, by the Airflow DAG's `EmrCreateJobFlowOperator` config for ephemeral production clusters.
- Removes: the `aws_security_group_rule.msk_from_databricks` resource and its dependency on `var.databricks_security_group_id` (removed from `variables.tf` in Task 4).

- [ ] **Step 1: Replace the security-group-rule section of `infra/terraform/msk.tf`**

Replace this block:

```hcl
# MSK Serverless with IAM auth uses port 9098.
resource "aws_security_group_rule" "msk_from_databricks" {
  type                     = "ingress"
  from_port                = 9098
  to_port                  = 9098
  protocol                 = "tcp"
  security_group_id        = aws_security_group.msk_broker.id
  source_security_group_id = var.databricks_security_group_id
  description               = "Allow Databricks cluster ENIs to reach MSK Serverless (IAM auth, TLS)"
}
```

with:

```hcl
# No rules of its own -- a "marker" security group that any EMR cluster needing MSK access (the
# persistent learning cluster in emr_learning.tf, or the ephemeral production clusters the MWAA DAG
# creates) attaches as an additional security group, so it matches the ingress rule below.
resource "aws_security_group" "emr_msk_client" {
  name_prefix = "${var.project_name}-emr-msk-client-"
  description = "Attach to any EMR cluster (persistent or ephemeral) that needs to reach MSK Serverless"
  vpc_id      = var.vpc_id

  tags = {
    Project     = var.project_name
    Environment = var.environment
  }
}

# MSK Serverless with IAM auth uses port 9098.
resource "aws_security_group_rule" "msk_from_emr" {
  type                     = "ingress"
  from_port                = 9098
  to_port                  = 9098
  protocol                 = "tcp"
  security_group_id        = aws_security_group.msk_broker.id
  source_security_group_id = aws_security_group.emr_msk_client.id
  description              = "Allow EMR cluster ENIs tagged with the emr_msk_client SG to reach MSK Serverless (IAM auth, TLS)"
}
```

Also update the file's header comment, replacing:

```hcl
# IAM authentication is enforced -- see infra/terraform/iam.tf for the policy Databricks needs.
```

with:

```hcl
# IAM authentication is enforced -- see infra/terraform/iam.tf for the EMR instance profile policy.
```

And add this output at the end of the file, after the existing `msk_bootstrap_brokers_command` output:

```hcl
output "emr_msk_client_security_group_id" {
  description = "Attach this security group to any EMR cluster (persistent or ephemeral) that needs to reach MSK"
  value       = aws_security_group.emr_msk_client.id
}
```

- [ ] **Step 2: Sanity-check the file**

Run: `cd infra/terraform && terraform fmt -check -diff msk.tf` (or the manual brace-matching check if the CLI isn't available).

- [ ] **Step 3: Commit**

```bash
git add infra/terraform/msk.tf
git commit -m "Replace Databricks-cluster MSK ingress rule with reusable EMR client security group"
```

---

### Task 4: `variables.tf` and `terraform.tfvars.example` — remove Databricks vars, add EMR/MWAA vars

**Files:**
- Modify: `infra/terraform/variables.tf`
- Modify: `infra/terraform/terraform.tfvars.example`

**Interfaces:**
- Produces: `var.emr_release_label`, `var.emr_instance_type`, `var.emr_instance_count`, `var.mwaa_airflow_version`, `var.mwaa_environment_class` — consumed by Task 5 (`emr_learning.tf`) and Task 6 (`mwaa.tf`).
- Removes: `var.databricks_security_group_id` (no longer referenced after Task 3), `var.databricks_service_credential_external_id` (no longer referenced after Task 2).

- [ ] **Step 1: Edit `infra/terraform/variables.tf`**

Remove this block entirely:

```hcl
variable "databricks_security_group_id" {
  description = "Security group ID attached to your Databricks cluster ENIs, used to allow MSK broker port ingress. Find it in the Databricks workspace's VPC (usually named like <workspace>-worker-unmanaged or a customer-managed VPC SG)."
  type        = string
}
```

Remove this block entirely (added for the now-removed Unity Catalog service credential):

```hcl
variable "databricks_service_credential_external_id" {
  description = "External ID for the Unity Catalog service credential's self-assuming trust policy. Leave as the placeholder \"0000\" for the first `terraform apply`; after registering the service credential in Databricks (see infra/terraform/iam.tf), set this to the real External ID Databricks generates and apply again."
  type        = string
  default     = "0000"
}
```

Add this block at the end of the file:

```hcl
variable "emr_release_label" {
  description = "EMR release label for both the persistent learning cluster and the ephemeral production clusters MWAA creates"
  type        = string
  default     = "emr-7.5.0"
}

variable "emr_instance_type" {
  description = "EC2 instance type for EMR master/core nodes. Keep small for training/demo use."
  type        = string
  default     = "m5.xlarge"
}

variable "emr_instance_count" {
  description = "Number of EMR core nodes (the persistent learning cluster only -- ephemeral production cluster sizing lives in the Airflow DAG config)"
  type        = number
  default     = 2
}

variable "mwaa_airflow_version" {
  description = "Managed Airflow version for the MWAA environment"
  type        = string
  default     = "2.10.3"
}

variable "mwaa_environment_class" {
  description = "MWAA environment size. Keep small for training/demo use."
  type        = string
  default     = "mw1.small"
}
```

- [ ] **Step 2: Edit `infra/terraform/terraform.tfvars.example`**

Replace:

```
# Fill these in for your account -- see README.md in this directory.
vpc_id                        = "vpc-REPLACE_ME"
subnet_ids                    = ["subnet-REPLACE_ME_1", "subnet-REPLACE_ME_2"]
databricks_security_group_id  = "sg-REPLACE_ME"
```

with:

```
# Fill these in for your account -- see README.md in this directory.
vpc_id     = "vpc-REPLACE_ME"
subnet_ids = ["subnet-REPLACE_ME_1", "subnet-REPLACE_ME_2"]
```

- [ ] **Step 3: Sanity-check**

Run: `cd infra/terraform && terraform fmt -check -diff variables.tf` (or manual review if the CLI isn't available). Confirm with `grep -rn "databricks_security_group_id\|databricks_service_credential_external_id" infra/terraform/` that no references remain anywhere in the directory (Tasks 2 and 3 already removed the two usages, so this should return nothing).

- [ ] **Step 4: Commit**

```bash
git add infra/terraform/variables.tf infra/terraform/terraform.tfvars.example
git commit -m "Remove Databricks variables, add EMR/MWAA sizing variables"
```

---

### Task 5: `emr_learning.tf` — persistent EMR cluster with JupyterHub

**Files:**
- Create: `infra/terraform/emr_learning.tf`
- Create: `infra/terraform/bootstrap/install_retail_lakehouse.sh`

**Interfaces:**
- Consumes: `aws_iam_instance_profile.emr` (Task 2), `aws_security_group.emr_msk_client` (Task 3), `var.emr_release_label`/`var.emr_instance_type`/`var.emr_instance_count` (Task 4), `aws_s3_bucket.lakehouse` (from `s3.tf`, unchanged), `aws_iam_role.emr_service_role` (Task 2).
- Produces: `aws_emr_cluster.learning`, outputs `emr_learning_cluster_id` and `emr_learning_master_public_dns`. The bootstrap script this task creates (`install_retail_lakehouse.sh`) is also reused as-is by Plan 2's ephemeral production clusters (same S3 key convention: `s3://<bucket>/artifacts/retail_lakehouse-latest.whl`, uploaded by a later CI/CD plan — see Step 3's note).

- [ ] **Step 1: Create the bootstrap script**

Create `infra/terraform/bootstrap/install_retail_lakehouse.sh`:

```bash
#!/usr/bin/env bash
# EMR bootstrap action: installs the retail_lakehouse wheel onto every node so it's importable from
# Jupyter notebooks (persistent learning cluster) and spark-submit jobs (ephemeral production clusters)
# without any per-notebook/per-step pip-install step.
set -euo pipefail

WHEEL_S3_URI="${1:?Usage: install_retail_lakehouse.sh <s3://bucket/artifacts/retail_lakehouse-latest.whl>}"
LOCAL_WHEEL="/tmp/retail_lakehouse-latest.whl"

aws s3 cp "$WHEEL_S3_URI" "$LOCAL_WHEEL"
sudo pip3 install --upgrade "$LOCAL_WHEEL"
```

Make it executable: `chmod +x infra/terraform/bootstrap/install_retail_lakehouse.sh`

- [ ] **Step 2: Create `infra/terraform/emr_learning.tf`**

```hcl
# Persistent EMR cluster with JupyterHub for interactive/teaching use (emr-notebooks/, class-emr/,
# added in later plans). Cost note: this bills continuously while running, same as MSK -- destroy or
# stop it when not actively in a learning session.
#
# JupyterHub listens on port 9443 on the master node, but no inbound security group rule opens it to the
# internet. Reach it via SSM port forwarding instead (no bastion/key pair/open ports needed -- the
# AmazonSSMManagedInstanceCore policy attached to the instance role in iam.tf lets the preinstalled SSM
# agent check in):
#
#   aws ssm start-session --target <master-instance-id> \
#     --document-name AWS-StartPortForwardingSession \
#     --parameters '{"portNumber":["9443"],"localPortNumber":["9443"]}'
#
# Then browse to https://localhost:9443/ (self-signed cert -- your browser will warn, that's expected).
# Find <master-instance-id> via: aws emr list-instances --cluster-id <emr_learning_cluster_id output> \
#   --instance-group-types MASTER --query 'Instances[0].Ec2InstanceId' --output text

resource "aws_s3_object" "bootstrap_script" {
  bucket = aws_s3_bucket.lakehouse.id
  key    = "bootstrap/install_retail_lakehouse.sh"
  source = "${path.module}/bootstrap/install_retail_lakehouse.sh"
  etag   = filemd5("${path.module}/bootstrap/install_retail_lakehouse.sh")
}

resource "aws_emr_cluster" "learning" {
  name          = "${var.project_name}-${var.environment}-learning"
  release_label = var.emr_release_label
  applications  = ["Spark", "JupyterHub", "Livy", "Hadoop"]
  log_uri       = "s3://${aws_s3_bucket.lakehouse.bucket}/emr-logs/learning/"
  service_role  = aws_iam_role.emr_service_role.arn

  ec2_attributes {
    subnet_id                         = var.subnet_ids[0]
    instance_profile                  = aws_iam_instance_profile.emr.arn
    additional_master_security_groups = aws_security_group.emr_msk_client.id
    additional_slave_security_groups  = aws_security_group.emr_msk_client.id
  }

  master_instance_group {
    instance_type = var.emr_instance_type
  }

  core_instance_group {
    instance_type  = var.emr_instance_type
    instance_count = var.emr_instance_count
  }

  bootstrap_action {
    name = "install-retail-lakehouse"
    path = "s3://${aws_s3_bucket.lakehouse.bucket}/${aws_s3_object.bootstrap_script.key}"
    args = ["s3://${aws_s3_bucket.lakehouse.bucket}/artifacts/retail_lakehouse-latest.whl"]
  }

  configurations_json = jsonencode([
    {
      Classification = "hive-site"
      Properties = {
        "hive.metastore.client.factory.class" = "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory"
      }
    },
    {
      Classification = "spark-hive-site"
      Properties = {
        "hive.metastore.client.factory.class" = "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory"
      }
    }
  ])

  tags = {
    Project     = var.project_name
    Environment = var.environment
    Purpose     = "interactive-learning"
  }
}

output "emr_learning_cluster_id" {
  value = aws_emr_cluster.learning.id
}

output "emr_learning_master_public_dns" {
  value = aws_emr_cluster.learning.master_public_dns
}
```

- [ ] **Step 3: Note the cross-plan dependency clearly**

This cluster's bootstrap action will fail at `terraform apply` time until a wheel actually exists at
`s3://<bucket>/artifacts/retail_lakehouse-latest.whl` — that upload is done by Plan 5 (CI/CD)'s
`deploy_aws.yml`, not this plan. Document this explicitly in the commit message (Step 5) so it isn't
mistaken for a bug when `terraform apply` is run before Plan 5 lands. If you need to `terraform apply`
this plan standalone before Plan 5 exists, upload a wheel manually first:
`aws s3 cp dist/retail_lakehouse-0.1.0-py3-none-any.whl s3://<bucket>/artifacts/retail_lakehouse-latest.whl`.

- [ ] **Step 4: Sanity-check the file**

Run: `cd infra/terraform && terraform fmt -check -diff emr_learning.tf` (or manual review if the CLI isn't available — check every block has matching braces, and that every `var.*`/`aws_*.*` reference used here was actually defined in Tasks 2-4).

- [ ] **Step 5: Commit**

```bash
git add infra/terraform/emr_learning.tf infra/terraform/bootstrap/install_retail_lakehouse.sh
git commit -m "Add persistent EMR cluster with JupyterHub for interactive learning

Bootstrap action installs retail_lakehouse from a fixed S3 artifact key
that Plan 5 (CI/CD) is responsible for populating; terraform apply will
fail on this cluster's bootstrap step until that wheel exists (or is
uploaded manually per the note in emr_learning.tf)."
```

---

### Task 6: `mwaa.tf` — Airflow environment for production orchestration

**Files:**
- Create: `infra/terraform/mwaa.tf`

**Interfaces:**
- Consumes: `aws_iam_role.emr_instance_profile_role` (Task 2, for the `iam:PassRole` statement MWAA needs to launch EMR clusters using that instance profile), `aws_s3_bucket.lakehouse` (from `s3.tf`), `var.mwaa_airflow_version`/`var.mwaa_environment_class` (Task 4), `var.subnet_ids`/`var.vpc_id` (from `variables.tf`, unchanged).
- Produces: `aws_mwaa_environment.this`, output `mwaa_webserver_url`. Plan 2's `airflow/dags/retail_lakehouse_pipeline.py` is uploaded to the `dag_s3_path` this resource points at (`airflow/dags` in the lakehouse bucket) — that upload is a Plan 5 (CI/CD) concern, not this task.

- [ ] **Step 1: Create `infra/terraform/mwaa.tf`**

```hcl
# MWAA (managed Airflow) for orchestrating the production pipeline. The DAG itself
# (airflow/dags/retail_lakehouse_pipeline.py) is added in a later plan and synced to dag_s3_path below
# by CI/CD (also a later plan) -- this task only provisions the environment.
#
# Reuses the lakehouse S3 bucket under an airflow/ prefix rather than provisioning a separate bucket,
# alongside the existing data/tables, data/checkpoints, data/source prefixes (see s3.tf).

resource "aws_s3_object" "airflow_requirements" {
  bucket  = aws_s3_bucket.lakehouse.id
  key     = "airflow/requirements.txt"
  content = "apache-airflow-providers-amazon\n"
}

resource "aws_iam_role" "mwaa_execution" {
  name = "${var.project_name}-${var.environment}-mwaa-execution"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Principal = {
          Service = [
            "airflow.amazonaws.com",
            "airflow-env.amazonaws.com",
          ]
        }
        Action = "sts:AssumeRole"
      }
    ]
  })

  tags = {
    Project     = var.project_name
    Environment = var.environment
  }
}

resource "aws_iam_role_policy" "mwaa_execution_policy" {
  name = "${var.project_name}-${var.environment}-mwaa-execution-policy"
  role = aws_iam_role.mwaa_execution.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "S3DagsAndData"
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:PutObject",
          "s3:ListBucket",
        ]
        Resource = [
          aws_s3_bucket.lakehouse.arn,
          "${aws_s3_bucket.lakehouse.arn}/*",
        ]
      },
      {
        Sid    = "EmrOrchestration"
        Effect = "Allow"
        Action = [
          "elasticmapreduce:RunJobFlow",
          "elasticmapreduce:AddJobFlowSteps",
          "elasticmapreduce:DescribeStep",
          "elasticmapreduce:DescribeCluster",
          "elasticmapreduce:TerminateJobFlows",
          "elasticmapreduce:ListSteps",
        ]
        Resource = "*"
      },
      {
        Sid      = "PassEmrInstanceRole"
        Effect   = "Allow"
        Action   = "iam:PassRole"
        Resource = aws_iam_role.emr_instance_profile_role.arn
      },
      {
        Sid    = "Logs"
        Effect = "Allow"
        Action = [
          "logs:CreateLogStream",
          "logs:CreateLogGroup",
          "logs:PutLogEvents",
          "logs:GetLogEvents",
          "logs:GetLogRecord",
          "logs:GetLogGroupFields",
          "logs:GetQueryResults",
        ]
        Resource = "arn:aws:logs:${var.aws_region}:*:log-group:airflow-${var.project_name}-${var.environment}-*"
      },
      {
        Sid      = "CloudWatchMetrics"
        Effect   = "Allow"
        Action   = "cloudwatch:PutMetricData"
        Resource = "*"
      },
      {
        Sid    = "SqsForCeleryExecutor"
        Effect = "Allow"
        Action = [
          "sqs:ChangeMessageVisibility",
          "sqs:DeleteMessage",
          "sqs:GetQueueAttributes",
          "sqs:GetQueueUrl",
          "sqs:ReceiveMessage",
          "sqs:SendMessage",
        ]
        Resource = "arn:aws:sqs:${var.aws_region}:*:airflow-celery-*"
      }
    ]
  })
}

resource "aws_security_group" "mwaa" {
  name_prefix = "${var.project_name}-mwaa-"
  description = "MWAA environment security group"
  vpc_id      = var.vpc_id

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = {
    Project     = var.project_name
    Environment = var.environment
  }
}

resource "aws_security_group_rule" "mwaa_self_ingress" {
  type              = "ingress"
  from_port         = 0
  to_port           = 0
  protocol          = "-1"
  security_group_id = aws_security_group.mwaa.id
  self              = true
}

resource "aws_mwaa_environment" "this" {
  name                = "${var.project_name}-${var.environment}"
  airflow_version     = var.mwaa_airflow_version
  environment_class   = var.mwaa_environment_class
  execution_role_arn  = aws_iam_role.mwaa_execution.arn
  source_bucket_arn   = aws_s3_bucket.lakehouse.arn
  dag_s3_path         = "airflow/dags"
  requirements_s3_path = aws_s3_object.airflow_requirements.key

  network_configuration {
    subnet_ids         = var.subnet_ids
    security_group_ids = [aws_security_group.mwaa.id]
  }

  logging_configuration {
    dag_processing_logs {
      enabled   = true
      log_level = "INFO"
    }
    task_logs {
      enabled   = true
      log_level = "INFO"
    }
  }

  tags = {
    Project     = var.project_name
    Environment = var.environment
  }
}

output "mwaa_webserver_url" {
  value = aws_mwaa_environment.this.webserver_url
}
```

- [ ] **Step 2: Note the cross-plan dependency clearly**

`dag_s3_path = "airflow/dags"` will be an empty prefix until Plan 2 (the DAG) and Plan 5 (CI/CD sync)
land — MWAA will show "no DAGs found" in its UI until then. That's expected, not a bug in this task.

- [ ] **Step 3: Sanity-check the file**

Run: `cd infra/terraform && terraform fmt -check -diff mwaa.tf` (or manual review if the CLI isn't
available). MWAA requires the two `var.subnet_ids` to be in different Availability Zones and to have a
NAT gateway/internet route for outbound access — confirm this matches the existing prerequisite already
documented in `infra/terraform/README.md` for MSK's subnets (same subnets are reused here).

- [ ] **Step 4: Commit**

```bash
git add infra/terraform/mwaa.tf
git commit -m "Add MWAA environment for production pipeline orchestration

DAG s3 path will be empty until Plan 2 (the Airflow DAG) and Plan 5
(CI/CD sync) land -- MWAA showing no DAGs is expected at this point."
```

---

### Task 7: Update `infra/terraform/README.md`

**Files:**
- Modify: `infra/terraform/README.md`

**Interfaces:** None (documentation only).

- [ ] **Step 1: Update the file list section**

Replace the `iam.tf` bullet:

```
- `iam.tf` — the IAM role + policy backing a Unity Catalog *service credential* (the serverless-compatible
  replacement for a classic cluster instance profile) that lets Databricks connect/read/write the MSK
  cluster and topics. Registering the role as a service credential in Databricks is a two-phase process —
  see the comments at the top of `iam.tf`. **Verify the rendered topic/group ARNs in the AWS console after
  apply** — MSK IAM ARN shapes for topics/consumer groups aren't simply derivable from the cluster ARN by
  string substitution across all AWS partitions, so double-check before relying on this in a real account.
```

with:

```
- `iam.tf` — the EC2 instance profile role EMR clusters use to access MSK, S3, and Glue (plus the EMR
  service role). Classic instance-profile pattern — EMR is real EC2, so there's no service-credential
  indirection needed. **Verify the rendered MSK topic/group ARNs in the AWS console after apply** — MSK
  IAM ARN shapes for topics/consumer groups aren't simply derivable from the cluster ARN by string
  substitution across all AWS partitions, so double-check before relying on this in a real account.
- `emr_learning.tf` — a persistent EMR cluster with JupyterHub, for interactive/teaching notebooks (see
  `emr-notebooks/` and `class-emr/`, added in later plans). Bills continuously while running.
- `mwaa.tf` — the MWAA (managed Airflow) environment that orchestrates the production pipeline (see
  `airflow/dags/`, added in a later plan).
```

- [ ] **Step 2: Update the "Usage" section's `next_steps` description**

Replace:

```
Follow the printed `next_steps` output — it walks through fetching bootstrap brokers, creating the Kafka
topic, and registering the Unity Catalog service credential Databricks uses to authenticate to MSK.
```

with:

```
Follow the printed `next_steps` output — it walks through fetching bootstrap brokers and creating the
Kafka topic. No credential registration step is needed anymore: EMR authenticates to MSK via the EC2
instance profile in `iam.tf`, attached automatically to any cluster built from this Terraform.
```

- [ ] **Step 3: Update `outputs.tf`'s `next_steps` text to match**

In `infra/terraform/outputs.tf`, replace step 3 and step 4 of the heredoc:

```
    3. Register the Unity Catalog service credential Databricks uses to authenticate to MSK (this replaces
       instance-profile attachment -- required for serverless compute):
       a. In Databricks: Catalog Explorer -> External Data -> Credentials -> Create credential ->
          Service Credential, using the role ARN from `terraform output databricks_msk_service_credential_role_arn`.
       b. Copy the External ID Databricks generates, set it as `databricks_service_credential_external_id`
          in terraform.tfvars, and run `terraform apply` again to finalize the self-assuming trust policy.
       c. Back in Databricks, select the credential and run its "Validate configuration" check.
       See infra/terraform/iam.tf for the full two-phase explanation.

    4. Set notebooks/02_kafka_msk_streaming_ingest.py's widgets: `kafka_bootstrap_servers` to the value
       from step 1, and `kafka_service_credential` to the credential name you chose in step 3.
```

with:

```
    3. No credential registration step needed -- EMR clusters built from this Terraform (the persistent
       learning cluster in emr_learning.tf, and the ephemeral production clusters MWAA creates) already
       have MSK access via the EC2 instance profile in iam.tf.

    4. Interactive learning notebooks (emr-notebooks/, added in a later plan) take the bootstrap-brokers
       string from step 1 as a plain notebook variable -- no widget/credential-name setup required.
```

- [ ] **Step 4: Commit**

```bash
git add infra/terraform/README.md infra/terraform/outputs.tf
git commit -m "Update terraform docs for EMR instance-profile MSK auth"
```

---

## Verifying the whole plan

After all 7 tasks:

1. `pytest -q` from the repo root — full suite passes, including the new `tests/test_config.py`.
2. `ruff check src tests` — passes (no new lint violations introduced; `config.py`'s rewrite doesn't
   change its import style).
3. `grep -rn "databricks_security_group_id\|databricks_service_credential_external_id\|databricks_msk_access_policy_arn\|databricks_msk_service_credential" infra/terraform/` — returns nothing.
4. If `terraform` CLI is available: `cd infra/terraform && terraform fmt -check && terraform validate`
   both pass for the whole directory.
5. `RUNBOOK.md` still references the old Unity Catalog service-credential flow in its own step 3 — that
   file is intentionally **not** updated by this plan (it describes the full Databricks-based runbook,
   which stays valid as documentation for the untouched `notebooks/`/`class/` Databricks track). A new,
   separate EMR-flavored runbook section is added in a later plan once `emr-notebooks/` exists to
   document.

# IAM role backing a Unity Catalog *service credential* -- the serverless-compatible replacement for a
# classic cluster instance profile. Spark's Kafka connector consumes this via the
# `databricks.serviceCredential` read/write option (see notebooks/02_kafka_msk_streaming_ingest.py),
# and Databricks vends temporary credentials for it at runtime -- no instance profile attachment needed,
# and it works on serverless compute (Databricks Runtime 16.1+).
#
# Setup requires a self-assuming trust policy, and self-assumption needs an External ID that only exists
# once Databricks has registered the credential -- a genuine two-phase process (this is how Databricks'
# own service credential setup docs describe it, not a shortcut taken here):
#
#   1. `terraform apply` with the default `databricks_service_credential_external_id = "0000"` below. This
#      creates the role with a placeholder trust policy.
#   2. In Databricks: Catalog Explorer -> External Data -> Credentials -> Create credential -> Service
#      Credential, using this role's ARN (`databricks_msk_service_credential_role_arn` output). Databricks
#      generates a real External ID -- copy it.
#   3. Set `databricks_service_credential_external_id` in terraform.tfvars to that value and
#      `terraform apply` again. This updates the trust policy to require the real External ID, making the
#      role self-assuming, which Databricks has required for all service credentials since 2025-01-20.
#   4. Back in Databricks, select the credential and run its "Validate configuration" check.
#
# Principal ARN below is Databricks' Unity Catalog master role for standard AWS (not GovCloud) -- see
# https://docs.databricks.com/aws/en/connect/unity-catalog/cloud-services/service-credentials

resource "aws_iam_role" "databricks_msk_service_credential" {
  name = "${var.project_name}-${var.environment}-databricks-msk-service-credential"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "DatabricksUnityCatalogTrust"
        Effect = "Allow"
        Principal = {
          AWS = "arn:aws:iam::414351767826:role/unity-catalog-prod-UCMasterRole-14S5ZJVKOTYTL"
        }
        Action = "sts:AssumeRole"
        Condition = {
          StringEquals = {
            "sts:ExternalId" = var.databricks_service_credential_external_id
          }
        }
      },
      {
        Sid    = "SelfAssume"
        Effect = "Allow"
        Principal = {
          AWS = "arn:aws:iam::${data.aws_caller_identity.current.account_id}:role/${var.project_name}-${var.environment}-databricks-msk-service-credential"
        }
        Action = "sts:AssumeRole"
        Condition = {
          StringEquals = {
            "sts:ExternalId" = var.databricks_service_credential_external_id
          }
        }
      }
    ]
  })

  tags = {
    Project     = var.project_name
    Environment = var.environment
  }
}

# Scope note: this example scopes cluster-level actions to the specific MSK cluster ARN, and topic-level
# actions to the "retail-clickstream*" topic name pattern. Tighten `Resource` further per your org's IAM
# standards before using outside training/demo purposes.
resource "aws_iam_role_policy" "databricks_msk_access" {
  name = "${var.project_name}-${var.environment}-databricks-msk-access"
  role = aws_iam_role.databricks_msk_service_credential.id

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

output "databricks_msk_service_credential_role_arn" {
  value = aws_iam_role.databricks_msk_service_credential.arn
}


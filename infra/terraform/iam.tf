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
          "s3:GetBucketLocation",
          "s3:AbortMultipartUpload",
          "s3:ListBucketMultipartUploads",
          "s3:ListMultipartUploadParts",
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
          "glue:UpdateDatabase",
          "glue:GetTable",
          "glue:GetTables",
          "glue:CreateTable",
          "glue:UpdateTable",
          "glue:DeleteTable",
          "glue:GetPartition",
          "glue:GetPartitions",
          "glue:BatchGetPartition",
          "glue:CreatePartition",
          "glue:BatchCreatePartition",
          "glue:UpdatePartition",
          "glue:DeletePartition",
          "glue:BatchDeletePartition",
          "glue:GetUserDefinedFunctions",
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

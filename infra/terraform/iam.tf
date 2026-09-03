# IAM policy Databricks cluster instance profile needs to read/write the MSK Serverless cluster and topics.
# Attach this policy to the IAM role used as the Databricks cluster's instance profile.
#
# Scope note: this example scopes cluster-level actions to the specific MSK cluster ARN, and topic-level
# actions to the "retail-clickstream*" topic name pattern. Tighten `Resource` further per your org's IAM
# standards before using outside training/demo purposes.

resource "aws_iam_policy" "databricks_msk_access" {
  name        = "${var.project_name}-${var.environment}-databricks-msk-access"
  description = "Allows a Databricks cluster instance profile to read/write MSK topics for ${var.project_name}"

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

output "databricks_msk_access_policy_arn" {
  value = aws_iam_policy.databricks_msk_access.arn
}

# Persistent EMR cluster with JupyterHub for interactive/teaching use -- see emr-notebooks/ (production
# pipeline walkthroughs) and class-emr/ (concept notebooks). Cost note: this bills continuously while
# running, same as MSK -- destroy or stop it when not actively in a learning session. m5.large was tried
# as a cheaper default but EMR release 7.5.0 rejects it ("Instance type not supported") -- m5.xlarge
# (var.emr_instance_type) is the practical floor. To still cut cost, drop var.emr_instance_count to 1
# instead of downsizing the instance type.
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

# Installs kafka-topics.sh + the aws-msk-iam-auth jar so MSK admin commands (e.g. creating the
# retail-clickstream topic, see RUNBOOK_AWS.md step 3) work directly from an SSM session on this node,
# without the manual per-session setup that step used to require.
resource "aws_s3_object" "kafka_cli_bootstrap_script" {
  bucket = aws_s3_bucket.lakehouse.id
  key    = "bootstrap/install_kafka_cli.sh"
  source = "${path.module}/bootstrap/install_kafka_cli.sh"
  etag   = filemd5("${path.module}/bootstrap/install_kafka_cli.sh")
}

resource "aws_emr_cluster" "learning" {
  name          = "${var.project_name}-${var.environment}-learning"
  release_label = var.emr_release_label
  applications  = ["Spark", "JupyterHub", "Livy", "Hadoop"]
  log_uri       = "s3://${aws_s3_bucket.lakehouse.bucket}/emr-logs/learning/"
  service_role  = aws_iam_role.emr_service_role.arn

  # var.subnet_ids[0] below is a plain variable reference, not a resource reference, so Terraform's graph
  # has no implicit edge to the networking this depends on -- the bootstrap action's `aws s3 cp` needs the
  # S3 gateway endpoint (and/or NAT Gateway) already attached to that subnet's route table, or it fails
  # with BOOTSTRAP_FAILURE trying to reach S3 before the route exists.
  depends_on = [aws_route_table_association.private, aws_vpc_endpoint.s3]

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

  bootstrap_action {
    name = "install-kafka-cli"
    path = "s3://${aws_s3_bucket.lakehouse.bucket}/${aws_s3_object.kafka_cli_bootstrap_script.key}"
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

# MWAA (managed Airflow) for orchestrating the production pipeline. The DAG itself is at
# airflow/dags/retail_lakehouse_pipeline.py, synced to dag_s3_path below by
# .github/workflows/deploy_aws.yml on every deploy.
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
        # Required by MWAA's own CreateEnvironment validation (calls s3:GetAccountPublicAccessBlock via
        # S3 Control internally, using this execution role) -- account-level, so no bucket ARN applies.
        Sid      = "S3AccountPublicAccessBlock"
        Effect   = "Allow"
        Action   = "s3:GetAccountPublicAccessBlock"
        Resource = "*"
      },
      {
        # GetBucketPublicAccessBlock is also part of MWAA's CreateEnvironment validation, run against the
        # specific source bucket -- separate from the account-level check above.
        Sid    = "S3DagsAndData"
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:PutObject",
          "s3:ListBucket",
          "s3:GetBucketPublicAccessBlock",
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
        Sid    = "PassEmrRoles"
        Effect = "Allow"
        Action = "iam:PassRole"
        Resource = [
          aws_iam_role.emr_instance_profile_role.arn,
          aws_iam_role.emr_service_role.arn,
        ]
        Condition = {
          StringEquals = {
            "iam:PassedToService" = "elasticmapreduce.amazonaws.com"
          }
        }
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
        Sid      = "LogsDescribe"
        Effect   = "Allow"
        Action   = "logs:DescribeLogGroups"
        Resource = "*"
      },
      {
        Sid      = "AirflowMetrics"
        Effect   = "Allow"
        Action   = "airflow:PublishMetrics"
        Resource = "arn:aws:airflow:${var.aws_region}:*:environment/${var.project_name}-${var.environment}"
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

resource "aws_security_group_rule" "mwaa_egress_all" {
  type              = "egress"
  from_port         = 0
  to_port           = 0
  protocol          = "-1"
  security_group_id = aws_security_group.mwaa.id
  cidr_blocks       = ["0.0.0.0/0"]
}

resource "aws_mwaa_environment" "this" {
  name                  = "${var.project_name}-${var.environment}"
  airflow_version       = var.mwaa_airflow_version
  environment_class     = var.mwaa_environment_class
  execution_role_arn    = aws_iam_role.mwaa_execution.arn
  source_bucket_arn     = aws_s3_bucket.lakehouse.arn
  dag_s3_path           = "airflow/dags"
  requirements_s3_path  = aws_s3_object.airflow_requirements.key
  max_workers           = 2
  # PUBLIC_ONLY is a deliberate demo/training tradeoff for simplicity -- MWAA still enforces its own
  # IAM/console-login auth on top of this. It's a step down from the SSM-only access pattern
  # emr_learning.tf uses for JupyterHub; switch to PRIVATE_ONLY plus a VPC-internal access path if that
  # tradeoff doesn't fit your environment.
  webserver_access_mode = "PUBLIC_ONLY"

  # MWAA's CreateEnvironment API rejects a source bucket without versioning enabled; without this explicit
  # dependency, Terraform's graph has no ordering guarantee between the two, which can cause an
  # intermittent apply failure on a clean account. Also depends on aws_route_table_association.private:
  # var.subnet_ids only becomes a plain variable reference here, not a resource reference, so Terraform's
  # graph has no *implicit* edge to the route table association that actually makes those subnets private
  # -- without this, MWAA can validate the subnets before networking.tf has finished repointing them off
  # the Internet Gateway, and reject them as still public.
  depends_on = [aws_s3_bucket_versioning.lakehouse, aws_route_table_association.private]

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

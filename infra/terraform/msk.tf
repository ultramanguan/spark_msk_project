# Amazon MSK Serverless: capacity scales automatically, billed per partition-hour and GB in/out/retained.
# Simpler to provision for a training/demo project than MSK Provisioned (no broker instance sizing).
# IAM authentication is enforced -- see infra/terraform/iam.tf for the policy Databricks needs.

resource "aws_security_group" "msk_broker" {
  name_prefix = "${var.project_name}-msk-"
  description = "MSK Serverless broker access for ${var.project_name}"
  vpc_id      = var.vpc_id

  tags = {
    Project     = var.project_name
    Environment = var.environment
  }
}

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

resource "aws_security_group_rule" "msk_egress_all" {
  type              = "egress"
  from_port         = 0
  to_port           = 0
  protocol          = "-1"
  security_group_id = aws_security_group.msk_broker.id
  cidr_blocks       = ["0.0.0.0/0"]
}

resource "aws_msk_serverless_cluster" "this" {
  cluster_name = "${var.project_name}-${var.environment}"

  vpc_config {
    subnet_ids         = var.subnet_ids
    security_group_ids = [aws_security_group.msk_broker.id]
  }

  client_authentication {
    sasl {
      iam {
        enabled = true
      }
    }
  }

  tags = {
    Project     = var.project_name
    Environment = var.environment
  }
}

output "msk_cluster_arn" {
  value = aws_msk_serverless_cluster.this.arn
}

output "msk_bootstrap_brokers_command" {
  description = "Run this after apply to fetch the bootstrap broker string used by the Spark kafka.bootstrap.servers option"
  value       = "aws kafka get-bootstrap-brokers --cluster-arn ${aws_msk_serverless_cluster.this.arn} --region ${var.aws_region} --query bootstrapBrokerStringSaslIam --output text"
}

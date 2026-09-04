variable "aws_region" {
  description = "AWS region to deploy into"
  type        = string
  default     = "us-east-1"
}

variable "project_name" {
  description = "Prefix used for naming all resources"
  type        = string
  default     = "retail-lakehouse"
}

variable "environment" {
  description = "Environment name, e.g. dev/prod. Used only for tagging/naming; keep clusters small for training/demo use."
  type        = string
  default     = "dev"
}

variable "vpc_id" {
  description = "VPC to deploy MSK into. Must be a VPC reachable from your Databricks workspace (same VPC, peered, or PrivateLink)."
  type        = string
}

variable "subnet_ids" {
  description = "At least 2 private subnet IDs in different AZs within var.vpc_id for MSK brokers."
  type        = list(string)
}

variable "databricks_security_group_id" {
  description = "Security group ID attached to your Databricks cluster ENIs, used to allow MSK broker port ingress. Find it in the Databricks workspace's VPC (usually named like <workspace>-worker-unmanaged or a customer-managed VPC SG)."
  type        = string
}

variable "kafka_topics" {
  description = "Kafka topics to create for the pipeline"
  type = list(object({
    name               = string
    partitions         = number
    replication_factor = number
  }))
  default = [
    { name = "retail-clickstream", partitions = 6, replication_factor = 3 },
  ]
}

variable "msk_kafka_version" {
  description = "MSK Serverless supports a fixed Kafka API version managed by AWS; this is informational for Provisioned mode if you switch msk.tf to that instead."
  type        = string
  default     = "3.5.1"
}

variable "databricks_service_credential_external_id" {
  description = "External ID for the Unity Catalog service credential's self-assuming trust policy. Leave as the placeholder \"0000\" for the first `terraform apply`; after registering the service credential in Databricks (see infra/terraform/iam.tf), set this to the real External ID Databricks generates and apply again."
  type        = string
  default     = "0000"
}

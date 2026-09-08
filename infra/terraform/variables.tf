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
  description = "Exactly 2 subnet IDs in different AZs within var.vpc_id, used by MSK and EMR always, and by MWAA if var.enable_mwaa is true. Only converted to private (via a dedicated route table through the NAT Gateway in var.nat_gateway_subnet_id) when var.enable_mwaa is true -- MSK/EMR don't require private subnets, only MWAA does. With var.enable_mwaa = false (the default), these are left exactly as they are, public or private."
  type        = list(string)
}

variable "nat_gateway_subnet_id" {
  description = "An existing PUBLIC subnet (route to an Internet Gateway) in var.vpc_id, distinct from var.subnet_ids, to host the shared NAT Gateway that makes var.subnet_ids private. Only needed if var.enable_mwaa is true -- leave unset otherwise. A NAT Gateway must live in a public subnet -- it can't host itself in the private subnets it serves."
  type        = string
  default     = null
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

variable "emr_release_label" {
  description = "EMR release label for both the persistent learning cluster and the ephemeral production clusters MWAA creates"
  type        = string
  default     = "emr-7.5.0"
}

variable "emr_instance_type" {
  description = "EC2 instance type for EMR master/core nodes. m5.large is rejected by EMR release 7.5.0 (\"Instance type not supported\") -- m5.xlarge is the practical floor for this release/application combo. Keep small for training/demo use."
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

variable "enable_mwaa" {
  description = "Whether to provision MWAA (managed Airflow) and its supporting IAM role/security group at all. Defaults to false: for study/interactive use (the EMR learning cluster + JupyterHub), MWAA is unnecessary extra cost and complexity -- it only matters if you're running the *scheduled production pipeline* (see RUNBOOK_AWS.md). Set to true to provision it."
  type        = bool
  default     = false
}

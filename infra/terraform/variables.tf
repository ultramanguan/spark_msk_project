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
  description = "EC2 instance type for EMR master/core nodes. Keep small for training/demo use."
  type        = string
  default     = "m5.large"
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

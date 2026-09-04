output "next_steps" {
  value = <<-EOT
    1. Fetch bootstrap brokers:
       terraform output -raw msk_bootstrap_brokers_command | bash

    2. Create the Kafka topic(s) using the Kafka admin CLI (Terraform's AWS provider has no native MSK
       topic resource). From a host with network access to the cluster (e.g. an EC2 instance in the same
       VPC, or Databricks itself via a one-off notebook cell):

       kafka-topics.sh --bootstrap-server <bootstrap-brokers> \
         --command-config client.properties \
         --create --topic retail-clickstream --partitions 6 --replication-factor 3

       See ../../scripts/create_msk_topics.sh for a wrapper and a sample client.properties for IAM auth.

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
  EOT
}


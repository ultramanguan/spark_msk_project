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

    3. Attach the IAM policy at `databricks_msk_access_policy_arn` to your Databricks cluster's instance
       profile role, and set that instance profile on the cluster running the pipeline notebooks.

    4. Set the kafka_bootstrap_servers widget in notebooks/02_kafka_msk_streaming_ingest.py to the value
       from step 1.
  EOT
}

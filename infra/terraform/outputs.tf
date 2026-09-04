output "next_steps" {
  value = <<-EOT
    1. Fetch bootstrap brokers:
       terraform output -raw msk_bootstrap_brokers_command | bash

    2. Create the Kafka topic(s) using the Kafka admin CLI (Terraform's AWS provider has no native MSK
       topic resource). From a host with network access to the cluster (e.g. an EC2 instance in the same
       VPC, or EMR itself via a Jupyter %%bash cell):

       kafka-topics.sh --bootstrap-server <bootstrap-brokers> \
         --command-config client.properties \
         --create --topic retail-clickstream --partitions 6 --replication-factor 3

       See ../../scripts/create_msk_topics.sh for a wrapper and a sample client.properties for IAM auth.

    3. No credential registration step needed -- EMR clusters built from this Terraform (the persistent
       learning cluster in emr_learning.tf, and the ephemeral production clusters MWAA creates) already
       have MSK access via the EC2 instance profile in iam.tf.

    4. Interactive learning notebooks (emr-notebooks/) take the bootstrap-brokers string from step 1
       as a plain notebook variable -- no widget/credential-name setup required.
  EOT
}


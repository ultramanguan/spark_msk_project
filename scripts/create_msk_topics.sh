#!/usr/bin/env bash
# Creates the Kafka topics required by the pipeline against a live MSK Serverless cluster using
# IAM authentication. Run from any host with network access to the MSK cluster (a Databricks
# notebook %sh cell, an EC2 instance in the same VPC, or your laptop if peered/VPN'd in).
#
# Requires: Kafka CLI tools (kafka-topics.sh, from a Kafka/Confluent distribution) and the
# aws-msk-iam-auth jar on the classpath. See:
# https://github.com/aws/aws-msk-iam-auth#configuration
set -euo pipefail

BOOTSTRAP_BROKERS="${1:?Usage: create_msk_topics.sh <bootstrap-brokers> [topic] [partitions] [replication-factor]}"
TOPIC="${2:-retail-clickstream}"
PARTITIONS="${3:-6}"
REPLICATION_FACTOR="${4:-3}"

CLIENT_PROPS="$(mktemp)"
trap 'rm -f "$CLIENT_PROPS"' EXIT
cat > "$CLIENT_PROPS" <<EOF
security.protocol=SASL_SSL
sasl.mechanism=AWS_MSK_IAM
sasl.jaas.config=software.amazon.msk.auth.iam.IAMLoginModule required;
sasl.client.callback.handler.class=software.amazon.msk.auth.iam.IAMClientCallbackHandler
EOF

echo "Creating topic '$TOPIC' ($PARTITIONS partitions, replication factor $REPLICATION_FACTOR) on $BOOTSTRAP_BROKERS"
kafka-topics.sh --bootstrap-server "$BOOTSTRAP_BROKERS" \
  --command-config "$CLIENT_PROPS" \
  --create --if-not-exists \
  --topic "$TOPIC" \
  --partitions "$PARTITIONS" \
  --replication-factor "$REPLICATION_FACTOR"

kafka-topics.sh --bootstrap-server "$BOOTSTRAP_BROKERS" --command-config "$CLIENT_PROPS" --describe --topic "$TOPIC"

#!/usr/bin/env bash
# EMR bootstrap action: installs the Apache Kafka CLI tools (kafka-topics.sh etc.) plus the aws-msk-iam-auth
# jar, so admin commands against the MSK Serverless cluster (e.g. creating topics) work directly from an
# `aws ssm start-session` on this node without a manual one-off setup each time -- see RUNBOOK_AWS.md step 3.
set -euo pipefail

KAFKA_VERSION="3.5.1"   # matches var.msk_kafka_version
INSTALL_DIR="/opt/kafka"

sudo mkdir -p "$INSTALL_DIR"
curl -fsSL "https://archive.apache.org/dist/kafka/${KAFKA_VERSION}/kafka_2.13-${KAFKA_VERSION}.tgz" \
  | sudo tar -xz -C "$INSTALL_DIR" --strip-components=1

# Dropped into libs/, this jar is picked up automatically by kafka-run-class.sh's own classpath globbing --
# no CLASSPATH export needed (and one wouldn't survive past this script anyway).
sudo curl -fsSL -o "$INSTALL_DIR/libs/aws-msk-iam-auth-all.jar" \
  "https://github.com/aws/aws-msk-iam-auth/releases/latest/download/aws-msk-iam-auth-all.jar"

# Symlinking into /usr/local/bin (always on PATH) is what actually makes these commands available in later
# interactive sessions -- exporting PATH here would only apply to this bootstrap script's own shell.
sudo ln -sf "$INSTALL_DIR"/bin/kafka-*.sh /usr/local/bin/

# Static IAM-auth config for kafka-topics.sh/kafka-console-*.sh --command-config -- content never changes,
# so it's simplest to just write it once here rather than regenerate it in every session.
sudo tee "$INSTALL_DIR/client.properties" > /dev/null <<'EOF'
security.protocol=SASL_SSL
sasl.mechanism=AWS_MSK_IAM
sasl.jaas.config=software.amazon.msk.auth.iam.IAMLoginModule required;
sasl.client.callback.handler.class=software.amazon.msk.auth.iam.IAMClientCallbackHandler
EOF

#!/usr/bin/env bash
# EMR bootstrap action: installs the retail_lakehouse wheel onto every node so it's importable from
# Jupyter notebooks (persistent learning cluster) and spark-submit jobs (ephemeral production clusters)
# without any per-notebook/per-step pip-install step.
set -euo pipefail

WHEEL_S3_URI="${1:?Usage: install_retail_lakehouse.sh <s3://bucket/artifacts/retail_lakehouse-latest.whl>}"
# pip requires a PEP 427-shaped filename (name-version-pythontag-abitag-platformtag.whl) to even parse the
# file -- the "-latest" alias name in S3 (2 segments) fails that check before pip ever reads the wheel's
# real metadata, so the local copy needs a compliant name regardless of what the S3 object is called.
LOCAL_WHEEL="/tmp/retail_lakehouse-0.0.0-py3-none-any.whl"

aws s3 cp "$WHEEL_S3_URI" "$LOCAL_WHEEL"
# [kafka] extra pulls in kafka-python + aws-msk-iam-sasl-signer-python, so
# retail_lakehouse.kafka_admin.create_topics(...) works out of the box on every node -- an importable
# alternative to shelling out to kafka-topics.sh for topic administration.
sudo pip3 install --upgrade "${LOCAL_WHEEL}[kafka]"

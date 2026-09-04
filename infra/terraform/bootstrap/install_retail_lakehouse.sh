#!/usr/bin/env bash
# EMR bootstrap action: installs the retail_lakehouse wheel onto every node so it's importable from
# Jupyter notebooks (persistent learning cluster) and spark-submit jobs (ephemeral production clusters)
# without any per-notebook/per-step pip-install step.
set -euo pipefail

WHEEL_S3_URI="${1:?Usage: install_retail_lakehouse.sh <s3://bucket/artifacts/retail_lakehouse-latest.whl>}"
LOCAL_WHEEL="/tmp/retail_lakehouse-latest.whl"

aws s3 cp "$WHEEL_S3_URI" "$LOCAL_WHEEL"
sudo pip3 install --upgrade "$LOCAL_WHEEL"

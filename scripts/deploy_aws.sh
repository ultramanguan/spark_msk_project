#!/usr/bin/env bash
set -euo pipefail
BUCKET="${1:?Usage: deploy_aws.sh <lakehouse-bucket-name>}"

rm -rf dist
python -m build
aws s3 cp dist/retail_lakehouse-*.whl "s3://$BUCKET/artifacts/retail_lakehouse-latest.whl"
aws s3 sync emr_jobs/ "s3://$BUCKET/emr_jobs/" --delete
aws s3 sync airflow/dags/ "s3://$BUCKET/airflow/dags/" --delete

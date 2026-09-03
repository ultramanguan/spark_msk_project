#!/usr/bin/env bash
set -euo pipefail
TARGET="${1:-dev}"
python -m build
databricks bundle validate -t "$TARGET"
databricks bundle deploy -t "$TARGET"
databricks bundle run retail_lakehouse_pipeline -t "$TARGET"

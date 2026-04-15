#!/bin/bash

# Export notebooks from Databricks workspace to local src/notebooks directory
# Prerequisites: Databricks CLI installed and authenticated

set -e

echo "Starting notebook export..."

# Create src/notebooks directory if it doesn't exist
mkdir -p src/notebooks

# Define workspace paths
WORKSPACE_BASE="/Users/illia.volchetskyi@innowise.com/turbine_project_pipelines/turbine_project"

# Export notebooks
echo "Exporting total_cleanup..."
databricks workspace export "${WORKSPACE_BASE}/monitoring/total_cleanup" \
  src/notebooks/total_cleanup.py --format SOURCE

echo "Exporting bronze_ingestion..."
databricks workspace export "${WORKSPACE_BASE}/bronze/bronze_ingestion" \
  src/notebooks/bronze_ingestion.py --format SOURCE

echo "Exporting file_arrival_simulator..."
databricks workspace export "${WORKSPACE_BASE}/bronze/file_arrival_simulator" \
  src/notebooks/file_arrival_simulator.py --format SOURCE

echo "Exporting silver_data_transformation..."
databricks workspace export "${WORKSPACE_BASE}/silver/silver_data_transformation" \
  src/notebooks/silver_data_transformation.py --format SOURCE

echo "Exporting gold_layer_analytics..."
databricks workspace export "${WORKSPACE_BASE}/gold/gold_layer_analytics" \
  src/notebooks/gold_layer_analytics.py --format SOURCE

echo ""
echo "✅ All notebooks exported successfully!"
echo ""
echo "Exported files:"
ls -lh src/notebooks/

echo ""
echo "Next steps:"
echo "1. Review the exported notebooks in src/notebooks/"
echo "2. Run: databricks bundle validate -t dev"
echo "3. Run: databricks bundle deploy -t dev"

# Notebooks Directory

This directory contains the source code for all notebooks used in the Turbine Project medallion architecture.

## Required Notebooks

### Monitoring
- `total_cleanup.py` - Cleanup script that runs first in bronze job

### Bronze Layer
- `bronze_ingestion.py` - Ingests raw turbine data
- `file_arrival_simulator.py` - Simulates file arrivals for testing

### Silver Layer
- `silver_data_transformation.py` - Cleanses and transforms bronze data

### Gold Layer
- `gold_layer_analytics.py` - Creates analytics aggregations

## Exporting Notebooks

### Option 1: Use the export script (Recommended)

From the project root directory:
```bash
chmod +x export_notebooks.sh
./export_notebooks.sh
```

### Option 2: Manual export

Export each notebook individually:
```bash
# Example:
databricks workspace export /Users/illia.volchetskyi@innowise.com/turbine_project_pipelines/turbine_project/monitoring/total_cleanup src/notebooks/total_cleanup.py --format SOURCE
```

## File Format

Notebooks should be exported as:
- **Python source** (`.py`) - Recommended for version control
- **Jupyter format** (`.ipynb`) - Alternative if you need to preserve outputs

The DAB job definitions reference these files using relative paths:
```yaml
notebook_path: ../src/notebooks/total_cleanup.py
```

## Version Control

These files should be:
✅ Committed to Git
✅ Reviewed in pull requests
✅ Deployed via CI/CD

## Notes

- Keep notebook code production-ready
- Avoid hardcoded paths when possible (use variables)
- Include error handling and logging
- Test notebooks locally before committing

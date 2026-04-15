# Turbine Project - Databricks Asset Bundle (DAB)

This project implements a Medallion Architecture (Bronze, Silver, Gold) using Databricks Asset Bundles for deployment automation.

## Project Structure

```
turbine_dab_project/
├── databricks.yml              # Main DAB configuration
├── README.md                   # This file
├── .github/
│   └── workflows/
│       └── deploy.yml          # CI/CD pipeline
├── src/
│   └── notebooks/              # Notebook source files (.py or .ipynb)
│       ├── total_cleanup.py
│       ├── bronze_ingestion.py
│       ├── file_arrival_simulator.py
│       ├── silver_data_transformation.py
│       └── gold_layer_analytics.py
└── resources/
    ├── bronze_job.yml          # Bronze layer job definition
    ├── silver_job.yml          # Silver layer job definition
    └── gold_job.yml            # Gold layer job definition
```

## Architecture

### Bronze Layer (tp_bronze)
- **Task 1**: `CLEANUP` - Runs total_cleanup notebook first
- **Task 2**: `bronze_layer_ingestion` - Ingests raw data (depends on CLEANUP)
- **Task 3**: `start_file_streaming` - Simulates file arrivals (depends on CLEANUP)

### Silver Layer (tp_silver)
- **Task**: `silver_pipeline` - Transforms and cleanses bronze data

### Gold Layer (tp_gold)
- **Task**: `gold_pipeline` - Creates analytics aggregations

## Prerequisites

1. **Databricks CLI** installed locally
   ```bash
   curl -fsSL https://raw.githubusercontent.com/databricks/setup-cli/main/install.sh | sh
   ```

2. **Databricks authentication**
   - Host: Your Databricks workspace URL
   - Token: Personal access token

3. **Git repository** with this code

## Local Development

### 1. Export Notebooks to src/notebooks/

Export your workspace notebooks as Python files:
```bash
# From your current workspace location, export each notebook
databricks workspace export /Users/illia.volchetskyi@innowise.com/turbine_project_pipelines/turbine_project/monitoring/total_cleanup src/notebooks/total_cleanup.py --format SOURCE
databricks workspace export /Users/illia.volchetskyi@innowise.com/turbine_project_pipelines/turbine_project/bronze/bronze_ingestion src/notebooks/bronze_ingestion.py --format SOURCE
databricks workspace export /Users/illia.volchetskyi@innowise.com/turbine_project_pipelines/turbine_project/bronze/file_arrival_simulator src/notebooks/file_arrival_simulator.py --format SOURCE
databricks workspace export /Users/illia.volchetskyi@innowise.com/turbine_project_pipelines/turbine_project/silver/silver_data_transformation src/notebooks/silver_data_transformation.py --format SOURCE
databricks workspace export /Users/illia.volchetskyi@innowise.com/turbine_project_pipelines/turbine_project/gold/gold_layer_analytics src/notebooks/gold_layer_analytics.py --format SOURCE
```

### 2. Validate Bundle

```bash
cd turbine_dab_project
databricks bundle validate -t dev
```

### 3. Deploy to Dev Environment

```bash
databricks bundle deploy -t dev
```

This will create:
- `tp_bronze_dev` job
- `tp_silver_dev` job
- `tp_gold_dev` job

### 4. Run Jobs

```bash
# Run bronze job (includes cleanup)
databricks bundle run tp_bronze -t dev

# Run silver job
databricks bundle run tp_silver -t dev

# Run gold job
databricks bundle run tp_gold -t dev
```

## CI/CD with GitHub Actions

### Setup GitHub Secrets

1. Go to your GitHub repository
2. Navigate to **Settings** → **Secrets and variables** → **Actions**
3. Add the following secrets:
   - `DATABRICKS_HOST`: Your Databricks workspace URL (e.g., `https://adb-123456789.azuredatabricks.net`)
   - `DATABRICKS_TOKEN`: Your personal access token

### Workflow Trigger

The deployment workflow runs automatically on:
- Push to `main` branch
- Manual trigger via workflow_dispatch

### Workflow Steps

1. **Checkout code** - Gets the latest code
2. **Setup Python** - Installs Python 3.11
3. **Install Databricks CLI** - Downloads and installs CLI
4. **Validate bundle** - Runs validation checks
5. **Deploy bundle** - Deploys to dev environment
6. **Show summary** - Displays deployment results

## Configuration Details

### databricks.yml

- **Bundle name**: `turbine_project_dab`
- **Target**: `dev` (can add `staging`, `prod` later)
- **Variables**: Catalog and schema names
- **Workspace root**: `.bundle/turbine_project_dab/dev`

### Job Definitions

Each job YAML file in `resources/` defines:
- Job name with `${bundle.target}` suffix
- Tasks with notebook paths
- Cluster configuration (Photon-enabled, single-user)
- Task dependencies (for bronze job)

## Next Steps

1. **Export notebooks** to `src/notebooks/` directory
2. **Commit and push** to GitHub
3. **Verify GitHub Actions** deployment
4. **Add schedules** to job YAMLs if needed:
   ```yaml
   schedule:
     quartz_cron_expression: "0 0 * * * ?"
     timezone_id: "America/New_York"
   ```

## Troubleshooting

### Validation fails
- Check YAML syntax
- Verify notebook paths match actual files
- Ensure all required fields are present

### Deployment fails
- Verify authentication (host and token)
- Check workspace permissions
- Review bundle validation output

### Job execution fails
- Check notebook code for errors
- Verify table and catalog names
- Review cluster configuration

## Additional Resources

- [Databricks Asset Bundles Documentation](https://docs.databricks.com/dev-tools/bundles/)
- [GitHub Actions Documentation](https://docs.github.com/en/actions)
- [Databricks CLI Reference](https://docs.databricks.com/dev-tools/cli/)

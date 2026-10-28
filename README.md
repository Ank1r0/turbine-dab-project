# Turbine Project - Databricks Asset Bundle (DAB)

A medallion pipeline (Bronze, Silver, Gold) on Databricks for simulated wind-turbine telemetry from 50 devices. The whole workspace is defined as a Databricks Asset Bundle and deployed by GitHub Actions.

## Project structure

```
.
├── databricks.yml                  # Bundle config: variables, dev target
├── .github/workflows/deploy.yml    # CI/CD: syntax check, validate, deploy
├── resources/
│   ├── bronze_job.yml              # tp_bronze:     CLEANUP -> bronze ingestion + file simulator
│   ├── silver_job.yml              # tp_silver:     silver transformation
│   ├── gold_job.yml                # tp_gold:       gold aggregations
│   ├── monitoring_job.yml          # tp_monitoring: reconciliation check
│   └── pipeline_job.yml            # tp_pipeline:   all of the above chained end to end
└── src/notebooks/
    ├── total_cleanup.py            # guarded full reset
    ├── file_arrival_simulator.py   # copies source files into the landing volume at random intervals
    ├── bronze_ingestion.py         # Auto Loader -> bronze Delta table
    ├── silver_data_transformation.py
    ├── gold_layer_analytics.py
    └── monitoring_pipeline.py      # bronze -> silver reconciliation, rejected readings, gold status
```

## Architecture

| Layer | Table(s) | What happens |
|---|---|---|
| Bronze | `bronze.turbine_events_raw` | Auto Loader reads raw JSON files as they arrive; no transformation. Each row holds an array of metric readings as a JSON string. |
| Silver | `silver.turbine_events_clean`, `silver.turbine_events_rejected` | The JSON payload is parsed into one row per reading. Every reading is validated; valid readings are pivoted into one wide, typed row per (device, event time) and appended unless that event is already in silver. Invalid readings go to the rejected table with a `reject_reason`. |
| Gold | `gold.turbine_metrics_{avg,min,max,stddev}` | One-minute windows per device and numeric metric. Affected windows are recomputed from the full silver table and MERGEd. |

### Silver validation rules

A reading is rejected, and written to `turbine_events_rejected`, when it has: no device id (`missing_device_id`), no metric name (`missing_metric`), no timestamp (`missing_timestamp`), a timestamp before 2000-01-01 or more than a day in the future (`malformed_timestamp`), no value (`missing_value`), a metric outside the known list (`unknown_metric`), or a value that cannot be parsed as the metric's type (`invalid_value`). A payload that cannot be parsed at all becomes one all-NULL reading and is rejected too, so nothing is dropped silently.

Assumption: all readings of one (device, timestamp) event arrive in the same micro-batch. A later batch carrying an event already in silver is ignored; silver is append-only so that gold can stream from it.

### Monitoring

`monitoring_pipeline.py` compares events, not raw row counts, because one bronze row (a file) holds many readings. It re-derives the valid (device, event time) set from bronze with the same rules as silver and checks that none is missing in silver and that silver has no duplicate keys. It also reports rejected readings per reason, data freshness and the gold tables. In `snapshot` mode (the job) a failed check fails the task. In `loop` mode it prints a report every `interval_seconds`.

## Parameters and bundle variables

Catalog and schema names come from bundle variables (`catalog`, `schema_bronze`, `schema_silver`, `schema_gold`) and are passed to every notebook as job parameters; the notebooks read them with widgets, so there are no hardcoded table or volume names. The `reset_on_run` variable (default `"false"`) controls the destructive `CLEANUP` task.

## Prerequisites

1. Databricks CLI installed locally
   ```bash
   curl -fsSL https://raw.githubusercontent.com/databricks/setup-cli/main/install.sh | sh
   ```
2. Databricks authentication (workspace URL and a personal access token)
3. In Unity Catalog: catalog `turbine_project`, schemas `bronze`, `silver`, `gold`, `autoloader_metadata`, a `landing_volume` in `bronze`, a `checkpoint_volume` and a `source_volume` (with the source JSON files under `turbine_events/turbine_events`) in `autoloader_metadata`.

## Local development

```bash
databricks bundle validate -t dev
databricks bundle deploy -t dev

# Full run, chained: cleanup (skipped unless reset_on_run=true) -> bronze + simulator -> silver -> gold -> reconciliation
databricks bundle run tp_pipeline -t dev

# Full restart from scratch (drops all tables, checkpoints and landing files)
databricks bundle run tp_pipeline -t dev --var="reset_on_run=true"

# Or run the layers separately
databricks bundle run tp_bronze -t dev
databricks bundle run tp_silver -t dev
databricks bundle run tp_gold -t dev
databricks bundle run tp_monitoring -t dev
```

All notebooks run on serverless compute (environment client `2`). Because serverless does not support `processingTime` triggers, each streaming notebook runs its stream with `availableNow=True` in a loop and stops after `idle_limit` consecutive iterations that read no new rows, so every job finishes on its own.

## CI/CD with GitHub Actions

Runs on every push to `main` and on manual trigger. Add the repository secrets `DATABRICKS_HOST` and `DATABRICKS_TOKEN` first.

1. Checkout and set up Python 3.11
2. Syntax-check every notebook (`python -m py_compile`)
3. Install the Databricks CLI
4. `databricks bundle validate -t dev`
5. `databricks bundle deploy -t dev`

CI deploys and validates the bundle but does not run the pipeline or any data tests.

## Known limitations

- The notebooks need a Databricks workspace to run; they were not executed outside it.
- Late readings of an event that already exists in silver are ignored (see the silver assumption above).
- Only a `dev` target exists.
- No scheduling or alerting is configured; the reconciliation task fails the job run, and notifications are left to the workspace.

# Notebooks

Source of every notebook deployed by the bundle (Databricks "notebook source" Python files).

| File | Layer | Purpose |
|---|---|---|
| `total_cleanup.py` | reset | Drops all pipeline tables, checkpoints and landing files. Only runs when `confirm_reset` is `true`. |
| `file_arrival_simulator.py` | bronze | Copies source JSON files into the landing volume at random 1-5 s intervals; ends when all files are delivered. |
| `bronze_ingestion.py` | bronze | Auto Loader into `turbine_events_raw`; ends after `idle_limit` idle iterations. |
| `silver_data_transformation.py` | silver | Parse, validate, pivot, deduplicate; invalid readings go to `turbine_events_rejected`. |
| `gold_layer_analytics.py` | gold | One-minute avg/min/max/stddev per device and metric; affected windows are recomputed from silver. |
| `monitoring_pipeline.py` | monitoring | Reconciles valid bronze events with silver, reports rejects and gold status. |

Every notebook takes its catalog and schema names from widgets, which the job definitions fill from the bundle variables in `databricks.yml`. The validity rules in `silver_data_transformation.py` and `monitoring_pipeline.py` are duplicated on purpose; change them in both.

`export_notebooks.sh` in the repo root exports notebooks from the original workspace; it overwrites these files with the workspace copies, so do not run it after editing here.

# Databricks notebook source
# MAGIC %md
# MAGIC # Total pipeline cleanup
# MAGIC **Destructive:** drops every pipeline table and deletes every checkpoint and the landing files, so the pipeline can restart from scratch.
# MAGIC It only runs when the `confirm_reset` parameter is `true` (bundle variable `reset_on_run`, default `false`). Otherwise it exits without touching anything.

# COMMAND ----------

# DBTITLE 1,Parameters and guard
import shutil
from pathlib import Path

dbutils.widgets.text("catalog", "turbine_project")
dbutils.widgets.text("schema_bronze", "bronze")
dbutils.widgets.text("schema_silver", "silver")
dbutils.widgets.text("schema_gold", "gold")
dbutils.widgets.text("confirm_reset", "false")

CATALOG = dbutils.widgets.get("catalog")
SCHEMA_BRONZE = dbutils.widgets.get("schema_bronze")
SCHEMA_SILVER = dbutils.widgets.get("schema_silver")
SCHEMA_GOLD = dbutils.widgets.get("schema_gold")

if dbutils.widgets.get("confirm_reset").strip().lower() != "true":
    dbutils.notebook.exit("Reset skipped: confirm_reset is not 'true'. Nothing was changed.")

CHECKPOINT_BASE = Path(f"/Volumes/{CATALOG}/autoloader_metadata/checkpoint_volume")
LANDING_PATH = Path(f"/Volumes/{CATALOG}/{SCHEMA_BRONZE}/landing_volume")

print("=" * 60)
print("TOTAL PIPELINE CLEANUP")
print("=" * 60)

# COMMAND ----------

# DBTITLE 1,Drop tables
tables = [
    f"{CATALOG}.{SCHEMA_BRONZE}.turbine_events_raw",
    f"{CATALOG}.{SCHEMA_SILVER}.turbine_events_clean",
    f"{CATALOG}.{SCHEMA_SILVER}.turbine_events_rejected",
    f"{CATALOG}.{SCHEMA_GOLD}.turbine_metrics_avg",
    f"{CATALOG}.{SCHEMA_GOLD}.turbine_metrics_min",
    f"{CATALOG}.{SCHEMA_GOLD}.turbine_metrics_max",
    f"{CATALOG}.{SCHEMA_GOLD}.turbine_metrics_stddev",
]
for table in tables:
    try:
        spark.sql(f"DROP TABLE IF EXISTS {table}")
        print(f"dropped {table}")
    except Exception as e:
        print(f"Error dropping {table}: {e}")

# COMMAND ----------

# DBTITLE 1,Clear landing volume
if LANDING_PATH.exists():
    json_files = list(LANDING_PATH.glob("*.json"))
    for f in json_files:
        f.unlink()
    print(f"Deleted {len(json_files)} files from landing_volume")
else:
    print("Landing volume not found")

# COMMAND ----------

# DBTITLE 1,Delete checkpoints and schema metadata
for name in ["bronze_checkpoint", "silver_checkpoint", "gold_checkpoint", "schema"]:
    path = CHECKPOINT_BASE / name
    if path.exists():
        shutil.rmtree(path)
        print(f"deleted {path}")
    else:
        print(f"{name} not found")

# Anything else in the checkpoint volume is an orphan
if CHECKPOINT_BASE.exists():
    known = {"bronze_checkpoint", "silver_checkpoint", "gold_checkpoint", "schema"}
    orphaned = [item for item in CHECKPOINT_BASE.iterdir() if item.name not in known]
    for item in orphaned:
        shutil.rmtree(item) if item.is_dir() else item.unlink()
    print(f"Cleaned {len(orphaned)} orphaned items")
else:
    print("Checkpoint base directory not found")

# COMMAND ----------

print("=" * 60)
print("CLEANUP COMPLETE - source volume unchanged, ready to run bronze, silver and gold from scratch.")
print("=" * 60)

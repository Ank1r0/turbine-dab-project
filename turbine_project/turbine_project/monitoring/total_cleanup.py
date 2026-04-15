# Databricks notebook source
# DBTITLE 1,🧹 Total Pipeline Cleanup
import shutil
from pathlib import Path

print("="*60)
print("🧹 TOTAL PIPELINE CLEANUP")
print("="*60)
print("\nThis will reset everything to a clean state:\n")

# 1. Drop Bronze Table
print("[1/9] Dropping Bronze table...")
try:
    spark.sql("DROP TABLE IF EXISTS turbine_project.bronze.turbine_events_raw")
    print("    ✅ Bronze table dropped")
except Exception as e:
    print(f"    ⚠️ Error: {e}")

# 2. Drop Silver Table
print("\n[2/9] Dropping Silver table...")
try:
    spark.sql("DROP TABLE IF EXISTS turbine_project.silver.turbine_events_clean")
    print("    ✅ Silver table dropped")
except Exception as e:
    print(f"    ⚠️ Error: {e}")

# 3. Drop Gold Tables (4 tables)
print("\n[3/9] Dropping Gold tables...")
gold_tables = [
    "turbine_project.gold.turbine_metrics_avg",
    "turbine_project.gold.turbine_metrics_min",
    "turbine_project.gold.turbine_metrics_max",
    "turbine_project.gold.turbine_metrics_stddev"
]
dropped_count = 0
for table in gold_tables:
    try:
        spark.sql(f"DROP TABLE IF EXISTS {table}")
        dropped_count += 1
    except Exception as e:
        print(f"    ⚠️ Error dropping {table}: {e}")
print(f"    ✅ {dropped_count}/4 Gold tables dropped")

# 4. Clear Landing Volume (files copied by simulator)
print("\n[4/9] Clearing landing volume...")
landing_path = Path("/Volumes/turbine_project/bronze/landing_volume")
if landing_path.exists():
    json_files = list(landing_path.glob("*.json"))
    for f in json_files:
        f.unlink()
    print(f"    ✅ Deleted {len(json_files)} files from landing_volume")
else:
    print("    ⚠️ Landing volume not found")

# 5. Delete Bronze Checkpoint
print("\n[5/9] Deleting Bronze checkpoint...")
bronze_checkpoint = Path("/Volumes/turbine_project/autoloader_metadata/checkpoint_volume/bronze_checkpoint")
if bronze_checkpoint.exists():
    shutil.rmtree(bronze_checkpoint)
    print("    ✅ Bronze checkpoint deleted")
else:
    print("    ⚠️ Bronze checkpoint not found")

# 6. Delete Silver Checkpoint
print("\n[6/9] Deleting Silver checkpoint...")
silver_checkpoint = Path("/Volumes/turbine_project/autoloader_metadata/checkpoint_volume/silver_checkpoint")
if silver_checkpoint.exists():
    shutil.rmtree(silver_checkpoint)
    print("    ✅ Silver checkpoint deleted")
else:
    print("    ⚠️ Silver checkpoint not found")

# 7. Delete Gold Checkpoint
print("\n[7/9] Deleting Gold checkpoint...")
gold_checkpoint = Path("/Volumes/turbine_project/autoloader_metadata/checkpoint_volume/gold_checkpoint")
if gold_checkpoint.exists():
    shutil.rmtree(gold_checkpoint)
    print("    ✅ Gold checkpoint deleted")
else:
    print("    ⚠️ Gold checkpoint not found")

# 8. Delete Schema Metadata
print("\n[8/9] Deleting schema metadata...")
schema_path = Path("/Volumes/turbine_project/autoloader_metadata/checkpoint_volume/schema")
if schema_path.exists():
    shutil.rmtree(schema_path)
    print("    ✅ Schema metadata deleted")
else:
    print("    ⚠️ Schema metadata not found")

# 9. Clear any orphaned checkpoint files
print("\n[9/9] Checking for orphaned checkpoints...")
checkpoint_base = Path("/Volumes/turbine_project/autoloader_metadata/checkpoint_volume")
if checkpoint_base.exists():
    all_items = list(checkpoint_base.iterdir())
    orphaned = [item for item in all_items if item.name not in ["bronze_checkpoint", "silver_checkpoint", "gold_checkpoint", "schema"]]
    if orphaned:
        for item in orphaned:
            if item.is_dir():
                shutil.rmtree(item)
            else:
                item.unlink()
        print(f"    ✅ Cleaned {len(orphaned)} orphaned items")
    else:
        print("    ✅ No orphaned checkpoints found")
else:
    print("    ⚠️ Checkpoint base directory not found")

# Summary
print("\n" + "="*60)
print("🎉 CLEANUP COMPLETE!")
print("="*60)
print("\n✅ All tables dropped (Bronze + Silver + 4 Gold)")
print("✅ All checkpoints cleared (Bronze + Silver + Gold)")
print("✅ Landing volume emptied")
print("\n📦 Source volume unchanged (1000 files ready)")
print("\n🚀 Ready to run the full pipeline from scratch!")
print("\nNext steps:")
print("  1. Run file_arrival_simulator")
print("  2. Run bronze_ingestion")
print("  3. Run silver_data_transformation")
print("  4. Run gold_layer_analytics")
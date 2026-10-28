# Databricks notebook source
# MAGIC %md
# MAGIC # File arrival simulator
# MAGIC Copies the source JSON files one by one into the landing volume at random intervals, to imitate a stream.
# MAGIC Each file is copied to a temporary name and then renamed, so Auto Loader never reads a half-written file.
# MAGIC The notebook ends once every source file has been delivered.

# COMMAND ----------

# DBTITLE 1,Configuration - Volume Paths
import shutil
import time
import random
from pathlib import Path

dbutils.widgets.text("catalog", "turbine_project")
dbutils.widgets.text("schema_bronze", "bronze")

CATALOG = dbutils.widgets.get("catalog")
SCHEMA_BRONZE = dbutils.widgets.get("schema_bronze")

SOURCE_VOLUME = Path(f"/Volumes/{CATALOG}/autoloader_metadata/source_volume/turbine_events/turbine_events")
TARGET_VOLUME = Path(f"/Volumes/{CATALOG}/{SCHEMA_BRONZE}/landing_volume")

MIN_DELAY_SEC = 1
MAX_DELAY_SEC = 5

FILE_PATTERN = "*.json"

# COMMAND ----------

# DBTITLE 1,Test Volume Paths
print("Testing volume paths...\n")

if SOURCE_VOLUME.exists():
    source_files = list(SOURCE_VOLUME.rglob(FILE_PATTERN))
    print(f"Source volume exists: {SOURCE_VOLUME}")
    print(f"   Found {len(source_files)} JSON files")
    if source_files:
        print(f"   First file: {source_files[0].name}")
else:
    raise FileNotFoundError(f"Source volume NOT found: {SOURCE_VOLUME}")

print()

if TARGET_VOLUME.exists():
    target_files = list(TARGET_VOLUME.glob(FILE_PATTERN))
    print(f"Target volume exists: {TARGET_VOLUME}")
    print(f"   Contains {len(target_files)} JSON files")
else:
    print(f"Target volume NOT found (will be created): {TARGET_VOLUME}")

# COMMAND ----------

# DBTITLE 1,Helper Functions - Recursive File Search
def get_files_to_copy():
    # Recursive search for JSON files in nested folders
    source_files = sorted(SOURCE_VOLUME.rglob(FILE_PATTERN))
    target_files = {f.name for f in TARGET_VOLUME.glob(FILE_PATTERN)}

    # Only copy files that don't exist in target yet
    return [f for f in source_files if f.name not in target_files]


def copy_atomic(src: Path, dst: Path):
    """
    Atomic copy:
    First copy to temporary file,
    then rename -> Auto Loader won't read half-written files
    """
    temp_dst = dst.with_suffix(".tmp")

    shutil.copy2(src, temp_dst)
    temp_dst.rename(dst)

# COMMAND ----------

def run():
    print("Starting file arrival simulator...")
    print(f"Source: {SOURCE_VOLUME}")
    print(f"Target: {TARGET_VOLUME}")

    TARGET_VOLUME.mkdir(parents=True, exist_ok=True)

    copied = 0
    while True:
        files = get_files_to_copy()

        if not files:
            print(f"All source files delivered ({copied} copied in this run).")
            return

        file_to_copy = files[0]
        destination = TARGET_VOLUME / file_to_copy.name

        copy_atomic(file_to_copy, destination)
        copied += 1

        print(f"Copied -> {file_to_copy.name}")

        time.sleep(random.randint(MIN_DELAY_SEC, MAX_DELAY_SEC))


# COMMAND ----------

run()

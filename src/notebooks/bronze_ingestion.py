# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze ingestion
# MAGIC Auto Loader reads the raw JSON files from the landing volume into the bronze Delta table, with no transformation.
# MAGIC
# MAGIC Serverless does not support `processingTime` triggers, so the stream runs with `availableNow=True` in a loop.
# MAGIC The loop ends after `idle_limit` consecutive iterations that read no new rows, so the job can complete once the simulator has delivered every file.

# COMMAND ----------

# DBTITLE 1,Parameters
import time
from pyspark.sql.functions import current_timestamp

dbutils.widgets.text("catalog", "turbine_project")
dbutils.widgets.text("schema_bronze", "bronze")
dbutils.widgets.text("poll_seconds", "10")
dbutils.widgets.text("idle_limit", "6")

CATALOG = dbutils.widgets.get("catalog")
SCHEMA_BRONZE = dbutils.widgets.get("schema_bronze")
POLL_SECONDS = int(dbutils.widgets.get("poll_seconds"))
IDLE_LIMIT = int(dbutils.widgets.get("idle_limit"))

CHECKPOINT_BASE = f"/Volumes/{CATALOG}/autoloader_metadata/checkpoint_volume"
LANDING_PATH = f"/Volumes/{CATALOG}/{SCHEMA_BRONZE}/landing_volume"
BRONZE_TABLE = f"{CATALOG}.{SCHEMA_BRONZE}.turbine_events_raw"

# COMMAND ----------

# DBTITLE 1,Bronze streaming ingestion
# Read raw JSON files with Auto Loader (no transformation)
stream_df = (spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.schemaLocation", f"{CHECKPOINT_BASE}/schema")
    .load(LANDING_PATH)
    .withColumn("ingestion_time", current_timestamp())
)

iteration = 0
idle_iterations = 0
while idle_iterations < IDLE_LIMIT:
    iteration += 1
    print(f"Iteration {iteration} - {time.strftime('%H:%M:%S')}")

    query = (stream_df.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", f"{CHECKPOINT_BASE}/bronze_checkpoint")
        .trigger(availableNow=True)
        .toTable(BRONZE_TABLE)
    )
    query.awaitTermination()

    progress = query.lastProgress
    rows_read = progress["numInputRows"] if progress else 0
    idle_iterations = idle_iterations + 1 if rows_read == 0 else 0
    print(f"Batch {iteration} done: {rows_read} new rows (idle {idle_iterations}/{IDLE_LIMIT})")
    time.sleep(POLL_SECONDS)

print(f"No new data for {IDLE_LIMIT} consecutive iterations - bronze ingestion finished.")

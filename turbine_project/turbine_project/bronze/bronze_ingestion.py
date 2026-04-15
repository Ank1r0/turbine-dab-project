# Databricks notebook source
# MAGIC %md
# MAGIC # Notebook to immitating the streaming

# COMMAND ----------

# DBTITLE 1,Bronze Streaming Ingestion
import time
from pyspark.sql.functions import current_timestamp

# Read raw JSON files with Auto Loader (no transformation)
stream_df = (spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.schemaLocation", "/Volumes/turbine_project/autoloader_metadata/checkpoint_volume/schema")
    .load("/Volumes/turbine_project/bronze/landing_volume")
    .withColumn("ingestion_time", current_timestamp())
)

# Continuous streaming loop
iteration = 0
while True:
    iteration += 1
    print(f"🔄 Iteration {iteration} - {time.strftime('%H:%M:%S')}")
    
    query = (stream_df.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", "/Volumes/turbine_project/autoloader_metadata/checkpoint_volume/bronze_checkpoint")
        .trigger(availableNow=True)
        .toTable("turbine_project.bronze.turbine_events_raw")
    )
    
    query.awaitTermination()
    print(f"✅ Batch {iteration} done")
    time.sleep(10)
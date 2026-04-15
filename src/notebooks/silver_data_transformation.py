# Databricks notebook source
import shutil
from pathlib import Path

# Drop the table to recreate it with correct schema
spark.sql("DROP TABLE IF EXISTS turbine_project.silver.turbine_events_clean")
print("✅ Silver table dropped")

# Delete the checkpoint to start fresh
checkpoint_path = Path("/Volumes/turbine_project/autoloader_metadata/checkpoint_volume/silver_checkpoint")
if checkpoint_path.exists():
    shutil.rmtree(checkpoint_path)
    print("✅ Silver checkpoint deleted")
else:
    print("⚠️ Checkpoint not found (already clean)")

print("\n🎉 Ready to recreate Silver table with correct schema!")

# COMMAND ----------

df = spark.table("turbine_project.bronze.turbine_events_raw")

display(df)

# COMMAND ----------

# DBTITLE 1,Read New Data from Bronze (Streaming)
# Read Bronze table as a STREAM (only new data)
bronze_stream = (spark.readStream
    .format("delta")
    .table("turbine_project.bronze.turbine_events_raw")
)
#Bronze stream is created.
bronze_stream.printSchema()

# COMMAND ----------

from pyspark.sql.functions import from_json, col
from pyspark.sql.types import ArrayType, StructType, StructField, StringType, LongType


# "values": [
# {"device_id": 13,
#  "metric": "Wind_speed",
#  "timestampUtc": 1774894923509,
#  "datatype": "float",
#  "value": 6.091},


metric_schema = ArrayType(StructType([
    StructField("device_id", LongType()),
    StructField("metric", StringType()),
    StructField("timestampUtc", LongType()),
    StructField("datatype", StringType()),
    StructField("value", StringType())
]))

# Step 2: Parse the JSON string
silver_stream = (bronze_stream
    .withColumn("parsed_values", from_json(col("values"), metric_schema))
)
#check the success

silver_stream.printSchema()

# COMMAND ----------

from pyspark.sql.functions import explode, col

# Add this transformation before foreachBatch
exploded_stream = (silver_stream
    .select(
        explode(col("parsed_values")).alias("metric_data"),
        col("ingestion_time")
    )
    .select(
        col("metric_data.device_id").alias("device_id"),
        col("metric_data.metric").alias("metric"),
        col("metric_data.value").alias("value"),
        col("metric_data.timestampUtc").alias("timestampUtc"),
        col("ingestion_time")
    )
)

# COMMAND ----------

import time
from pyspark.sql.functions import from_unixtime, current_timestamp, col

def process_batch(batch_df, batch_id):
    print(f"Processing batch {batch_id}")
    
    # Pivot and transform
    wide_df = (batch_df
        .groupBy("device_id", "timestampUtc")
        .pivot("metric")
        .agg({"value": "first"})
        # Convert timestampUtc (milliseconds) to datetime
        .withColumn("event_time", from_unixtime(col("timestampUtc")/1000).cast("timestamp"))
        # Add inserted_at column
        .withColumn("inserted_at", current_timestamp())
        # Drop original timestampUtc
        .drop("timestampUtc")
        # Cast metric columns to proper types
        .withColumn("Wind_speed", col("Wind_speed").cast("double"))
        .withColumn("AirPres", col("AirPres").cast("double"))
        .withColumn("Ambient_temperature", col("Ambient_temperature").cast("double"))
        .withColumn("Humidity", col("Humidity").cast("double"))
        .withColumn("Power_output", col("Power_output").cast("double"))
        .withColumn("Rotor_rpm", col("Rotor_rpm").cast("int"))
        .withColumn("Error_code", col("Error_code").cast("int"))
        .withColumn("Brake_active", col("Brake_active").cast("boolean"))
        .withColumn("Grid_connected", col("Grid_connected").cast("boolean"))
        .withColumn("Operation_mode", col("Operation_mode").cast("string"))
        # Deduplication
        .dropDuplicates(["device_id", "event_time"])
    )
    
    # Write to Silver
    wide_df.write.mode("append").saveAsTable("turbine_project.silver.turbine_events_clean")

# Continuous streaming loop (same as Bronze)
iteration = 0
while True:
    iteration += 1
    print(f"🔄 Iteration {iteration} - {time.strftime('%H:%M:%S')}")
    
    query = (exploded_stream.writeStream
        .foreachBatch(process_batch)
        .option("checkpointLocation", "/Volumes/turbine_project/autoloader_metadata/checkpoint_volume/silver_checkpoint")
        .trigger(availableNow=True)  # ✅ Required for serverless
        .start()
    )
    
    query.awaitTermination()
    print(f"✅ Batch {iteration} done")
    time.sleep(10)  # Same as Bronze

# COMMAND ----------

print(schemaCheck)

# COMMAND ----------

from pyspark.sql import functions as F

df = spark.table("turbine_project.silver.turbine_events_clean")

df_check = df.groupBy("device_id").agg(
    F.count("*").alias("info_count")
)

display(df_check)

# COMMAND ----------

# Query silver table for device_id = 12
df_check2 = spark.sql("SELECT * FROM turbine_project.silver.turbine_events_clean WHERE device_id = '13'")

display(df_check2)

# COMMAND ----------

schemaCheck = spark.table("turbine_project.silver.turbine_events_clean")

schemaCheck.printSchema()
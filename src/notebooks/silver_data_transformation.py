# Databricks notebook source
# MAGIC %md
# MAGIC # Silver transformation
# MAGIC Streams new bronze rows, parses the nested JSON payload into one row per metric reading, validates every reading, pivots the valid readings into one wide typed row per (device, event time), and appends only events that are not in silver yet.
# MAGIC
# MAGIC **Rejected readings** go to `turbine_events_rejected` with a `reject_reason`, so nothing is dropped silently:
# MAGIC `missing_device_id`, `missing_metric`, `missing_timestamp`, `malformed_timestamp`, `missing_value`, `unknown_metric`, `invalid_value`.
# MAGIC
# MAGIC The validity rules are duplicated in `monitoring_pipeline.py` for the reconciliation check - keep both in sync.
# MAGIC
# MAGIC Assumption: all readings of one (device, timestamp) event arrive in the same micro-batch. A later batch carrying the same event is ignored (silver is append-only so that gold can stream from it).

# COMMAND ----------

# DBTITLE 1,Parameters
import time
from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, StructType, StructField, StringType, LongType

dbutils.widgets.text("catalog", "turbine_project")
dbutils.widgets.text("schema_bronze", "bronze")
dbutils.widgets.text("schema_silver", "silver")
dbutils.widgets.text("poll_seconds", "10")
dbutils.widgets.text("idle_limit", "6")

CATALOG = dbutils.widgets.get("catalog")
SCHEMA_BRONZE = dbutils.widgets.get("schema_bronze")
SCHEMA_SILVER = dbutils.widgets.get("schema_silver")
POLL_SECONDS = int(dbutils.widgets.get("poll_seconds"))
IDLE_LIMIT = int(dbutils.widgets.get("idle_limit"))

BRONZE_TABLE = f"{CATALOG}.{SCHEMA_BRONZE}.turbine_events_raw"
SILVER_TABLE = f"{CATALOG}.{SCHEMA_SILVER}.turbine_events_clean"
REJECTED_TABLE = f"{CATALOG}.{SCHEMA_SILVER}.turbine_events_rejected"
CHECKPOINT_PATH = f"/Volumes/{CATALOG}/autoloader_metadata/checkpoint_volume/silver_checkpoint"

# COMMAND ----------

# DBTITLE 1,Schema and validity rules
# One element of the "values" array in a bronze row:
# {"device_id": 13, "metric": "Wind_speed", "timestampUtc": 1774894923509, "datatype": "float", "value": 6.091}
metric_schema = ArrayType(StructType([
    StructField("device_id", LongType()),
    StructField("metric", StringType()),
    StructField("timestampUtc", LongType()),
    StructField("datatype", StringType()),
    StructField("value", StringType())
]))

DOUBLE_METRICS = ["Wind_speed", "AirPres", "Ambient_temperature", "Humidity", "Power_output"]
INT_METRICS = ["Rotor_rpm", "Error_code"]
BOOLEAN_METRICS = ["Brake_active", "Grid_connected"]
STRING_METRICS = ["Operation_mode"]
ALL_METRICS = DOUBLE_METRICS + INT_METRICS + BOOLEAN_METRICS + STRING_METRICS

MIN_TIMESTAMP_MS = 946684800000          # 2000-01-01 UTC: anything earlier is malformed
MAX_FUTURE_MS = 24 * 3600 * 1000         # more than 1 day ahead of now is malformed


def reject_reason(max_timestamp_ms):
    """First failing rule for a reading, or NULL if the reading is valid."""
    numeric = DOUBLE_METRICS + INT_METRICS
    return (
        F.when(F.col("device_id").isNull(), "missing_device_id")
         .when(F.col("metric").isNull(), "missing_metric")
         .when(F.col("timestampUtc").isNull(), "missing_timestamp")
         .when((F.col("timestampUtc") < MIN_TIMESTAMP_MS) | (F.col("timestampUtc") > max_timestamp_ms), "malformed_timestamp")
         .when(F.col("value").isNull(), "missing_value")
         .when(~F.col("metric").isin(ALL_METRICS), "unknown_metric")
         # try_cast returns NULL instead of raising when ANSI mode is on
         .when(F.col("metric").isin(numeric) & F.expr("try_cast(value AS double)").isNull(), "invalid_value")
         .when(F.col("metric").isin(BOOLEAN_METRICS) & F.expr("try_cast(value AS boolean)").isNull(), "invalid_value")
    )

# COMMAND ----------

# DBTITLE 1,Read new data from bronze (streaming)
bronze_stream = (spark.readStream
    .format("delta")
    .table(BRONZE_TABLE)
)

# explode_outer keeps rows whose payload failed to parse (NULL array) as one all-NULL reading,
# which the rules below send to the rejected table instead of losing it.
exploded_stream = (bronze_stream
    .withColumn("parsed_values", F.from_json(F.col("values"), metric_schema))
    .select(F.explode_outer("parsed_values").alias("m"), F.col("ingestion_time"))
    .select(
        F.col("m.device_id").alias("device_id"),
        F.col("m.metric").alias("metric"),
        F.col("m.value").alias("value"),
        F.col("m.timestampUtc").alias("timestampUtc"),
        F.col("ingestion_time"),
    )
)

# COMMAND ----------

# DBTITLE 1,Validate, pivot, deduplicate, append
def process_batch(batch_df, batch_id):
    max_timestamp_ms = int(time.time() * 1000) + MAX_FUTURE_MS
    tagged = batch_df.withColumn("reject_reason", reject_reason(max_timestamp_ms))

    rejected = (tagged.filter(F.col("reject_reason").isNotNull())
        .withColumn("batch_id", F.lit(batch_id))
        .withColumn("rejected_at", F.current_timestamp()))
    rejected.write.mode("append").saveAsTable(REJECTED_TABLE)

    valid = tagged.filter(F.col("reject_reason").isNull())

    wide = (valid
        .groupBy("device_id", "timestampUtc")
        .pivot("metric", ALL_METRICS)
        .agg(F.first("value"))
        # millisecond precision is kept; going through from_unixtime would truncate to seconds
        .withColumn("event_time", (F.col("timestampUtc") / 1000).cast("timestamp"))
        .withColumn("inserted_at", F.current_timestamp())
        .drop("timestampUtc")
    )
    for c in DOUBLE_METRICS:
        wide = wide.withColumn(c, F.col(c).cast("double"))
    for c in INT_METRICS:
        wide = wide.withColumn(c, F.col(c).cast("double").cast("int"))
    for c in BOOLEAN_METRICS:
        wide = wide.withColumn(c, F.col(c).cast("boolean"))
    wide = wide.dropDuplicates(["device_id", "event_time"])

    # Deduplicate across batches: skip events that are already in silver.
    if spark.catalog.tableExists(SILVER_TABLE):
        existing = spark.table(SILVER_TABLE).select("device_id", "event_time")
        wide = wide.join(existing, ["device_id", "event_time"], "left_anti")

    wide.write.mode("append").saveAsTable(SILVER_TABLE)
    print(f"Batch {batch_id} written")

# COMMAND ----------

# DBTITLE 1,Run until bronze has no new data
iteration = 0
idle_iterations = 0
while idle_iterations < IDLE_LIMIT:
    iteration += 1
    print(f"Iteration {iteration} - {time.strftime('%H:%M:%S')}")

    query = (exploded_stream.writeStream
        .foreachBatch(process_batch)
        .option("checkpointLocation", CHECKPOINT_PATH)
        .trigger(availableNow=True)  # processingTime is not supported on serverless
        .start()
    )
    query.awaitTermination()

    progress = query.lastProgress
    rows_read = progress["numInputRows"] if progress else 0
    idle_iterations = idle_iterations + 1 if rows_read == 0 else 0
    print(f"Batch {iteration} done: {rows_read} input rows (idle {idle_iterations}/{IDLE_LIMIT})")
    time.sleep(POLL_SECONDS)

print(f"No new bronze data for {IDLE_LIMIT} consecutive iterations - silver transformation finished.")

# COMMAND ----------

# DBTITLE 1,Quick checks
display(spark.table(SILVER_TABLE).groupBy("device_id").agg(F.count("*").alias("events")).orderBy("device_id"))

if spark.catalog.tableExists(REJECTED_TABLE):
    display(spark.table(REJECTED_TABLE).groupBy("reject_reason").agg(F.count("*").alias("readings")))

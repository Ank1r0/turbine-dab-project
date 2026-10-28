# Databricks notebook source
# MAGIC %md
# MAGIC # Gold analytics
# MAGIC One-minute aggregates (avg, min, max, stddev) per device and numeric metric, one gold table per statistic.
# MAGIC
# MAGIC For every micro-batch of new silver rows, the notebook finds the (device, window) pairs the batch touches and **recomputes those windows from the full silver table**, then MERGEs them into gold.
# MAGIC A window that receives late rows in a later batch is therefore recomputed from all of its rows, never overwritten with a partial aggregate.
# MAGIC
# MAGIC The loop ends after `idle_limit` consecutive iterations with no new silver data.

# COMMAND ----------

# DBTITLE 1,Parameters
import time
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType, FloatType, LongType

dbutils.widgets.text("catalog", "turbine_project")
dbutils.widgets.text("schema_silver", "silver")
dbutils.widgets.text("schema_gold", "gold")
dbutils.widgets.text("poll_seconds", "30")
dbutils.widgets.text("idle_limit", "4")

CATALOG = dbutils.widgets.get("catalog")
SCHEMA_SILVER = dbutils.widgets.get("schema_silver")
SCHEMA_GOLD = dbutils.widgets.get("schema_gold")
POLL_INTERVAL = int(dbutils.widgets.get("poll_seconds"))
IDLE_LIMIT = int(dbutils.widgets.get("idle_limit"))

SILVER_TABLE = f"{CATALOG}.{SCHEMA_SILVER}.turbine_events_clean"
GOLD_SCHEMA = f"{CATALOG}.{SCHEMA_GOLD}"
CHECKPOINT_PATH = f"/Volumes/{CATALOG}/autoloader_metadata/checkpoint_volume/gold_checkpoint"
WINDOW = "1 minute"
STATS = ("avg", "min", "max", "stddev")

_EXCLUDE = {"device_id", "event_time", "inserted_at"}
_NUMERIC = (DoubleType, IntegerType, FloatType, LongType)

NUMERIC_COLS = [
    f.name
    for f in spark.table(SILVER_TABLE).schema.fields
    if isinstance(f.dataType, _NUMERIC) and f.name not in _EXCLUDE
]

print(f"Numeric columns ({len(NUMERIC_COLS)}): {NUMERIC_COLS}")

# COMMAND ----------

# DBTITLE 1,Write helpers
def _write_metric(df, metric: str) -> None:
    table  = f"{GOLD_SCHEMA}.turbine_metrics_{metric}"
    suffix = f"_{metric}"
    cols   = [c for c in df.columns if c.endswith(suffix)]

    out = df.select(
        F.col("device_id"),
        F.col("window_start"),
        F.col("window_end"),
        *[F.col(c).alias(c[: -len(suffix)]) for c in cols],
        F.col("inserted_at"),
    )

    view = f"_gold_batch_{metric}"
    out.createOrReplaceTempView(view)

    if not spark.catalog.tableExists(table):
        out.write.format("delta").saveAsTable(table)
        print(f"  [created] {table}")
    else:
        # Each row in `out` is a complete recomputation of its window, so overwriting is safe.
        spark.sql(f"""
            MERGE INTO {table} AS t
            USING {view}        AS s
            ON  t.device_id    = s.device_id
            AND t.window_start = s.window_start
            WHEN MATCHED     THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT  *
        """)
        print(f"  [merged]  {table}")


def process_batch(batch_df, batch_id: int) -> None:
    if batch_df.isEmpty():
        print(f"Batch {batch_id}: empty.")
        return

    print(f"Batch {batch_id}: processing...")

    # (device, window) pairs touched by this batch
    touched = (
        batch_df
        .select("device_id", F.window("event_time", WINDOW).getField("start").alias("window_start"))
        .distinct()
    )

    # All silver rows of those windows, not just the rows in this batch
    full_windows = (
        spark.table(SILVER_TABLE)
        .withColumn("window_start", F.window("event_time", WINDOW).getField("start"))
        .join(touched, ["device_id", "window_start"], "inner")
    )

    agg_exprs = [
        expr
        for c in NUMERIC_COLS
        for expr in (
            F.avg(c).alias(f"{c}_avg"),
            F.min(c).alias(f"{c}_min"),
            F.max(c).alias(f"{c}_max"),
            F.stddev(c).alias(f"{c}_stddev"),
        )
    ]

    agg_df = (
        full_windows
        .groupBy("device_id", F.window("event_time", WINDOW))
        .agg(*agg_exprs)
        .select(
            F.col("device_id"),
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            *[F.col(f"{c}_{stat}") for stat in STATS for c in NUMERIC_COLS],
            F.current_timestamp().alias("inserted_at"),
        )
    )

    for metric in STATS:
        _write_metric(agg_df, metric)

    print(f"Batch {batch_id}: done.\n")

# COMMAND ----------

# DBTITLE 1,Run until silver has no new data
# On serverless, processingTime trigger is unsupported: availableNow=True drains all new silver data
# since the last checkpoint and terminates, then the loop sleeps and repeats.
# "Only new data": the checkpoint stores the last committed silver offset between runs.
# Silver is append-only, so the streaming read never sees update/delete commits.

silver_stream = spark.readStream.format("delta").table(SILVER_TABLE)

print("Gold pipeline started.\n")

iteration = 0
idle_iterations = 0
while idle_iterations < IDLE_LIMIT:
    iteration += 1
    query = (
        silver_stream.writeStream
        .foreachBatch(process_batch)
        .option("checkpointLocation", CHECKPOINT_PATH)
        .trigger(availableNow=True)
        .start()
    )
    query.awaitTermination()

    progress = query.lastProgress
    rows_read = progress["numInputRows"] if progress else 0
    idle_iterations = idle_iterations + 1 if rows_read == 0 else 0

    for metric in STATS:
        table = f"{GOLD_SCHEMA}.turbine_metrics_{metric}"
        if spark.catalog.tableExists(table):
            print(f"{metric.upper()} - {spark.table(table).count()} rows")
        else:
            print(f"{metric.upper()} - table not found")

    print(f"Iteration {iteration}: {rows_read} new silver rows (idle {idle_iterations}/{IDLE_LIMIT}). Sleeping {POLL_INTERVAL}s...\n")
    time.sleep(POLL_INTERVAL)

print(f"No new silver data for {IDLE_LIMIT} consecutive iterations - gold pipeline finished.")

# COMMAND ----------

for metric in STATS:
    table = f"{GOLD_SCHEMA}.turbine_metrics_{metric}"
    if spark.catalog.tableExists(table):
        display(spark.sql(f"SELECT * FROM {table} ORDER BY device_id ASC, window_start ASC"))

# Databricks notebook source
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType, FloatType, LongType
import time

SILVER_TABLE    = "turbine_project.silver.turbine_events_clean"
GOLD_SCHEMA     = "turbine_project.gold"
CHECKPOINT_PATH = "/Volumes/turbine_project/autoloader_metadata/checkpoint_volume/gold_checkpoint"
WATERMARK       = "30 seconds"
WINDOW          = "1 minute"
POLL_INTERVAL   = 30  # seconds between iterations

_EXCLUDE = {"device_id", "event_time", "inserted_at"}
_NUMERIC = (DoubleType, IntegerType, FloatType, LongType)

NUMERIC_COLS = [
    f.name
    for f in spark.table(SILVER_TABLE).schema.fields
    if isinstance(f.dataType, _NUMERIC) and f.name not in _EXCLUDE
]

print(f"Numeric columns ({len(NUMERIC_COLS)}): {NUMERIC_COLS}")

# COMMAND ----------

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

    # Check if table exists using SQL (handles three-part names correctly)
    table_exists = spark.sql(f"SHOW TABLES IN {GOLD_SCHEMA} LIKE 'turbine_metrics_{metric}'").count() > 0
    
    if not table_exists:
        out.write.format("delta").saveAsTable(table)
        print(f"  [created] {table}")
    else:
        # MERGE deduplicates: if same device_id + window_start arrives again
        # (partial window recomputed after late data), we overwrite — no duplicates
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
        batch_df
        .groupBy("device_id", F.window("event_time", WINDOW))
        .agg(*agg_exprs)
        .select(
            F.col("device_id"),
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            *[F.col(f"{c}_avg")    for c in NUMERIC_COLS],
            *[F.col(f"{c}_min")    for c in NUMERIC_COLS],
            *[F.col(f"{c}_max")    for c in NUMERIC_COLS],
            *[F.col(f"{c}_stddev") for c in NUMERIC_COLS],
            F.current_timestamp().alias("inserted_at"),
        )
    )

    for metric in ("avg", "min", "max", "stddev"):
        _write_metric(agg_df, metric)

    print(f"Batch {batch_id}: done.\n")

# COMMAND ----------

# On serverless, processingTime trigger is unsupported.
# Pattern: availableNow=True drains all new Silver data since last checkpoint,
# terminates cleanly, then we sleep and repeat — simulating a continuous pipeline.
#
# Late data strategy:
#   withWatermark keeps window state open for WATERMARK duration.
#   If a batch has partial data for a window (e.g. 10:00–10:00:30),
#   it writes a partial aggregate. When remaining data arrives next iteration,
#   MERGE ON (device_id, window_start) overwrites it — no duplicates, no gaps.
#
# "Only new data" guarantee:
#   checkpointLocation persists the last committed Silver offset between runs,
#   so each availableNow run starts exactly where the previous one ended.

silver_stream = (
    spark.readStream
    .format("delta")
    .table(SILVER_TABLE)
    .withWatermark("event_time", WATERMARK)
)

print("Gold pipeline started.\n")

while True:
    query = (
        silver_stream.writeStream
        .foreachBatch(process_batch)
        .outputMode("append")
        .option("checkpointLocation", CHECKPOINT_PATH)
        .trigger(availableNow=True)
        .start()
    )
    query.awaitTermination()

    for metric in ("avg", "min", "max", "stddev"):
        table = f"{GOLD_SCHEMA}.turbine_metrics_{metric}"
        if spark.catalog.tableExists(table):
            count = spark.table(table).count()
            print(f"{metric.upper()} — {count} rows")
            display(spark.sql(f"SELECT * FROM {table} ORDER BY device_id ASC"))
        else:
            print(f"{metric.upper()} — table not found")

    print(f"Sleeping {POLL_INTERVAL}s...\n")
    time.sleep(POLL_INTERVAL)
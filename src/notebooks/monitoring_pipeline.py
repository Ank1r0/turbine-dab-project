# Databricks notebook source
# MAGIC %md
# MAGIC # Pipeline monitoring and reconciliation
# MAGIC Bronze holds one row per raw file (each with an array of readings) while silver holds one row per (device, event time), so raw row counts are **not** comparable. The check therefore works on events:
# MAGIC
# MAGIC 1. Re-derive the set of **valid events** (device, event time) from bronze, using the same rules as `silver_data_transformation.py`.
# MAGIC 2. Every valid bronze event must be present in silver (`missing_in_silver = 0`), and silver must contain no duplicate events.
# MAGIC 3. Rejected readings are reported per `reject_reason`; gold tables are reported with row counts and window range.
# MAGIC
# MAGIC `mode = snapshot` (default, used by the job) runs once and **fails the task** if a check fails when `fail_on_mismatch = true`. `mode = loop` prints a report every `interval_seconds` for interactive use; in loop mode silver may legitimately lag behind bronze, so mismatches are reported but do not fail.
# MAGIC
# MAGIC The validity rules are duplicated from the silver notebook - keep both in sync.

# COMMAND ----------

# DBTITLE 1,Parameters
import time
from datetime import datetime
from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, StructType, StructField, StringType, LongType

dbutils.widgets.text("catalog", "turbine_project")
dbutils.widgets.text("schema_bronze", "bronze")
dbutils.widgets.text("schema_silver", "silver")
dbutils.widgets.text("schema_gold", "gold")
dbutils.widgets.text("mode", "snapshot")
dbutils.widgets.text("fail_on_mismatch", "true")
dbutils.widgets.text("interval_seconds", "15")

CATALOG = dbutils.widgets.get("catalog")
SCHEMA_BRONZE = dbutils.widgets.get("schema_bronze")
SCHEMA_SILVER = dbutils.widgets.get("schema_silver")
SCHEMA_GOLD = dbutils.widgets.get("schema_gold")
MODE = dbutils.widgets.get("mode").strip().lower()
FAIL_ON_MISMATCH = dbutils.widgets.get("fail_on_mismatch").strip().lower() == "true"
INTERVAL_SECONDS = int(dbutils.widgets.get("interval_seconds"))

BRONZE_TABLE = f"{CATALOG}.{SCHEMA_BRONZE}.turbine_events_raw"
SILVER_TABLE = f"{CATALOG}.{SCHEMA_SILVER}.turbine_events_clean"
REJECTED_TABLE = f"{CATALOG}.{SCHEMA_SILVER}.turbine_events_rejected"
GOLD_TABLES = {stat: f"{CATALOG}.{SCHEMA_GOLD}.turbine_metrics_{stat}" for stat in ("avg", "min", "max", "stddev")}

# COMMAND ----------

# DBTITLE 1,Validity rules (same as silver)
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

MIN_TIMESTAMP_MS = 946684800000
MAX_FUTURE_MS = 24 * 3600 * 1000


def reject_reason(max_timestamp_ms):
    numeric = DOUBLE_METRICS + INT_METRICS
    return (
        F.when(F.col("device_id").isNull(), "missing_device_id")
         .when(F.col("metric").isNull(), "missing_metric")
         .when(F.col("timestampUtc").isNull(), "missing_timestamp")
         .when((F.col("timestampUtc") < MIN_TIMESTAMP_MS) | (F.col("timestampUtc") > max_timestamp_ms), "malformed_timestamp")
         .when(F.col("value").isNull(), "missing_value")
         .when(~F.col("metric").isin(ALL_METRICS), "unknown_metric")
         .when(F.col("metric").isin(numeric) & F.expr("try_cast(value AS double)").isNull(), "invalid_value")
         .when(F.col("metric").isin(BOOLEAN_METRICS) & F.expr("try_cast(value AS boolean)").isNull(), "invalid_value")
    )

# COMMAND ----------

# DBTITLE 1,Checks
def bronze_valid_events():
    """Distinct (device_id, event_time) that silver must contain, derived from bronze."""
    max_timestamp_ms = int(time.time() * 1000) + MAX_FUTURE_MS
    return (
        spark.table(BRONZE_TABLE)
        .withColumn("parsed_values", F.from_json(F.col("values"), metric_schema))
        .select(F.explode_outer("parsed_values").alias("m"))
        .select(
            F.col("m.device_id").alias("device_id"),
            F.col("m.metric").alias("metric"),
            F.col("m.value").alias("value"),
            F.col("m.timestampUtc").alias("timestampUtc"),
        )
        .withColumn("reject_reason", reject_reason(max_timestamp_ms))
        .filter(F.col("reject_reason").isNull())
        .select("device_id", (F.col("timestampUtc") / 1000).cast("timestamp").alias("event_time"))
        .distinct()
    )


def run_checks():
    report = {"failures": []}

    expected = bronze_valid_events()
    silver = spark.table(SILVER_TABLE)

    report["bronze_files"] = spark.table(BRONZE_TABLE).count()
    report["bronze_valid_events"] = expected.count()
    report["silver_events"] = silver.count()
    report["missing_in_silver"] = expected.join(silver.select("device_id", "event_time"), ["device_id", "event_time"], "left_anti").count()
    report["duplicate_events_in_silver"] = (
        silver.groupBy("device_id", "event_time").count().filter(F.col("count") > 1).count()
    )
    report["silver_devices"] = silver.select("device_id").distinct().count()
    report["latest_bronze_ingestion"] = spark.table(BRONZE_TABLE).agg(F.max("ingestion_time")).collect()[0][0]
    report["latest_silver_insert"] = silver.agg(F.max("inserted_at")).collect()[0][0]

    if spark.catalog.tableExists(REJECTED_TABLE):
        report["rejected_by_reason"] = {
            r["reject_reason"]: r["n"]
            for r in spark.table(REJECTED_TABLE).groupBy("reject_reason").agg(F.count("*").alias("n")).collect()
        }
    else:
        report["rejected_by_reason"] = {}

    report["gold"] = {}
    for stat, table in GOLD_TABLES.items():
        if spark.catalog.tableExists(table):
            g = spark.table(table)
            report["gold"][stat] = {
                "rows": g.count(),
                "first_window": g.agg(F.min("window_start")).collect()[0][0],
                "last_window": g.agg(F.max("window_start")).collect()[0][0],
            }
        else:
            report["gold"][stat] = None

    if report["missing_in_silver"] > 0:
        report["failures"].append(f"{report['missing_in_silver']} valid bronze events are missing in silver")
    if report["duplicate_events_in_silver"] > 0:
        report["failures"].append(f"{report['duplicate_events_in_silver']} duplicated (device_id, event_time) keys in silver")
    return report


def print_report(report):
    print(f"Bronze files: {report['bronze_files']:,} | valid events derived from bronze: {report['bronze_valid_events']:,}")
    print(f"Silver events: {report['silver_events']:,} across {report['silver_devices']} devices")
    print(f"Missing in silver: {report['missing_in_silver']:,} | duplicated keys in silver: {report['duplicate_events_in_silver']:,}")
    print(f"Latest bronze ingestion: {report['latest_bronze_ingestion']} | latest silver insert: {report['latest_silver_insert']}")
    print(f"Rejected readings by reason: {report['rejected_by_reason'] or 'none'}")
    for stat, info in report["gold"].items():
        print(f"Gold {stat}: " + ("table not found" if info is None else f"{info['rows']:,} rows, windows {info['first_window']} -> {info['last_window']}"))
    print("RESULT: " + ("OK" if not report["failures"] else "FAILED - " + "; ".join(report["failures"])))

# COMMAND ----------

# DBTITLE 1,Run
if MODE == "loop":
    iteration = 0
    try:
        while True:
            iteration += 1
            print(f"\n{'=' * 70}\nCheck #{iteration} - {datetime.now():%Y-%m-%d %H:%M:%S}\n{'=' * 70}")
            print_report(run_checks())
            time.sleep(INTERVAL_SECONDS)
    except KeyboardInterrupt:
        print(f"Monitoring stopped after {iteration} checks")
else:
    result = run_checks()
    print_report(result)
    if FAIL_ON_MISMATCH and result["failures"]:
        raise AssertionError("Pipeline reconciliation failed: " + "; ".join(result["failures"]))

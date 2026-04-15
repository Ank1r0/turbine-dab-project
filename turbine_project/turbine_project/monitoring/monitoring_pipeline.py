# Databricks notebook source
# DBTITLE 1,Configuration and Imports
import time
from datetime import datetime

# Configuration
MONITORING_INTERVAL_SEC = 15  # Check every 15 seconds
BRONZE_TABLE = "turbine_project.bronze.turbine_events_raw"
SILVER_TABLE = "turbine_project.silver.turbine_events_clean"

print("✅ Monitoring Pipeline Configured")
print(f"📊 Monitoring interval: {MONITORING_INTERVAL_SEC} seconds")
print(f"🔵 Bronze table: {BRONZE_TABLE}")
print(f"🔷 Silver table: {SILVER_TABLE}")

# COMMAND ----------

# DBTITLE 1,Monitoring Functions
def get_layer_metrics(table_name, layer_name):
    """
    Collect metrics from a specific layer table
    """
    try:
        # Check if table exists
        df = spark.table(table_name)
        
        # Get total records (works for all layers)
        total_records = df.count()
        
        # Get unique devices only for Silver (Bronze has raw JSON, no device_id column)
        if layer_name == "Bronze":
            unique_devices = "N/A"
            latest_ts = df.agg(F.max("ingestion_time").alias("latest")).collect()[0]["latest"]
        else:  # Silver
            unique_devices = df.select("device_id").distinct().count()
            latest_ts = df.agg(F.max("event_time").alias("latest")).collect()[0]["latest"]
        
        return {
            "layer": layer_name,
            "total_records": total_records,
            "unique_devices": unique_devices,
            "latest_timestamp": latest_ts,
            "status": "✅ OK"
        }
    except Exception as e:
        return {
            "layer": layer_name,
            "total_records": 0,
            "unique_devices": "N/A",
            "latest_timestamp": None,
            "status": f"❌ Error: {str(e)[:50]}"
        }

print("✅ Monitoring functions defined")

# COMMAND ----------

# DBTITLE 1,Continuous Monitoring Loop
iteration = 0

print("="*70)
print("📊 PIPELINE MONITORING STARTED")
print("="*70)
print("Press 'Cancel' to stop\n")

try:
    while True:
        iteration += 1
        current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        print(f"\n{'='*70}")
        print(f"🔍 Check #{iteration} - {current_time}")
        print(f"{'='*70}")
        
        # Get metrics from both layers
        bronze_metrics = get_layer_metrics(BRONZE_TABLE, "Bronze")
        silver_metrics = get_layer_metrics(SILVER_TABLE, "Silver")
        
        # Display Bronze metrics
        print(f"\n🔵 BRONZE LAYER {bronze_metrics['status']}")
        print(f"   Total Records: {bronze_metrics['total_records']:,}")
        print(f"   Unique Devices: {bronze_metrics['unique_devices']}")
        print(f"   Latest Ingestion: {bronze_metrics['latest_timestamp']}")
        
        # Display Silver metrics
        print(f"\n🔷 SILVER LAYER {silver_metrics['status']}")
        print(f"   Total Records: {silver_metrics['total_records']:,}")
        print(f"   Unique Devices: {silver_metrics['unique_devices']}")
        print(f"   Latest Event: {silver_metrics['latest_timestamp']}")
        
        # Calculate processing metrics
        print(f"\n📊 PROCESSING METRICS")
        record_diff = bronze_metrics['total_records'] - silver_metrics['total_records']
        if bronze_metrics['total_records'] > 0:
            processing_rate = (silver_metrics['total_records'] / bronze_metrics['total_records']) * 100
            print(f"   Processing Rate: {processing_rate:.1f}%")
        else:
            print(f"   Processing Rate: N/A (no Bronze data)")
        
        print(f"   Records Pending: {record_diff:,}")
        
        # Wait before next check
        print(f"\n⏳ Next check in {MONITORING_INTERVAL_SEC} seconds...")
        time.sleep(MONITORING_INTERVAL_SEC)
        
except KeyboardInterrupt:
    print(f"\n\n⏹️ Monitoring stopped by user")
    print(f"Total checks completed: {iteration}")
except Exception as e:
    print(f"\n\n❌ Error occurred: {e}")

# COMMAND ----------

# DBTITLE 1,Quick Status Snapshot (Single Check)
# Run this cell for a one-time status check (without looping)

print("📊 PIPELINE STATUS SNAPSHOT")
print("="*70)

# Get current metrics
bronze_metrics = get_layer_metrics(BRONZE_TABLE, "Bronze")
silver_metrics = get_layer_metrics(SILVER_TABLE, "Silver")

# Display as a summary table
print(f"\n{'Layer':<10} {'Records':<15} {'Devices':<10} {'Status':<20}")
print("-" * 70)
print(f"{bronze_metrics['layer']:<10} {bronze_metrics['total_records']:<15,} {bronze_metrics['unique_devices']:<10} {bronze_metrics['status']:<20}")
print(f"{silver_metrics['layer']:<10} {silver_metrics['total_records']:<15,} {silver_metrics['unique_devices']:<10} {silver_metrics['status']:<20}")

# Processing summary
if bronze_metrics['total_records'] > 0:
    processing_rate = (silver_metrics['total_records'] / bronze_metrics['total_records']) * 100
    print(f"\n📊 Processing Rate: {processing_rate:.1f}%")
    print(f"⏳ Records Pending: {bronze_metrics['total_records'] - silver_metrics['total_records']:,}")
else:
    print(f"\n⚠️ No data in Bronze layer yet")

# COMMAND ----------

# DBTITLE 1,Check Gold Layer Tables
from pyspark.sql import functions as F


# Check all Gold layer aggregation tables
print("🏆 GOLD LAYER STATUS")
print("="*70)

gold_tables = [
    ("AVG", "turbine_project.gold.turbine_metrics_avg"),
    ("MIN", "turbine_project.gold.turbine_metrics_min"),
    ("MAX", "turbine_project.gold.turbine_metrics_max"),
    ("STDDEV", "turbine_project.gold.turbine_metrics_stddev")
]

for metric_type, table_name in gold_tables:
    print(f"\n📊 {metric_type} Table: {table_name}")
    
    if spark.catalog.tableExists(table_name):
        df = spark.table(table_name)
        count = df.count()
        
        if count > 0:
            unique_devices = df.select("device_id").distinct().count()
            unique_windows = df.select("window_start").distinct().count()
            earliest = df.agg(F.min("window_start").alias("earliest")).collect()[0]["earliest"]
            latest = df.agg(F.max("window_start").alias("latest")).collect()[0]["latest"]
            
            print(f"   Status: ✅ Active")
            print(f"   Total Records: {count:,}")
            print(f"   Unique Devices: {unique_devices}")
            print(f"   Time Windows: {unique_windows}")
            print(f"   Earliest Window: {earliest}")
            print(f"   Latest Window: {latest}")
        else:
            print(f"   Status: ⚠️  Table exists but empty")
    else:
        print(f"   Status: ❌ Table not found")

print("\n" + "="*70)
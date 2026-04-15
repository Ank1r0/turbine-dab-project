# Databricks notebook source
BRONZE_TABLE = "turbine_project.bronze.turbine_events_raw"
SILVER_TABLE = "turbine_project.silver.turbine_events_clean"




# COMMAND ----------

df_bronze = spark.table(BRONZE_TABLE)
df_bronze.count()   


# COMMAND ----------

df_silver = spark.table(SILVER_TABLE)
df_silver.count()
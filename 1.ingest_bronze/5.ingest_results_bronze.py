# Databricks notebook source
# MAGIC %run "../config_env/dl_config"
# MAGIC

# COMMAND ----------

# MAGIC %run "../utilities/functions_bronze"

# COMMAND ----------

# MAGIC %sql
# MAGIC use bronze_db

# COMMAND ----------

# get last_watermark for incremental load 
last_watermark = get_last_watermark("bronze_db","inc_load_watermarks","results")
last_watermark

# COMMAND ----------

from pyspark.sql.types import (
    StructType, StructField,
    IntegerType, StringType, FloatType
)

results_schema = StructType(
    fields=[
        StructField("resultId", IntegerType(), False),
        StructField("raceId", IntegerType(), True),
        StructField("driverId", IntegerType(), True),
        StructField("constructorId", IntegerType(), True),
        StructField("number", IntegerType(), True),
        StructField("grid", IntegerType(), True),
        StructField("position", IntegerType(), True),
        StructField("positionText", StringType(), True),
        StructField("positionOrder", IntegerType(), True),
        StructField("points", FloatType(), True),
        StructField("laps", IntegerType(), True),
        StructField("time", StringType(), True),
        StructField("milliseconds", IntegerType(), True),
        StructField("fastestLap", IntegerType(), True),
        StructField("rank", IntegerType(), True),
        StructField("fastestLapTime", StringType(), True),
        StructField("fastestLapSpeed", FloatType(), True),
        StructField("statusId", StringType(), True)
    ]
)


# COMMAND ----------

# Read file from raw container: Incremental load
from pyspark.sql.functions import col

results_raw_df = (
    spark.read.schema(results_schema)
    .json(f"{raw_container_path}/results")
    .withColumn("file_upload_ts", col("_metadata.file_modification_time"))
    .filter(col("file_upload_ts") > last_watermark)
)

# COMMAND ----------

# Add bronze layer metadata columns
if results_raw_df.head(1):  # Only run if df is not empty
    results_bronze_df = add_bronze_metadata(
        df=results_raw_df,
        source_system="ergast_sys",
        file_name="results",
        pk_hash_cols  = ["raceId", "driverId"]
    )

# COMMAND ----------

# Save data to delta table
if results_bronze_df.head(1):
    results_bronze_df.write \
        .format("delta") \
        .mode("append") \
        .saveAsTable("bronze_db.results_bronze")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Save watermark timestamp for next load

# COMMAND ----------

# get max(load_ts) from this batch and add standard bronze metadata columns.
if results_raw_df.head(1):  # Only run if df is not empty
    max_load_ts_df = compute_last_watermark(
        df=results_bronze_df,
        source_system="ergast_sys",
        file_name="results",
        watermark_type="load_ts",
        updated_by="ingest_results_bronze"
    )

# COMMAND ----------

# Create temp view of max_load_ts_df
if results_raw_df.head(1):
    max_load_ts_df.createOrReplaceTempView("v_max_load_ts")

# COMMAND ----------

# Update watermark table with max(load_ts)/last_watermark for this batch
if results_raw_df.head(1):
    update_watermark_table("v_max_load_ts", "bronze_db.inc_load_watermarks")

# COMMAND ----------

from datetime import datetime

dbutils.notebook.exit("Success at: " + str(datetime.now())) 
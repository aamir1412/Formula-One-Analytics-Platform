# Databricks notebook source
# MAGIC %run "../config_env/dl_config"

# COMMAND ----------

# MAGIC %run "../utilities/functions_bronze"

# COMMAND ----------

# get last_watermark for incremental load 
last_watermark = get_last_watermark("bronze_db","inc_load_watermarks","lap_times")
last_watermark

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType

lap_times_schema = StructType(
    fields=[StructField("raceId", IntegerType(), False),
            StructField("driverId", IntegerType(), True),
            StructField("lap", IntegerType(), True),
            StructField("position", IntegerType(), True),
            StructField("time", StringType(), True),
            StructField("milliseconds", IntegerType(), True)
])

# COMMAND ----------

# Read file from raw container: Incremental load 
from pyspark.sql.functions import col

lap_times_raw_df = (
    spark.read
         .option("header", "true")
         .schema(lap_times_schema)
         .csv(f"{raw_container_path}/lap_times")
         .withColumn("file_upload_ts", col("_metadata.file_modification_time"))
         .filter(col("file_upload_ts") > last_watermark)
)

# COMMAND ----------

# Add bronze layer metadata columns
if lap_times_raw_df.head(1):  # Only run if df is not empty
    lap_times_bronze_df = add_bronze_metadata(
        df=lap_times_raw_df,
        source_system="ergast_sys",
        file_name="lap_times",
        pk_hash_cols  = ["raceId", "driverId", "lap"] 
    )

# COMMAND ----------

# Save data to delta table
if lap_times_raw_df.head(1):  # Only run if df is not empty
    lap_times_bronze_df.write.format("delta").mode("append").saveAsTable(
        "bronze_db.lap_times_bronze"
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ### Save watermark timestamp for next load

# COMMAND ----------

# get max(load_ts) from this batch and add standard bronze metadata columns.
if lap_times_raw_df.head(1):  # Only run if df is not empty
    max_load_ts_df = compute_last_watermark(
        df=lap_times_bronze_df,
        source_system="ergast_sys",
        file_name="lap_times",
        watermark_type="load_ts",
        updated_by="ingest_lap_times_bronze"
    )

# COMMAND ----------

# Create temp view of max_load_ts_df
max_load_ts_df.createOrReplaceTempView("v_max_load_ts")

# COMMAND ----------

# Update watermark table with max(load_ts)/last_watermark for this batch
if lap_times_raw_df.head(1):
    update_watermark_table("v_max_load_ts", "bronze_db.inc_load_watermarks")

# COMMAND ----------

from datetime import datetime

dbutils.notebook.exit("Success at: " + str(datetime.now())) 
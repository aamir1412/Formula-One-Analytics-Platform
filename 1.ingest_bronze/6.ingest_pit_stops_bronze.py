# Databricks notebook source
# MAGIC %run "../config_env/dl_config"

# COMMAND ----------

# MAGIC %run "../utilities/functions_bronze"

# COMMAND ----------

# get last_watermark for incremental load 
last_watermark = get_last_watermark("bronze_db","inc_load_watermarks","pit_stops")
last_watermark

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType

pit_stops_schema = StructType(
    fields=[StructField("raceId", IntegerType(), False),
            StructField("driverId", IntegerType(), True),
            StructField("stop", StringType(), True),
            StructField("lap", IntegerType(), True),
            StructField("time", StringType(), True),
            StructField("duration", StringType(), True),
            StructField("milliseconds", IntegerType(), True)
])

# COMMAND ----------

# Read file from raw container: Incremental load 
from pyspark.sql.functions import col

pit_stops_raw_df = (
    spark.read
         .option("header", "true")
         .option("multiLine", True)
         .schema(pit_stops_schema)         
         .json(f"{raw_container_path}/pit_stops")
         .withColumn("file_upload_ts", col("_metadata.file_modification_time"))
         .filter(col("file_upload_ts") > last_watermark)
)

# COMMAND ----------

# Add bronze layer metadata columns
if pit_stops_raw_df.head(1):  # Only run if df is not empty
    pit_stops_bronze_df = add_bronze_metadata(
        df=pit_stops_raw_df,
        source_system="ergast_sys",
        file_name="pit_stops",
        pk_hash_cols  = ["raceId", "driverId", "stop"] 
    )

# COMMAND ----------

# Save data to delta table
if pit_stops_raw_df.head(1):  # Only run if df is not empty
    pit_stops_bronze_df.write.format("delta").mode("append").saveAsTable(
        "bronze_db.pit_Stops_bronze"
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ### Save watermark timestamp for next load

# COMMAND ----------

# get max(load_ts) from this batch and add standard bronze metadata columns.
if pit_stops_raw_df.head(1):  # Only run if df is not empty
    max_load_ts_df = compute_last_watermark(
        df=pit_stops_bronze_df,
        source_system="ergast_sys",
        file_name="pit_stops",
        watermark_type="load_ts",
        updated_by="ingest_pit_stops_bronze"
    )

# COMMAND ----------

# Create temp view of max_load_ts_df
if pit_stops_raw_df.head(1):    
    max_load_ts_df.createOrReplaceTempView("v_max_load_ts")

# COMMAND ----------

# Update watermark table with max(load_ts)/last_watermark for this batch
if pit_stops_raw_df.head(1):
    update_watermark_table("v_max_load_ts", "bronze_db.inc_load_watermarks")

# COMMAND ----------

from datetime import datetime

dbutils.notebook.exit("Success at: " + str(datetime.now())) 
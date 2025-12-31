# Databricks notebook source
# MAGIC %run "../config_env/dl_config"

# COMMAND ----------

# MAGIC %run "../utilities/functions_bronze"

# COMMAND ----------

# get last_watermark for incremental load 
last_watermark = get_last_watermark("bronze_db","inc_load_watermarks","qualifying")
last_watermark

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType

qualifying_schema = StructType(
    fields=[
        StructField("qualifyId", IntegerType(), False),
        StructField("raceId", IntegerType(), True),
        StructField("driverId", IntegerType(), True),
        StructField("constructorId", IntegerType(), True),
        StructField("number", IntegerType(), True),
        StructField("position", IntegerType(), True),
        StructField("q1", StringType(), True),
        StructField("q2", StringType(), True),
        StructField("q3", StringType(), True),
    ]
)

# COMMAND ----------

# Read file from raw container: Incremental load
from pyspark.sql.functions import col

qualifying_raw_df = (
    spark.read.option("header", "true")
    .option("multiline", "true")
    .schema(qualifying_schema)
    .json(f"{raw_container_path}/qualifying")
    .withColumn("file_upload_ts", col("_metadata.file_modification_time"))
    .filter(col("file_upload_ts") > last_watermark)
)

# COMMAND ----------

# Add bronze layer metadata columns
if qualifying_raw_df.head(1):  # Only run if df is not empty
    qualifying_bronze_df = add_bronze_metadata(
        df=qualifying_raw_df,
        source_system="ergast_sys",
        file_name="qualifying",
        pk_hash_cols  = ["raceId", "driverId"]
    )

# COMMAND ----------

# Save data to delta table
if qualifying_raw_df.head(1):  # Only run if df is not empty
    qualifying_bronze_df.write.format("delta").mode("append").saveAsTable(
        "bronze_db.qualifying_bronze"
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ### Save watermark timestamp for next load

# COMMAND ----------

# get max(load_ts) from this batch and add standard bronze metadata columns.
if qualifying_raw_df.head(1):  # Only run if df is not empty
    max_load_ts_df = compute_last_watermark(
        df=qualifying_bronze_df,
        source_system="ergast_sys",
        file_name="qualifying",
        watermark_type="load_ts",
        updated_by="ingest_qualifying_bronze"
    )

# COMMAND ----------

# Create temp view of max_load_ts_df
max_load_ts_df.createOrReplaceTempView("v_max_load_ts")

# COMMAND ----------

# Update watermark table with max(load_ts)/last_watermark for this batch
if qualifying_raw_df.head(1):
    update_watermark_table("v_max_load_ts", "bronze_db.inc_load_watermarks")

# COMMAND ----------

from datetime import datetime

dbutils.notebook.exit("Success at: " + str(datetime.now())) 
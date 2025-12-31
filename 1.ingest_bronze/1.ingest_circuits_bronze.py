# Databricks notebook source
# MAGIC %run "../config_env/dl_config"

# COMMAND ----------

# MAGIC %run "../utilities/functions_bronze"

# COMMAND ----------

# MAGIC %sql
# MAGIC use bronze_db

# COMMAND ----------

# get last_watermark for incremental load 
last_watermark = get_last_watermark("bronze_db","inc_load_watermarks", "circuits")
last_watermark

# COMMAND ----------

from pyspark.sql.types import (
    StructType,    StructField,    IntegerType,    StringType,    DoubleType
)
circuits_schema = StructType(
    fields=[
        StructField("circuitId", IntegerType(), False),
        StructField("circuitRef", StringType(), True),
        StructField("name", StringType(), True),
        StructField("location", StringType(), True),
        StructField("country", StringType(), True),
        StructField("lat", DoubleType(), True),
        StructField("lng", DoubleType(), True),
        StructField("alt", IntegerType(), True),
        StructField("url", StringType(), True),
    ]
)

# COMMAND ----------

# Read file from raw container: Incremental load
from pyspark.sql.functions import col, current_timestamp

circuits_raw_df = (
    spark.read.schema(circuits_schema)
    .option("header", True)
    .csv(f"{raw_container_path}/circuits")
    .withColumn("file_upload_ts", col("_metadata.file_modification_time"))
    .filter(col("file_upload_ts") > last_watermark)
)


# COMMAND ----------

# Add bronze layer metadata columns
if circuits_raw_df.head(1):  # Only run if df is not empty
    circuits_bronze_df = add_bronze_metadata(
        df=circuits_raw_df,
        source_system="ergast_sys",
        file_name="circuits",
        pk_hash_cols  = ["circuitRef"]
    )

# COMMAND ----------

# Save data to delta table
if circuits_raw_df.head(1):  # Only run if df is not empty
    circuits_bronze_df.write.format("delta").mode("append").saveAsTable(
        "bronze_db.circuits_bronze"
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ### Save watermark timestamp for next load

# COMMAND ----------

# get max(load_ts) from this batch and add standard bronze metadata columns.
if circuits_raw_df.head(1):  # Only run if df is not empty
    max_load_ts_df = compute_last_watermark(
        df=circuits_bronze_df,
        source_system="ergast_sys",
        file_name="circuits",
        watermark_type="load_ts",
        updated_by="ingest_circuits_bronze"
    )

# COMMAND ----------

# Create temp view of max_load_ts_df
if circuits_raw_df.head(1):
    max_load_ts_df.createOrReplaceTempView("v_max_load_ts")

# COMMAND ----------

# Update watermark table with max(load_ts)/last_watermark for this batch
if circuits_raw_df.head(1):
    update_watermark_table("v_max_load_ts", "bronze_db.inc_load_watermarks")

# COMMAND ----------

from datetime import datetime

dbutils.notebook.exit("Success at: " + str(datetime.now())) 
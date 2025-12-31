# Databricks notebook source
# MAGIC %run "../config_env/dl_config"

# COMMAND ----------

# MAGIC %run "../utilities/functions_bronze"

# COMMAND ----------

# get last_watermark for incremental load 
last_watermark = get_last_watermark("bronze_db","inc_load_watermarks","drivers")
last_watermark

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType

name_schema = StructType(
    [
        StructField("forename", StringType(), True),
        StructField("surname", StringType(), True),
    ]
)

drivers_schema = StructType(
    [
        StructField("driverId", IntegerType(), False),
        StructField("driverRef", StringType(), True),
        StructField("number", IntegerType(), True),
        StructField("code", StringType(), True),
        StructField("name", name_schema),
        StructField("dob", DateType(), True),
        StructField("nationality", StringType(), True),
        StructField("url", StringType(), True),
    ]
)

# COMMAND ----------

# Read file from raw container: Incremental load 
from pyspark.sql.functions import col

drivers_raw_df = (
    spark.read
         .option("header", "true")  
         .schema(drivers_schema)       
         .json(f"{raw_container_path}/drivers")
         .withColumn("file_upload_ts", col("_metadata.file_modification_time"))
         .filter(col("file_upload_ts") > last_watermark)
)

# COMMAND ----------

# Add bronze layer metadata columns
if drivers_raw_df.head(1):  # Only run if df is not empty
    drivers_bronze_df = add_bronze_metadata(
        df=drivers_raw_df,
        source_system="ergast_sys",
        file_name="drivers",
        pk_hash_cols  = ["driverRef"]
    )

# COMMAND ----------

# Save data to delta table
if drivers_raw_df.head(1):  # Only run if df is not empty
    drivers_bronze_df.write.format("delta").mode("append").saveAsTable(
        "bronze_db.drivers_bronze"
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ### Save watermark timestamp for next load

# COMMAND ----------

# get max(load_ts) from this batch and add standard bronze metadata columns.
if drivers_raw_df.head(1):  # Only run if df is not empty
    max_load_ts_df = compute_last_watermark(
        df=drivers_bronze_df,
        source_system="ergast_sys",
        file_name="drivers",
        watermark_type="load_ts",
        updated_by="ingest_drivers_bronze"
    )

# COMMAND ----------

# Create temp view of max_load_ts_df
if drivers_raw_df.head(1):
    max_load_ts_df.createOrReplaceTempView("v_max_load_ts")

# COMMAND ----------

# Update watermark table with max(load_ts)/last_watermark for this batch
if drivers_raw_df.head(1):
    update_watermark_table("v_max_load_ts", "bronze_db.inc_load_watermarks")

# COMMAND ----------

from datetime import datetime

dbutils.notebook.exit("Success at: " + str(datetime.now())) 
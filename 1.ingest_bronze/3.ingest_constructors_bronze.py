# Databricks notebook source
# MAGIC %run "../config_env/dl_config"

# COMMAND ----------

# MAGIC %run "../utilities/functions_bronze"

# COMMAND ----------

# MAGIC %sql
# MAGIC use bronze_db

# COMMAND ----------

# get last_watermark for incremental load 
last_watermark = get_last_watermark("bronze_db","inc_load_watermarks","constructors")
last_watermark

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType

constructors_schema = StructType([
    StructField("constructorId", IntegerType(), True),
    StructField("constructorRef", StringType(), True),
    StructField("name", StringType(), True),
    StructField("nationality", StringType(), True),
    StructField("url", StringType(), True)
])


# COMMAND ----------

# Read file from raw container: Incremental load
from pyspark.sql.functions import col

constructors_raw_df = (
    spark.read
        .option("header", "true")
        .schema(constructors_schema)
        .json(f"{raw_container_path}/constructors")
        .withColumn("file_upload_ts", col("_metadata.file_modification_time"))
        .filter(col("file_upload_ts") > last_watermark)
)

# COMMAND ----------

# Add bronze layer metadata columns
if constructors_raw_df.head(1):  # Only run if df is not empty
    constructors_bronze_df = add_bronze_metadata(
        df=constructors_raw_df,
        source_system="ergast_sys",
        file_name="constructors",
        pk_hash_cols=["constructorRef"]
    )

# COMMAND ----------

# Save data to delta table
if constructors_raw_df.head(1):
    constructors_bronze_df.write.format("delta").mode("append").saveAsTable(
        "bronze_db.constructors_bronze"
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ### Save watermark timestamp for next load

# COMMAND ----------

# get max(load_ts) from this batch and add standard bronze metadata columns.
if constructors_raw_df.head(1):   # Only run if df is not empty
    max_load_ts_df = compute_last_watermark(
        df=constructors_bronze_df,
        source_system="ergast_sys",
        file_name="constructors",
        watermark_type="load_ts",
        updated_by="ingest_constructors_bronze"
    )

# COMMAND ----------

# Create temp view of max_load_ts_df
max_load_ts_df.createOrReplaceTempView("v_max_load_ts")

# COMMAND ----------

# Update watermark table with max(load_ts)/last_watermark for this batch
if constructors_raw_df.head(1): 
    update_watermark_table("v_max_load_ts", "bronze_db.inc_load_watermarks")

# COMMAND ----------

from datetime import datetime

dbutils.notebook.exit("Success at: " + str(datetime.now())) 
# Databricks notebook source
# MAGIC %run "../config_env/dl_config"

# COMMAND ----------

# MAGIC %sql
# MAGIC use silver_db

# COMMAND ----------

# Read data from bronze layer Delta table
circuits_bz_df = spark.read.table("bronze_db.circuits_bronze")


# COMMAND ----------

# Drop duplicates
circuits_bz_deduped_df = circuits_bz_df.dropDuplicates(["pk_hash"])

# COMMAND ----------

# Select and rename columns
from pyspark.sql.functions import col

circuits_select_df = (
    circuits_bz_deduped_df
        .select(
            col("circuitId").alias("circuit_id"),
            col("circuitRef").alias("circuit_ref"),
            col("name").alias("circuit_name"),
            col("location"),
            col("country"),
            col("lat").alias("latitude"),
            col("lng").alias("longitude"),
            col("alt").alias("altitude"),
            col("pk_hash"),
            col("source_system"),
            col("batch_id")
        )
)


# COMMAND ----------

# Add silver metadata columns
from pyspark.sql.functions import lit, current_timestamp, col

circuits_updated_df = (
    circuits_select_df
        .withColumn("create_ts", current_timestamp()) 
        .withColumn("update_ts", current_timestamp())        
)

# COMMAND ----------

# Create temp view for merge operation
circuits_updated_df.createOrReplaceTempView("v_circuits_silver")

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- Merge data from temp view into silver delta table
# MAGIC MERGE INTO silver_db.circuits_silver AS target
# MAGIC USING v_circuits_silver AS source
# MAGIC ON target.pk_hash = source.pk_hash
# MAGIC
# MAGIC -- Update existing rows
# MAGIC WHEN MATCHED THEN
# MAGIC   UPDATE SET
# MAGIC     target.circuit_id = source.circuit_id,
# MAGIC     target.circuit_name = source.circuit_name,
# MAGIC     target.location = source.location,
# MAGIC     target.country = source.country,
# MAGIC     target.latitude = source.latitude,
# MAGIC     target.longitude = source.longitude,
# MAGIC     target.altitude = source.altitude,
# MAGIC     target.update_ts = current_timestamp()
# MAGIC
# MAGIC -- Insert new rows
# MAGIC WHEN NOT MATCHED THEN
# MAGIC   INSERT (
# MAGIC       circuit_id,
# MAGIC       circuit_ref,
# MAGIC       circuit_name,
# MAGIC       location,
# MAGIC       country,
# MAGIC       latitude,
# MAGIC       longitude,
# MAGIC       altitude,
# MAGIC
# MAGIC       pk_hash,
# MAGIC       source_system,
# MAGIC       batch_id,
# MAGIC       create_ts,
# MAGIC       update_ts
# MAGIC   )
# MAGIC   VALUES (
# MAGIC       source.circuit_id,
# MAGIC       source.circuit_ref,
# MAGIC       source.circuit_name,
# MAGIC       source.location,
# MAGIC       source.country,
# MAGIC       source.latitude,
# MAGIC       source.longitude,
# MAGIC       source.altitude,
# MAGIC       
# MAGIC       source.pk_hash,
# MAGIC       source.source_system,
# MAGIC       source.batch_id,
# MAGIC       source.create_ts,
# MAGIC       source.update_ts
# MAGIC   );
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from silver_db.circuits_silver
# MAGIC

# COMMAND ----------

from datetime import datetime

dbutils.notebook.exit("Success at: " + str(datetime.now())) 
# Databricks notebook source
# MAGIC %run "../config_env/dl_config"

# COMMAND ----------

# Read data from bronze layer delta table
lap_times_bz_df = spark.read.table("bronze_db.lap_times_bronze")

# COMMAND ----------

# Select and rename columns
from pyspark.sql.functions import col

lap_times_select_df = (
    lap_times_bz_df
        .select(
            col("raceId").alias("race_id"),
            col("driverId").alias("driver_id"),
            col("lap"),
            col("position"),
            col("time"),
            col("milliseconds"),
            col("pk_hash"),
            col("source_system"),
            col("batch_id")
        )
)


# COMMAND ----------

# Add new and silver metadata columns
from pyspark.sql.functions import lit, current_timestamp, col, to_timestamp

lap_times_updated_df = (
    lap_times_select_df
        .withColumn("create_ts", current_timestamp())
        .withColumn("update_ts", current_timestamp())    
)

# COMMAND ----------

# Create temp view for merge operation
lap_times_updated_df.createOrReplaceTempView("v_lap_times_silver")

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO silver_db.lap_times_silver AS target
# MAGIC USING v_lap_times_silver AS source
# MAGIC ON  target.pk_hash   = source.pk_hash 
# MAGIC WHEN MATCHED 
# MAGIC THEN UPDATE SET    
# MAGIC     target.position        = source.position,
# MAGIC     target.time            = source.time,
# MAGIC     target.milliseconds    = source.milliseconds,
# MAGIC     target.update_ts       = current_timestamp() 
# MAGIC WHEN NOT MATCHED
# MAGIC THEN INSERT (
# MAGIC     race_id,
# MAGIC     driver_id,
# MAGIC     lap,
# MAGIC     `position`,
# MAGIC     `time`,
# MAGIC     `milliseconds`,
# MAGIC     
# MAGIC     pk_hash,
# MAGIC     source_system,
# MAGIC     batch_id,
# MAGIC     create_ts,
# MAGIC     update_ts
# MAGIC )
# MAGIC VALUES (
# MAGIC     source.race_id,
# MAGIC     source.driver_id,
# MAGIC     source.lap,
# MAGIC     source.position,
# MAGIC     source.time,
# MAGIC     source.milliseconds,
# MAGIC     
# MAGIC     source.pk_hash,
# MAGIC     source.source_system,
# MAGIC     source.batch_id,
# MAGIC     source.create_ts,
# MAGIC     source.update_ts
# MAGIC );

# COMMAND ----------

from datetime import datetime

dbutils.notebook.exit("Success at: " + str(datetime.now())) 
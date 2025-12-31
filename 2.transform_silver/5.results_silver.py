# Databricks notebook source
# MAGIC %run "../config_env/dl_config"

# COMMAND ----------

# Read data from bronze layer delta table
results_bz_df = spark.read.table("bronze_db.results_bronze")   

# COMMAND ----------

# Drop duplicates
results_bz_deduped_df = results_bz_df.dropDuplicates(["pk_hash"])

# COMMAND ----------

# Select and rename columns
from pyspark.sql.functions import col

results_select_df = results_bz_deduped_df.select(
    col("resultId").alias("result_id"),
    col("raceId").alias("race_id"),
    col("driverId").alias("driver_id"),
    col("constructorId").alias("constructor_id"),
    col("number"),
    col("grid"),
    col("position"),
    col("positionText").alias("position_text"),
    col("positionOrder").alias("position_order"),
    col("points"),
    col("laps"),
    col("time"),
    col("milliseconds"),
    col("fastestLap").alias("fastest_lap"),
    col("rank"),
    col("fastestLapTime").alias("fastest_lap_time"),
    col("fastestLapSpeed").alias("fastest_lap_speed"),
    col("statusId").alias("status_id"),
    col("pk_hash"),
    col("source_system"),
    col("batch_id")
)


# COMMAND ----------

# Add silver metadata columns
from pyspark.sql.functions import lit, current_timestamp, col
 
results_updated_df = (
    results_select_df.withColumn("create_ts", current_timestamp())
    .withColumn("update_ts", current_timestamp())
)

# COMMAND ----------

# Create temp view for merge operation
results_updated_df.createOrReplaceTempView("v_results_silver")

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO silver_db.results_silver AS target
# MAGIC USING v_results_silver AS source
# MAGIC ON target.pk_hash = source.pk_hash 
# MAGIC WHEN MATCHED 
# MAGIC THEN UPDATE SET
# MAGIC     target.result_id            = source.result_id,
# MAGIC     target.constructor_id        = source.constructor_id,
# MAGIC     target.number               = source.number,
# MAGIC     target.grid                 = source.grid,
# MAGIC     target.position             = source.position,
# MAGIC     target.position_text        = source.position_text,
# MAGIC     target.position_order       = source.position_order,
# MAGIC     target.points               = source.points,
# MAGIC     target.laps                 = source.laps,
# MAGIC     target.time                 = source.time,
# MAGIC     target.milliseconds         = source.milliseconds,
# MAGIC     target.fastest_lap          = source.fastest_lap,
# MAGIC     target.rank                 = source.rank,
# MAGIC     target.fastest_lap_time     = source.fastest_lap_time,
# MAGIC     target.fastest_lap_speed    = source.fastest_lap_speed,
# MAGIC     target.status_id            = source.status_id,
# MAGIC     target.update_ts            = current_timestamp() 
# MAGIC -- Insert new records
# MAGIC WHEN NOT MATCHED
# MAGIC THEN INSERT (
# MAGIC     result_id,
# MAGIC     race_id,
# MAGIC     driver_id,
# MAGIC     constructor_id,
# MAGIC     `number`,
# MAGIC     grid,
# MAGIC     `position`,
# MAGIC     position_text,
# MAGIC     position_order,
# MAGIC     points,
# MAGIC     laps,
# MAGIC     `time`,
# MAGIC     `milliseconds`,
# MAGIC     fastest_lap,
# MAGIC     rank,
# MAGIC     fastest_lap_time,
# MAGIC     fastest_lap_speed,
# MAGIC     status_id,
# MAGIC     
# MAGIC     pk_hash,    
# MAGIC     source_system,
# MAGIC     batch_id,
# MAGIC     create_ts,
# MAGIC     update_ts    
# MAGIC )
# MAGIC VALUES (
# MAGIC     source.result_id,
# MAGIC     source.race_id,
# MAGIC     source.driver_id,
# MAGIC     source.constructor_id,
# MAGIC     source.number,
# MAGIC     source.grid,
# MAGIC     source.position,
# MAGIC     source.position_text,
# MAGIC     source.position_order,
# MAGIC     source.points,
# MAGIC     source.laps,
# MAGIC     source.time,
# MAGIC     source.milliseconds,
# MAGIC     source.fastest_lap,
# MAGIC     source.rank,
# MAGIC     source.fastest_lap_time,
# MAGIC     source.fastest_lap_speed,
# MAGIC     source.status_id,
# MAGIC
# MAGIC     source.pk_hash,    
# MAGIC     source.source_system,
# MAGIC     source.batch_id,
# MAGIC     source.create_ts,
# MAGIC     source.update_ts    
# MAGIC )

# COMMAND ----------

from datetime import datetime

dbutils.notebook.exit("Success at: " + str(datetime.now())) 
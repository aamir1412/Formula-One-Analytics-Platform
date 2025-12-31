# Databricks notebook source
# MAGIC %run "../config_env/dl_config"

# COMMAND ----------

# Read data from bronze layer delta table
pit_stops_bz_df = spark.read.table("bronze_db.pit_stops_bronze")

# COMMAND ----------

# Drop duplicates
pit_stops_bz_deduped_df = pit_stops_bz_df.dropDuplicates(["pk_hash"])

# COMMAND ----------

print(pit_stops_bz_df.count())
print(pit_stops_bz_deduped_df.count())

# COMMAND ----------

# Select and rename columns
from pyspark.sql.functions import col

pit_stops_select_df = pit_stops_bz_deduped_df.select(
    col("raceId").alias("race_id"),
    col("driverId").alias("driver_id"),
    col("stop"),
    col("lap"),
    col("time"),
    col("duration"),
    col("milliseconds"),
    col("pk_hash"),
    col("source_system"),
    col("batch_id"),
)

# COMMAND ----------

# Add new and silver metadata columns
from pyspark.sql.functions import lit, current_timestamp, col, to_timestamp

pit_stops_updated_df = (
    pit_stops_select_df
        .withColumn("create_ts", current_timestamp())
        .withColumn("update_ts", current_timestamp()) 
)

# COMMAND ----------

# Create temp view for merge operation
pit_stops_updated_df.createOrReplaceTempView("v_pit_stops_silver")

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC MERGE INTO silver_db.pit_stops_silver AS target
# MAGIC USING v_pit_stops_silver AS source
# MAGIC ON  target.pk_hash = source.pk_hash 
# MAGIC -- Merge data from temp view into silver delta table
# MAGIC WHEN MATCHED 
# MAGIC THEN UPDATE SET        
# MAGIC     target.lap             = source.lap,
# MAGIC     target.time            = source.time,
# MAGIC     target.duration        = source.duration,
# MAGIC     target.milliseconds    = source.milliseconds,
# MAGIC     target.update_ts       = current_timestamp() 
# MAGIC -- Insert new records
# MAGIC WHEN NOT MATCHED THEN
# MAGIC   INSERT (
# MAGIC     race_id,
# MAGIC     driver_id,
# MAGIC     stop,
# MAGIC     lap,
# MAGIC     `time`,
# MAGIC     duration,
# MAGIC     `milliseconds`,
# MAGIC     pk_hash,
# MAGIC     source_system,
# MAGIC     batch_id,
# MAGIC     create_ts,
# MAGIC     update_ts  
# MAGIC   )
# MAGIC   VALUES (
# MAGIC     source.race_id,
# MAGIC     source.driver_id,
# MAGIC     source.stop,
# MAGIC     source.lap,
# MAGIC     source.time,
# MAGIC     source.duration,
# MAGIC     source.milliseconds,
# MAGIC     source.pk_hash,
# MAGIC     source.source_system,
# MAGIC     source.batch_id,
# MAGIC     source.create_ts,
# MAGIC     source.update_ts 
# MAGIC   )
# MAGIC

# COMMAND ----------

from datetime import datetime

dbutils.notebook.exit("Success at: " + str(datetime.now())) 
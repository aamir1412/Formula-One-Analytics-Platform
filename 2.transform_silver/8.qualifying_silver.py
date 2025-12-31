# Databricks notebook source
# MAGIC %run "../config_env/dl_config"

# COMMAND ----------

# Read data from bronze layer delta table
qualifying_bz_df = spark.read.table("bronze_db.qualifying_bronze")

# COMMAND ----------

# Drop duplicates
qualifying_bz_deduped_df = qualifying_bz_df.dropDuplicates(["pk_hash"])

# COMMAND ----------

from pyspark.sql.functions import col

qualifying_select_df = (
    qualifying_bz_deduped_df
        .select(
            col("qualifyId").alias("qualify_id"),
            col("raceId").alias("race_id"),
            col("driverId").alias("driver_id"),
            col("constructorId").alias("constructor_id"),
            col("number"),
            col("position"),
            col("q1"),
            col("q2"),
            col("q3"),
            col("pk_hash"),
            col("source_system"),
            col("batch_id")
        )
)


# COMMAND ----------

# Add new and silver metadata columns
from pyspark.sql.functions import lit, current_timestamp, col, to_timestamp
 
qualifying_updated_df = (
    qualifying_select_df
        .withColumn("create_ts", current_timestamp())
        .withColumn("update_ts", current_timestamp()) 
)

# COMMAND ----------

# Create temp view for merge operation
qualifying_updated_df.createOrReplaceTempView("v_qualifying_silver")

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC MERGE INTO silver_db.qualifying_silver AS target
# MAGIC USING v_qualifying_silver AS source
# MAGIC ON target.pk_hash = source.pk_hash 
# MAGIC -- Merge data from temp view into silver delta table
# MAGIC WHEN MATCHED 
# MAGIC THEN UPDATE SET 
# MAGIC     target.qualify_id          = source.qualify_id,    
# MAGIC     target.constructor_id   = source.constructor_id,
# MAGIC     target.number           = source.number,
# MAGIC     target.position         = source.position,
# MAGIC     target.q1               = source.q1,
# MAGIC     target.q2               = source.q2,
# MAGIC     target.q3               = source.q3,
# MAGIC     target.update_ts        = current_timestamp()
# MAGIC -- Insert new records
# MAGIC WHEN NOT MATCHED THEN
# MAGIC   INSERT (
# MAGIC     qualify_id,
# MAGIC     race_id,
# MAGIC     driver_id,
# MAGIC     constructor_id,
# MAGIC     `number`,
# MAGIC     `position`,
# MAGIC     q1,
# MAGIC     q2,
# MAGIC     q3,
# MAGIC     
# MAGIC     pk_hash,    
# MAGIC     source_system,
# MAGIC     batch_id,
# MAGIC     create_ts,
# MAGIC     update_ts 
# MAGIC   )
# MAGIC   VALUES (
# MAGIC     source.qualify_id,
# MAGIC     source.race_id,
# MAGIC     source.driver_id,
# MAGIC     source.constructor_id,
# MAGIC     source.number,
# MAGIC     source.position,
# MAGIC     source.q1,
# MAGIC     source.q2,
# MAGIC     source.q3,
# MAGIC     
# MAGIC     source.pk_hash,
# MAGIC     source.source_system,
# MAGIC     source.batch_id,
# MAGIC     source.create_ts,
# MAGIC     source.update_ts 
# MAGIC   )

# COMMAND ----------

from datetime import datetime

dbutils.notebook.exit("Success at: " + str(datetime.now())) 
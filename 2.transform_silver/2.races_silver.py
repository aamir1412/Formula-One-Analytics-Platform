# Databricks notebook source
# MAGIC %run "../config_env/dl_config"

# COMMAND ----------

# MAGIC %sql
# MAGIC use silver_db

# COMMAND ----------

# Read data from bronze layer delta table
races_bz_df = spark.read.table("bronze_db.races_bronze")

# COMMAND ----------

# Drop duplicates
races_bz_deduped_df = races_bz_df.dropDuplicates(["pk_hash"])

# COMMAND ----------

# Select and rename columns
from pyspark.sql.functions import col 

races_select_df = (
    races_bz_deduped_df
        .select(
            col("raceId").alias("race_id"),
            col("year").alias("race_year"),
            col("round").cast("int"),
            col("circuitId").alias("circuit_id"),
            col("name").alias("race_name"),
            col("date").alias("ingestion_date"),
            col("time").cast("string"),
            col("pk_hash").cast("string"),
            col("source_system").cast("string"),
            col("batch_id").cast("timestamp")
        )
)


# COMMAND ----------

# Add silver metadata columns
from pyspark.sql.functions import expr, current_timestamp, sha2, concat_ws, col

races_updated_df = (
    races_select_df
        .withColumn(
            "race_timestamp",
            expr("""
                try_to_timestamp(
                    concat(ingestion_date, ' ', time),
                    'yyyy-MM-dd HH:mm:ss'
                )
            """)
        )
        .withColumn("create_ts", current_timestamp())
        .withColumn("update_ts", current_timestamp())
)

# COMMAND ----------

# Create temp view for merge operation
races_updated_df.createOrReplaceTempView("v_races_silver")

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- Merge data from temp view into silver delta table
# MAGIC MERGE INTO silver_db.races_silver AS target
# MAGIC USING v_races_silver AS source
# MAGIC ON target.pk_hash = source.pk_hash
# MAGIC WHEN MATCHED THEN
# MAGIC   UPDATE SET
# MAGIC     target.race_id       = source.race_id,  
# MAGIC     target.circuit_id      = source.circuit_id,
# MAGIC     target.race_name       = source.race_name,
# MAGIC     target.ingestion_date  = source.ingestion_date,
# MAGIC     target.race_timestamp  = source.race_timestamp,
# MAGIC     target.update_ts       = current_timestamp()
# MAGIC -- Insert new rows
# MAGIC WHEN NOT MATCHED THEN
# MAGIC   INSERT (
# MAGIC       race_id,
# MAGIC       race_year,
# MAGIC       round,
# MAGIC       circuit_id,
# MAGIC       race_name,
# MAGIC       ingestion_date,
# MAGIC       race_timestamp,
# MAGIC
# MAGIC       pk_hash,
# MAGIC       source_system,
# MAGIC       batch_id,
# MAGIC       create_ts,
# MAGIC       update_ts    
# MAGIC   )
# MAGIC   VALUES (
# MAGIC       source.race_id,
# MAGIC       source.race_year,
# MAGIC       source.round,
# MAGIC       source.circuit_id,
# MAGIC       source.race_name,
# MAGIC       source.ingestion_date,
# MAGIC       source.race_timestamp,
# MAGIC
# MAGIC       source.pk_hash,
# MAGIC       source.source_system,
# MAGIC       source.batch_id,
# MAGIC       source.create_ts,
# MAGIC       source.update_ts
# MAGIC   )

# COMMAND ----------

from datetime import datetime

dbutils.notebook.exit("Success at: " + str(datetime.now())) 
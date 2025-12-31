# Databricks notebook source
# MAGIC %run "../config_env/dl_config"

# COMMAND ----------

# Read data from bronze layer delta table
drivers_bz_df = spark.read.table("bronze_db.drivers_bronze")    

# COMMAND ----------

# Drop duplicates
drivers_bz_deduped_df = drivers_bz_df.dropDuplicates(["pk_hash"])

# COMMAND ----------

# Select and rename columns
from pyspark.sql.functions import col, struct, expr, concat, lit

drivers_select_df = (
    drivers_bz_deduped_df
        .select(
            col("driverId").alias("driver_id"),
            col("driverRef").alias("driver_ref"),
            col("number").alias("driver_number"),
            col("code"),
            concat(
                col("name.forename"),
                lit(" "),
                col("name.surname")
            ).alias("driver_name"),
            col("dob"),
            col("nationality").cast("string").alias("driver_nationality"),
            col("pk_hash").cast("string").alias("pk_hash"),
            col("source_system").cast("string").alias("source_system"),
            col("batch_id")
        )
)

# COMMAND ----------

# Add silver metadata columns
from pyspark.sql.functions import lit, current_timestamp, col, coalesce


drivers_updated_df = drivers_select_df.withColumn(
    "create_ts", current_timestamp()
).withColumn("update_ts", current_timestamp())

# COMMAND ----------

# Create temp view for merge operation
drivers_updated_df.createOrReplaceTempView("v_drivers_silver")

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- Merge data from temp view into silver delta table
# MAGIC MERGE INTO silver_db.drivers_silver AS target
# MAGIC USING v_drivers_silver AS source
# MAGIC ON target.pk_hash = source.pk_hash
# MAGIC WHEN MATCHED
# MAGIC THEN UPDATE SET
# MAGIC     target.driver_id           = source.driver_id,
# MAGIC     target.driver_number        = source.driver_number,
# MAGIC     target.code                 = source.code,
# MAGIC     target.driver_name          = source.driver_name,
# MAGIC     target.dob                  = source.dob,
# MAGIC     target.driver_nationality   = source.driver_nationality,
# MAGIC
# MAGIC     target.source_system        = source.source_system,
# MAGIC     target.batch_id             = source.batch_id,    
# MAGIC     target.update_ts            = current_timestamp()
# MAGIC -- Insert new records
# MAGIC WHEN NOT MATCHED
# MAGIC THEN INSERT (
# MAGIC     driver_id,
# MAGIC     driver_ref,
# MAGIC     driver_number,
# MAGIC     `code`,
# MAGIC     driver_name,
# MAGIC     dob,
# MAGIC     driver_nationality,
# MAGIC
# MAGIC     pk_hash,
# MAGIC     source_system,
# MAGIC     batch_id,
# MAGIC     create_ts,
# MAGIC     update_ts 
# MAGIC )
# MAGIC VALUES (
# MAGIC     source.driver_id,
# MAGIC     source.driver_ref,
# MAGIC     source.driver_number,
# MAGIC     source.code,
# MAGIC     source.driver_name,
# MAGIC     source.dob,
# MAGIC     source.driver_nationality,
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
# Databricks notebook source
# MAGIC %run "../config_env/dl_config"

# COMMAND ----------

# Read data from bronze layer delta table
constructors_bz_df = spark.read.table("bronze_db.constructors_bronze")

# COMMAND ----------

# Drop duplicates
constructors_bz_deduped_df = constructors_bz_df.dropDuplicates(["pk_hash"])

# COMMAND ----------

# Select and rename columns
from pyspark.sql.functions import col, expr
 
constructors_select_df = (
    constructors_bz_deduped_df
        .select(            
            col("constructorId").alias("constructor_id"),
            col("constructorRef").alias("constructor_ref"),
            col("name").alias("constructor_name"),
            col("nationality").cast("string"),            
            col("pk_hash").cast("string"),
            col("source_system").cast("string"),
            col("batch_id")
        )
)

# COMMAND ----------

# Add silver metadata columns
from pyspark.sql.functions import lit, current_timestamp, col

constructors_updated_df = (
    constructors_select_df
        .withColumn("create_ts", current_timestamp()) 
        .withColumn("update_ts", current_timestamp())    
)

# COMMAND ----------

# Create temp view for merge operation
constructors_updated_df.createOrReplaceTempView("v_constructors_silver")

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- Merge data from temp view into silver delta table
# MAGIC MERGE INTO silver_db.constructors_silver AS target
# MAGIC USING v_constructors_silver AS source
# MAGIC ON target.pk_hash = source.pk_hash
# MAGIC WHEN MATCHED
# MAGIC THEN UPDATE SET
# MAGIC     target.constructor_id   = source.constructor_id,
# MAGIC     target.constructor_name  = source.constructor_name,
# MAGIC     target.nationality       = source.nationality,
# MAGIC     target.source_system     = source.source_system,
# MAGIC     target.batch_id          = source.batch_id,
# MAGIC     target.update_ts         = current_timestamp() 
# MAGIC -- Insert new records
# MAGIC WHEN NOT MATCHED
# MAGIC THEN INSERT (
# MAGIC     constructor_id,
# MAGIC     constructor_ref,
# MAGIC     constructor_name,
# MAGIC     nationality,
# MAGIC
# MAGIC     pk_hash,
# MAGIC     source_system,
# MAGIC     batch_id,
# MAGIC     create_ts,
# MAGIC     update_ts 
# MAGIC )
# MAGIC VALUES (
# MAGIC     source.constructor_id,
# MAGIC     source.constructor_ref,
# MAGIC     source.constructor_name,
# MAGIC     source.nationality,
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
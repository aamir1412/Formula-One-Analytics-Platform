# Databricks notebook source
# MAGIC %run "../config_env/dl_config"

# COMMAND ----------

# Read drivers_silver delta table
drivers_df = spark.read.table("silver_db.drivers_silver")

# COMMAND ----------

# Read constructors_silver delta table
constructors_df = (
    spark.read
         .table("silver_db.constructors_silver")
         .withColumnRenamed("constructor_name", "team")
)

# COMMAND ----------

# Read circuits_silver delta table
circuits_df = (
    spark.read
         .table("silver_db.circuits_silver")
         .withColumnRenamed("location", "circuit_location")
)

# COMMAND ----------

# Read races_silver delta table
races_df = (
    spark.read
         .table("silver_db.races_silver")
         .withColumnRenamed("race_timestamp", "race_date")
)

# COMMAND ----------

# Read results_silver delta table
results_df = (
    spark.read
         .table("silver_db.results_silver")
         .withColumnRenamed("time", "race_time")
         .withColumnRenamed("race_id", "result_race_id")
         .withColumnRenamed("driver_id","result_driver_id")
)

# COMMAND ----------

# Join race and circuit table
race_circuits_df = (
    races_df
        .join(circuits_df, races_df.circuit_id == circuits_df.circuit_id) \
        .select(
            races_df.race_id,
            races_df.race_year,
            races_df.race_name,
            races_df.race_date,
            circuits_df.circuit_location
        )
)

# COMMAND ----------

# Join multiple df for race_results
race_results_df = (
    results_df
        .join(
            race_circuits_df,
            results_df.result_race_id == race_circuits_df.race_id
        )
        .join(
            drivers_df,
            results_df.result_driver_id == drivers_df.driver_id
        )
        .join(
            constructors_df,
            results_df.constructor_id == constructors_df.constructor_id
        )
)

# COMMAND ----------

# Select and rename columns
from pyspark.sql.functions import col

race_results_select_df = (
    race_results_df
        .select(
            col("race_id"),
            col("race_year"),
            col("race_name"),
            col("race_date"),
            col("circuit_location"),
            col("result_driver_id").alias("driver_id"),
            col("driver_name"),
            col("driver_number"),
            col("driver_nationality"),
            col("team"),
            col("grid"),
            col("fastest_lap"),
            col("race_time"),
            col("points"),
            col("position")
        )
)


# COMMAND ----------

final_df = race_results_select_df.dropDuplicates(["race_id", "driver_id"])

# COMMAND ----------

print(final_df.count())
print(race_results_df.count())

# COMMAND ----------

# Create temp view on race_results_df(final_df)
final_df.createOrReplaceTempView("v_race_results")

# COMMAND ----------

# MAGIC %sql
# MAGIC select
# MAGIC   race_id,
# MAGIC   circuit_location,
# MAGIC   driver_name,
# MAGIC   team,
# MAGIC   row_number() over(partition by driver_id, race_id order by race_id) as rn,
# MAGIC   -- points,
# MAGIC   -- position,
# MAGIC   driver_id
# MAGIC from v_race_results
# MAGIC order by
# MAGIC   driver_id asc

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- Merge v_race_results into gold_db.race_results
# MAGIC MERGE INTO gold_db.race_results AS tgt
# MAGIC USING v_race_results AS src
# MAGIC ON  tgt.driver_id = src.driver_id
# MAGIC AND tgt.race_id     = src.race_id
# MAGIC
# MAGIC WHEN MATCHED THEN
# MAGIC   UPDATE SET
# MAGIC     tgt.race_year           = src.race_year,
# MAGIC     tgt.race_name           = src.race_name,
# MAGIC     tgt.race_date           = src.race_date,
# MAGIC     tgt.circuit_location    = src.circuit_location,
# MAGIC     tgt.driver_number       = src.driver_number,
# MAGIC     tgt.driver_nationality  = src.driver_nationality,
# MAGIC     tgt.team                = src.team,
# MAGIC     tgt.grid                = src.grid,
# MAGIC     tgt.fastest_lap         = src.fastest_lap,
# MAGIC     tgt.race_time           = src.race_time,
# MAGIC     tgt.points              = src.points,
# MAGIC     tgt.position            = src.position,
# MAGIC     tgt.updated_date        = current_timestamp()
# MAGIC
# MAGIC WHEN NOT MATCHED THEN
# MAGIC   INSERT (
# MAGIC     race_id,
# MAGIC     race_year,
# MAGIC     race_name,
# MAGIC     race_date,
# MAGIC     circuit_location,
# MAGIC     driver_id,
# MAGIC     driver_name,
# MAGIC     driver_number,
# MAGIC     driver_nationality,
# MAGIC     team,
# MAGIC     grid,
# MAGIC     fastest_lap,
# MAGIC     race_time,
# MAGIC     points,
# MAGIC     position,
# MAGIC     created_date,
# MAGIC     updated_date
# MAGIC   )
# MAGIC   VALUES (
# MAGIC     src.race_id,
# MAGIC     src.race_year,
# MAGIC     src.race_name,
# MAGIC     src.race_date,
# MAGIC     src.circuit_location,
# MAGIC     src.driver_id,
# MAGIC     src.driver_name,
# MAGIC     src.driver_number,
# MAGIC     src.driver_nationality,
# MAGIC     src.team,
# MAGIC     src.grid,
# MAGIC     src.fastest_lap,
# MAGIC     src.race_time,
# MAGIC     src.points,
# MAGIC     src.position,
# MAGIC     current_timestamp(),
# MAGIC     current_timestamp()  -- updated_date = created_date
# MAGIC   );

# COMMAND ----------

from datetime import datetime

dbutils.notebook.exit("Success at: " + str(datetime.now())) 
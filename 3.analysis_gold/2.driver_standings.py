# Databricks notebook source
# MAGIC %run "../config_env/dl_config"

# COMMAND ----------

# MAGIC %sql
# MAGIC use gold_db

# COMMAND ----------

# Read race_results delta table
race_results_df = spark.read.table("gold_db.race_results")

# COMMAND ----------

# Calculate driver standings
from pyspark.sql.functions import sum, when, count, col

driver_standings_df = race_results_df.groupBy(
    "race_year", "driver_id", "driver_name", "driver_nationality"
).agg(
    sum("points").alias("total_points"),
    count(when(col("position") == 1, True)).alias("wins"),
)

# COMMAND ----------

# Rank the driver standings
from pyspark.sql.window import Window
from pyspark.sql.functions import desc, rank, asc, current_timestamp

driver_rank_spec = Window.partitionBy("race_year").orderBy(
    desc("total_points"), desc("wins")
)

driver_rank_df = driver_standings_df.withColumn(
    "rank", rank().over(driver_rank_spec)
).withColumn("created_date", current_timestamp())

# COMMAND ----------

# Create temp view from driver_rank_df
driver_rank_df.createOrReplaceTempView("v_driver_standings")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Clear previous stg data
# MAGIC TRUNCATE TABLE gold_db.driver_standings_stg;
# MAGIC
# MAGIC -- Insert data into stg table for driver standings  
# MAGIC INSERT INTO gold_db.driver_standings_stg (
# MAGIC     race_year,
# MAGIC     driver_id,
# MAGIC     driver_name,
# MAGIC     driver_nationality,
# MAGIC     total_points,
# MAGIC     wins,
# MAGIC     rank,
# MAGIC     created_date
# MAGIC )
# MAGIC SELECT
# MAGIC     race_year,
# MAGIC     driver_id,
# MAGIC     driver_name,
# MAGIC     driver_nationality,
# MAGIC     total_points,
# MAGIC     wins,
# MAGIC     rank,
# MAGIC     created_date
# MAGIC FROM v_driver_standings;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Clear driver_standings table
# MAGIC TRUNCATE TABLE gold_db.driver_standings;
# MAGIC
# MAGIC -- Insert data from stg into driver_standings
# MAGIC INSERT INTO gold_db.driver_standings (
# MAGIC     race_year,
# MAGIC     driver_id,
# MAGIC     driver_name,
# MAGIC     driver_nationality,
# MAGIC     total_points,
# MAGIC     wins,
# MAGIC     rank,
# MAGIC     created_date
# MAGIC )
# MAGIC SELECT
# MAGIC     race_year,
# MAGIC     driver_id,
# MAGIC     driver_name,
# MAGIC     driver_nationality,
# MAGIC     total_points,
# MAGIC     wins,
# MAGIC     rank,
# MAGIC     created_date
# MAGIC FROM gold_db.driver_standings_stg;

# COMMAND ----------

from datetime import datetime

dbutils.notebook.exit("Success at: " + str(datetime.now())) 
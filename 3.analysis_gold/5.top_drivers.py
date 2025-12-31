# Databricks notebook source
# MAGIC %sql
# MAGIC -- Calculate and save aggregate metrics for top drivers 
# MAGIC CREATE OR REPLACE TEMP VIEW v_top_drivers AS
# MAGIC WITH driver_agg AS (
# MAGIC     SELECT      
# MAGIC         driver_name,
# MAGIC         COUNT(1)               AS total_races,
# MAGIC         SUM(calculated_points) AS total_points,
# MAGIC         AVG(calculated_points) AS avg_points
# MAGIC     FROM gold_db.top_driver_team_standings
# MAGIC     GROUP BY race_year, driver_name
# MAGIC     -- HAVING COUNT(1) >= 50 (select this to remove noise/drivers with less than 50 races)
# MAGIC )
# MAGIC SELECT
# MAGIC     driver_name,
# MAGIC     total_races,
# MAGIC     total_points,
# MAGIC     avg_points,
# MAGIC     RANK() OVER (ORDER BY avg_points DESC) AS driver_rank,
# MAGIC     current_timestamp() AS created_date
# MAGIC FROM driver_agg;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Clear top_drivers_stg
# MAGIC TRUNCATE TABLE gold_db.top_drivers_stg;
# MAGIC
# MAGIC -- Insert into top_drivers_stg
# MAGIC INSERT INTO gold_db.top_drivers_stg (
# MAGIC     driver_name,
# MAGIC     total_races,
# MAGIC     total_points,
# MAGIC     avg_points,
# MAGIC     driver_rank,
# MAGIC     created_date
# MAGIC )
# MAGIC SELECT
# MAGIC     driver_name,
# MAGIC     total_races,
# MAGIC     total_points,
# MAGIC     avg_points,
# MAGIC     driver_rank,
# MAGIC     created_date
# MAGIC FROM v_top_drivers;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Clear top_drivers
# MAGIC TRUNCATE TABLE gold_db.top_drivers;
# MAGIC
# MAGIC -- Insert data from stg into top_drivers
# MAGIC INSERT INTO gold_db.top_drivers (
# MAGIC     driver_name,
# MAGIC     total_races,
# MAGIC     total_points,
# MAGIC     avg_points,
# MAGIC     driver_rank,
# MAGIC     created_date
# MAGIC )
# MAGIC SELECT
# MAGIC     driver_name,
# MAGIC     total_races,
# MAGIC     total_points,
# MAGIC     avg_points,
# MAGIC     driver_rank,
# MAGIC     created_date
# MAGIC FROM gold_db.top_drivers_stg;
# MAGIC

# COMMAND ----------

from datetime import datetime

dbutils.notebook.exit("Success at: " + str(datetime.now())) 
# Databricks notebook source
# MAGIC %sql
# MAGIC -- Calculate and save aggregate metrics for top drivers 
# MAGIC CREATE OR REPLACE TEMP VIEW v_drivers_trend AS
# MAGIC WITH driver_agg AS (
# MAGIC     SELECT
# MAGIC         race_year,
# MAGIC         driver_name,
# MAGIC         COUNT(1)               AS total_races,
# MAGIC         SUM(calculated_points) AS total_points,
# MAGIC         AVG(calculated_points) AS avg_points
# MAGIC     FROM gold_db.top_driver_team_standings
# MAGIC     GROUP BY race_year, driver_name
# MAGIC     -- HAVING COUNT(1) >= 50
# MAGIC )
# MAGIC SELECT
# MAGIC     race_year,
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
# MAGIC TRUNCATE TABLE gold_db.drivers_trend_stg;
# MAGIC
# MAGIC -- Insert into top_drivers_stg
# MAGIC INSERT INTO gold_db.drivers_trend_stg (
# MAGIC     race_year,
# MAGIC     driver_name,
# MAGIC     total_races,
# MAGIC     total_points,
# MAGIC     avg_points,
# MAGIC     driver_rank,
# MAGIC     created_date
# MAGIC )
# MAGIC SELECT
# MAGIC     race_year,
# MAGIC     driver_name,
# MAGIC     total_races,
# MAGIC     total_points,
# MAGIC     avg_points,
# MAGIC     driver_rank,
# MAGIC     created_date
# MAGIC FROM v_drivers_trend;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Clear top_drivers
# MAGIC TRUNCATE TABLE gold_db.drivers_trend;
# MAGIC
# MAGIC -- Insert data from stg into top_drivers
# MAGIC INSERT INTO gold_db.drivers_trend (
# MAGIC     race_year,
# MAGIC     driver_name,
# MAGIC     total_races,
# MAGIC     total_points,
# MAGIC     avg_points,
# MAGIC     driver_rank,
# MAGIC     created_date
# MAGIC )
# MAGIC SELECT
# MAGIC     race_year,
# MAGIC     driver_name,
# MAGIC     total_races,
# MAGIC     total_points,
# MAGIC     avg_points,
# MAGIC     driver_rank,
# MAGIC     created_date
# MAGIC FROM gold_db.drivers_trend_stg;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from gold_db.drivers_trend
# MAGIC order by race_year desc, driver_rank asc 
# MAGIC limit 10

# COMMAND ----------

from datetime import datetime

dbutils.notebook.exit("Success at: " + str(datetime.now())) 
# Databricks notebook source
# MAGIC %sql
# MAGIC -- Build top 10 driver-team standings dataframe
# MAGIC
# MAGIC CREATE OR REPLACE TEMP VIEW v_top_driver_team AS
# MAGIC     SELECT 
# MAGIC         races.race_year,
# MAGIC         constructors.constructor_name AS team_name,
# MAGIC         drivers.driver_id,
# MAGIC         drivers.driver_name,
# MAGIC         races.race_id,
# MAGIC         results.position,
# MAGIC         results.points,
# MAGIC         11 - results.position AS calculated_points,
# MAGIC         current_timestamp() AS created_date
# MAGIC     FROM silver_db.results_silver results
# MAGIC     JOIN silver_db.drivers_silver drivers 
# MAGIC         ON results.driver_id = drivers.driver_id
# MAGIC     JOIN silver_db.constructors_silver constructors 
# MAGIC         ON results.constructor_id = constructors.constructor_id
# MAGIC     JOIN silver_db.races_silver races 
# MAGIC         ON results.race_id = races.race_id
# MAGIC     WHERE results.position <= 10
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Clear stg table
# MAGIC TRUNCATE TABLE gold_db.top_driver_team_stg;
# MAGIC
# MAGIC -- Insert data into stg table for top_driver_team
# MAGIC INSERT INTO gold_db.top_driver_team_stg (
# MAGIC     race_year,
# MAGIC     team_name,
# MAGIC     driver_id,
# MAGIC     driver_name,
# MAGIC     race_id,
# MAGIC     position,
# MAGIC     points,
# MAGIC     calculated_points,
# MAGIC     created_date
# MAGIC )
# MAGIC SELECT
# MAGIC     race_year,
# MAGIC     team_name,
# MAGIC     driver_id,
# MAGIC     driver_name,
# MAGIC     race_id,
# MAGIC     position,
# MAGIC     points,
# MAGIC     calculated_points,
# MAGIC     created_date
# MAGIC FROM v_top_driver_team;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Truncate top_driver_team_standings table
# MAGIC TRUNCATE TABLE gold_db.top_driver_team_standings;
# MAGIC
# MAGIC -- Insert data from stg into top_driver_team_standings
# MAGIC INSERT INTO gold_db.top_driver_team_standings (
# MAGIC     race_year,
# MAGIC     team_name,
# MAGIC     driver_id,
# MAGIC     driver_name,
# MAGIC     race_id,
# MAGIC     position,
# MAGIC     points,
# MAGIC     calculated_points,
# MAGIC     created_date
# MAGIC )
# MAGIC SELECT 
# MAGIC     race_year,
# MAGIC     team_name,
# MAGIC     driver_id,
# MAGIC     driver_name,
# MAGIC     race_id,
# MAGIC     position,
# MAGIC     points,
# MAGIC     calculated_points,
# MAGIC     created_date
# MAGIC FROM gold_db.top_driver_team_stg;

# COMMAND ----------

from datetime import datetime

dbutils.notebook.exit("Success at: " + str(datetime.now())) 
# Databricks notebook source
# MAGIC %sql
# MAGIC -- Calculate and save aggregate metrics for top teams 
# MAGIC CREATE OR REPLACE TEMPORARY VIEW v_teams_trend AS
# MAGIC WITH team_agg AS (
# MAGIC     SELECT
# MAGIC         race_year,
# MAGIC         team_name,
# MAGIC         COUNT(1)               AS total_races,
# MAGIC         SUM(calculated_points) AS total_points,
# MAGIC         AVG(calculated_points) AS avg_points
# MAGIC     FROM gold_db.top_driver_team_standings
# MAGIC     GROUP BY race_year, team_name   
# MAGIC     -- HAVING COUNT(1) >= 50 (select this to remove noise/ only teams with 50 or more races allowed)
# MAGIC ) 
# MAGIC SELECT
# MAGIC     race_year,
# MAGIC     team_name,
# MAGIC     total_races,
# MAGIC     total_points,
# MAGIC     avg_points,
# MAGIC     RANK() OVER (ORDER BY avg_points DESC) AS team_rank,
# MAGIC     current_timestamp() AS created_date
# MAGIC FROM team_agg
# MAGIC ORDER BY race_year desc, avg_points DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Clear top teams staging table
# MAGIC TRUNCATE TABLE gold_db.teams_trend_stg;
# MAGIC
# MAGIC -- Insert into top_teams_stg table from v_top_teams
# MAGIC INSERT INTO gold_db.teams_trend_stg (
# MAGIC     race_year,
# MAGIC     team_name,
# MAGIC     total_races,
# MAGIC     total_points,
# MAGIC     avg_points,
# MAGIC     team_rank,
# MAGIC     created_date
# MAGIC )
# MAGIC SELECT
# MAGIC     race_year,
# MAGIC     team_name,
# MAGIC     total_races,
# MAGIC     total_points,
# MAGIC     avg_points,
# MAGIC     team_rank,
# MAGIC     created_date
# MAGIC FROM v_teams_trend;
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Clear top teams table
# MAGIC TRUNCATE TABLE gold_db.teams_trend;
# MAGIC
# MAGIC -- Insert into top_teams table from top_teams_stg
# MAGIC INSERT INTO gold_db.teams_trend (
# MAGIC     race_year,
# MAGIC     team_name,
# MAGIC     total_races,
# MAGIC     total_points,
# MAGIC     avg_points,
# MAGIC     team_rank,
# MAGIC     created_date
# MAGIC )
# MAGIC SELECT
# MAGIC     race_year,
# MAGIC     team_name,
# MAGIC     total_races,
# MAGIC     total_points,
# MAGIC     avg_points,
# MAGIC     team_rank,
# MAGIC     created_date
# MAGIC FROM gold_db.teams_trend_stg;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from gold_db.teams_trend
# MAGIC limit 10

# COMMAND ----------

from datetime import datetime

dbutils.notebook.exit("Success at: " + str(datetime.now())) 
# Databricks notebook source


# COMMAND ----------

# MAGIC %sql
# MAGIC -- Calculate and save aggregate metrics for top teams 
# MAGIC CREATE OR REPLACE TEMPORARY VIEW v_top_teams AS
# MAGIC WITH team_agg AS (
# MAGIC     SELECT
# MAGIC         team_name,
# MAGIC         COUNT(1)               AS total_races,
# MAGIC         SUM(calculated_points) AS total_points,
# MAGIC         AVG(calculated_points) AS avg_points
# MAGIC     FROM gold_db.top_driver_team_standings
# MAGIC     GROUP BY team_name   
# MAGIC     HAVING COUNT(1) >= 50 
# MAGIC ) 
# MAGIC SELECT
# MAGIC     team_name,
# MAGIC     total_races,
# MAGIC     total_points,
# MAGIC     avg_points,
# MAGIC     RANK() OVER (ORDER BY avg_points DESC) AS team_rank,
# MAGIC     current_timestamp() AS created_date
# MAGIC FROM team_agg
# MAGIC ORDER BY avg_points DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Clear top teams staging table
# MAGIC TRUNCATE TABLE gold_db.top_teams_stg;
# MAGIC
# MAGIC -- Insert into top_teams_stg table from v_top_teams
# MAGIC INSERT INTO gold_db.top_teams_stg (
# MAGIC     team_name,
# MAGIC     total_races,
# MAGIC     total_points,
# MAGIC     avg_points,
# MAGIC     team_rank,
# MAGIC     created_date
# MAGIC )
# MAGIC SELECT
# MAGIC     team_name,
# MAGIC     total_races,
# MAGIC     total_points,
# MAGIC     avg_points,
# MAGIC     team_rank,
# MAGIC     created_date
# MAGIC FROM v_top_teams;
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Clear top teams table
# MAGIC TRUNCATE TABLE gold_db.top_teams;
# MAGIC
# MAGIC -- Insert into top_teams table from top_teams_stg
# MAGIC INSERT INTO gold_db.top_teams (
# MAGIC     team_name,
# MAGIC     total_races,
# MAGIC     total_points,
# MAGIC     avg_points,
# MAGIC     team_rank,
# MAGIC     created_date
# MAGIC )
# MAGIC SELECT
# MAGIC     team_name,
# MAGIC     total_races,
# MAGIC     total_points,
# MAGIC     avg_points,
# MAGIC     team_rank,
# MAGIC     created_date
# MAGIC FROM gold_db.top_teams_stg;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from gold_db.top_teams
# MAGIC limit 10

# COMMAND ----------

from datetime import datetime

dbutils.notebook.exit("Success at: " + str(datetime.now())) 

# COMMAND ----------


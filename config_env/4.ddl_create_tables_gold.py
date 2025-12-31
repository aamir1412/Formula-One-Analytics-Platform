# Databricks notebook source
# MAGIC %sql
# MAGIC -- -- Drop existing table
# MAGIC -- DROP TABLE IF EXISTS gold_db.race_results;
# MAGIC
# MAGIC -- Ccreate race_results delta table
# MAGIC CREATE TABLE IF NOT EXISTS gold_db.race_results (
# MAGIC     race_id              INT,
# MAGIC     race_year            INT,
# MAGIC     race_name            STRING,
# MAGIC     race_date            TIMESTAMP,
# MAGIC     circuit_location     STRING,
# MAGIC     driver_id            INT,
# MAGIC     driver_name          STRING,
# MAGIC     driver_number        INT,
# MAGIC     driver_nationality   STRING,
# MAGIC     team                 STRING,
# MAGIC     grid                 INT,
# MAGIC     fastest_lap          STRING,
# MAGIC     race_time            STRING,
# MAGIC     points               FLOAT,
# MAGIC     position             STRING,
# MAGIC     created_date         TIMESTAMP,
# MAGIC     updated_date         TIMESTAMP
# MAGIC )
# MAGIC USING DELTA
# MAGIC PARTITIONED BY (race_year)
# MAGIC TBLPROPERTIES (
# MAGIC     delta.columnMapping.mode = 'name',
# MAGIC     delta.autoOptimize.optimizeWrite = true,
# MAGIC     delta.autoOptimize.autoCompact = true
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC -- -- Drop existing table
# MAGIC -- DROP TABLE IF EXISTS gold_db.driver_standings;
# MAGIC
# MAGIC -- Create driver_standings delta table
# MAGIC CREATE TABLE IF NOT EXISTS gold_db.driver_standings (
# MAGIC     race_year           INT,
# MAGIC     driver_id           INT,
# MAGIC     driver_name         STRING,
# MAGIC     driver_nationality  STRING,
# MAGIC     total_points        DOUBLE,
# MAGIC     wins                BIGINT,
# MAGIC     rank                INT,
# MAGIC     created_date        TIMESTAMP
# MAGIC )
# MAGIC USING DELTA
# MAGIC TBLPROPERTIES (
# MAGIC     delta.columnMapping.mode = 'name',
# MAGIC     delta.autoOptimize.optimizeWrite = true,
# MAGIC     delta.autoOptimize.autoCompact = true
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create a stg table for driver standings
# MAGIC CREATE TABLE IF NOT EXISTS gold_db.driver_standings_stg
# MAGIC LIKE gold_db.driver_standings;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- Drop existing table if it exists
# MAGIC -- DROP TABLE IF EXISTS gold_db.constructor_standings;
# MAGIC
# MAGIC -- Create constructor_standings delta table
# MAGIC CREATE TABLE IF NOT EXISTS gold_db.constructor_standings (
# MAGIC     race_year       INT     NOT NULL,
# MAGIC     team            STRING  NOT NULL,
# MAGIC     total_points    DOUBLE,
# MAGIC     wins            BIGINT,
# MAGIC     rank            INT,
# MAGIC     created_date    TIMESTAMP
# MAGIC )
# MAGIC USING DELTA
# MAGIC PARTITIONED BY (race_year)
# MAGIC TBLPROPERTIES (
# MAGIC     delta.columnMapping.mode = 'name',
# MAGIC     delta.autoOptimize.optimizeWrite = true,
# MAGIC     delta.autoOptimize.autoCompact = true
# MAGIC );
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create a stg table for constructor standings
# MAGIC CREATE TABLE IF NOT EXISTS gold_db.constructor_standings_stg
# MAGIC LIKE gold_db.constructor_standings;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- Create top_driver_team_standings delta table
# MAGIC CREATE TABLE IF NOT EXISTS gold_db.top_driver_team_standings (
# MAGIC     race_year          INT,
# MAGIC     team_name          STRING,
# MAGIC     driver_id          INT,
# MAGIC     driver_name        STRING,
# MAGIC     race_id            INT,
# MAGIC     `position`         INT,
# MAGIC     points             FLOAT,
# MAGIC     calculated_points  INT,
# MAGIC     created_date       TIMESTAMP
# MAGIC )
# MAGIC USING DELTA
# MAGIC PARTITIONED BY (race_year)
# MAGIC TBLPROPERTIES (
# MAGIC   delta.columnMapping.mode = 'name',
# MAGIC   delta.autoOptimize.optimizeWrite = true,
# MAGIC   delta.autoOptimize.autoCompact = true
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create stg table for top_driver_team_standings
# MAGIC CREATE TABLE IF NOT EXISTS gold_db.top_driver_team_stg
# MAGIC LIKE gold_db.top_driver_team_standings;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- Create top_drivers delta table
# MAGIC CREATE TABLE IF NOT EXISTS gold_db.top_drivers (
# MAGIC     driver_name     STRING,
# MAGIC     total_races     BIGINT,
# MAGIC     total_points    BIGINT,
# MAGIC     avg_points      FLOAT,
# MAGIC     driver_rank     INT,
# MAGIC     created_date    TIMESTAMP
# MAGIC )
# MAGIC USING DELTA
# MAGIC TBLPROPERTIES (
# MAGIC     delta.columnMapping.mode = 'name',
# MAGIC     delta.autoOptimize.optimizeWrite = true,
# MAGIC     delta.autoOptimize.autoCompact = true
# MAGIC )
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create staging table for top drivers
# MAGIC CREATE TABLE IF NOT EXISTS gold_db.top_drivers_stg
# MAGIC LIKE gold_db.top_drivers;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- Create top_drivers delta table
# MAGIC CREATE TABLE IF NOT EXISTS gold_db.drivers_trend (
# MAGIC     driver_name     STRING,
# MAGIC     race_year       INT,
# MAGIC     total_races     BIGINT,
# MAGIC     total_points    BIGINT,
# MAGIC     avg_points      FLOAT,
# MAGIC     driver_rank     INT,
# MAGIC     created_date    TIMESTAMP
# MAGIC )
# MAGIC USING DELTA
# MAGIC TBLPROPERTIES (
# MAGIC     delta.columnMapping.mode = 'name',
# MAGIC     delta.autoOptimize.optimizeWrite = true,
# MAGIC     delta.autoOptimize.autoCompact = true
# MAGIC )
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create staging table for top drivers
# MAGIC CREATE TABLE IF NOT EXISTS gold_db.drivers_trend_stg
# MAGIC LIKE gold_db.drivers_trend;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- Create top_teams delta table
# MAGIC CREATE TABLE IF NOT EXISTS gold_db.top_teams (
# MAGIC     team_name STRING,
# MAGIC     total_races BIGINT,
# MAGIC     total_points BIGINT,
# MAGIC     avg_points DOUBLE,
# MAGIC     team_rank INT,
# MAGIC     created_date TIMESTAMP
# MAGIC )
# MAGIC USING DELTA
# MAGIC TBLPROPERTIES (
# MAGIC     delta.columnMapping.mode = 'name',
# MAGIC     delta.autoOptimize.optimizeWrite = true,
# MAGIC     delta.autoOptimize.autoCompact = true
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS gold_db.top_teams_stg
# MAGIC LIKE gold_db.top_teams;

# COMMAND ----------

# %sql drop  table gold_db.teams_trend_stg

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- Create top_teams delta table
# MAGIC CREATE TABLE IF NOT EXISTS gold_db.teams_trend (
# MAGIC     race_year INT,
# MAGIC     team_name STRING,
# MAGIC     total_races BIGINT,
# MAGIC     total_points BIGINT,
# MAGIC     avg_points DOUBLE,
# MAGIC     team_rank INT,
# MAGIC     created_date TIMESTAMP
# MAGIC )
# MAGIC USING DELTA
# MAGIC TBLPROPERTIES (
# MAGIC     delta.columnMapping.mode = 'name',
# MAGIC     delta.autoOptimize.optimizeWrite = true,
# MAGIC     delta.autoOptimize.autoCompact = true
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS gold_db.teams_trend_stg
# MAGIC LIKE gold_db.teams_trend;

# COMMAND ----------

# %sql show tables in gold_db
# Databricks notebook source
# MAGIC
# MAGIC %run "../config_env/dl_config"

# COMMAND ----------

# Read race_results delta table
race_results_df = spark.read.table("gold_db.race_results")

# COMMAND ----------

# Calculate constructor standings
from pyspark.sql.functions import sum, when, count, col, expr

constructor_standings_df = (
    race_results_df
        .groupBy(
            "race_year",
            "team"
        )
        .agg(
            sum("points").alias("total_points"),
            count(when(col("position") == 1, True)).alias("wins")
        )
)


# COMMAND ----------

# Rank the constructor standings
from pyspark.sql.window import Window
from pyspark.sql.functions import desc, rank, asc, current_timestamp

constructor_rank_spec = Window.partitionBy("race_year").orderBy(
    desc("total_points"),
    desc("wins")
)

constructor_rank_df = (
    constructor_standings_df
        .withColumn("rank", rank().over(constructor_rank_spec))
        .withColumn("created_date", current_timestamp())
);

# COMMAND ----------

# Create or replace temp view from constructor_rank_df
constructor_rank_df.createOrReplaceTempView("v_constructor_rank")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Clear previous stg data
# MAGIC TRUNCATE TABLE gold_db.constructor_standings_stg;
# MAGIC
# MAGIC -- Insert data into stg table for constructor standings 
# MAGIC INSERT INTO gold_db.constructor_standings_stg (
# MAGIC     race_year,
# MAGIC     team,
# MAGIC     total_points,
# MAGIC     wins,
# MAGIC     rank,
# MAGIC     created_date
# MAGIC )
# MAGIC SELECT
# MAGIC     race_year,
# MAGIC     team,
# MAGIC     total_points,
# MAGIC     wins,
# MAGIC     rank,
# MAGIC     created_date
# MAGIC FROM v_constructor_rank;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Clear gold table
# MAGIC TRUNCATE TABLE gold_db.constructor_standings;
# MAGIC
# MAGIC -- Insert data from stg into constructor_standings
# MAGIC INSERT INTO gold_db.constructor_standings (
# MAGIC     race_year,
# MAGIC     team,
# MAGIC     total_points,
# MAGIC     wins,
# MAGIC     rank,
# MAGIC     created_date
# MAGIC )
# MAGIC SELECT
# MAGIC     race_year,
# MAGIC     team,
# MAGIC     total_points,
# MAGIC     wins,
# MAGIC     rank,
# MAGIC     created_date
# MAGIC FROM gold_db.constructor_standings_stg;
# MAGIC

# COMMAND ----------

from datetime import datetime

dbutils.notebook.exit("Success at: " + str(datetime.now())) 
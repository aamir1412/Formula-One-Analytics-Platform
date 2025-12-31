# Databricks notebook source
# MAGIC %md
# MAGIC ### Z-Order gold layer delta tables

# COMMAND ----------

# MAGIC %sql
# MAGIC OPTIMIZE gold_db.race_results
# MAGIC ZORDER BY (race_id, driver_name);

# COMMAND ----------

# MAGIC %sql
# MAGIC OPTIMIZE gold_db.driver_standings
# MAGIC ZORDER BY (race_year, driver_name);

# COMMAND ----------

# MAGIC %sql
# MAGIC OPTIMIZE gold_db.constructor_standings
# MAGIC ZORDER BY (race_year, team);

# COMMAND ----------

# MAGIC %sql
# MAGIC OPTIMIZE gold_db.top_driver_team_standings
# MAGIC ZORDER BY (driver_id, team_name);
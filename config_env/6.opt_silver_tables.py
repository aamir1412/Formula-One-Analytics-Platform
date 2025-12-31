# Databricks notebook source
# MAGIC %md
# MAGIC ### Z-Order silver layer delta tables

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Results table
# MAGIC OPTIMIZE silver_db.results_silver
# MAGIC ZORDER BY (driver_id, constructor_id);
# MAGIC
# MAGIC -- Lap times
# MAGIC OPTIMIZE silver_db.lap_times_silver
# MAGIC ZORDER BY (driver_id, lap);
# MAGIC
# MAGIC -- Pit stops
# MAGIC OPTIMIZE silver_db.pit_stops_silver
# MAGIC ZORDER BY (driver_id);
# MAGIC
# MAGIC -- Qualifying
# MAGIC OPTIMIZE silver_db.qualifying_silver
# MAGIC ZORDER BY (driver_id, constructor_id);
# MAGIC
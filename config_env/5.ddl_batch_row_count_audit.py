# Databricks notebook source
# MAGIC %sql 
# MAGIC use audit_db

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- Table to track row counts per batch run
# MAGIC CREATE TABLE IF NOT EXISTS audit_db.table_row_counts (
# MAGIC     db_name        STRING      NOT NULL,
# MAGIC     table_name     STRING      NOT NULL,
# MAGIC     row_count      BIGINT      NOT NULL,
# MAGIC     batch_id       STRING      NOT NULL,
# MAGIC     run_ts         TIMESTAMP   NOT NULL,
# MAGIC     run_date       DATE        NOT NULL
# MAGIC )
# MAGIC USING DELTA
# MAGIC PARTITIONED BY (run_date)
# MAGIC TBLPROPERTIES (
# MAGIC     delta.columnMapping.mode = 'name',
# MAGIC     delta.autoOptimize.optimizeWrite = true,
# MAGIC     delta.autoOptimize.autoCompact  = true
# MAGIC );
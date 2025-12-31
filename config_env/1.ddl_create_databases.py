# Databricks notebook source
# MAGIC %run "./dl_config"

# COMMAND ----------

# Create databases
spark.sql(f"create database if not exists bronze_db")
spark.sql(f"create database if not exists silver_db")
spark.sql(f"create database if not exists gold_db")
spark.sql(f"create database if not exists audit_db")


# COMMAND ----------

# MAGIC %sql
# MAGIC show databases
# MAGIC
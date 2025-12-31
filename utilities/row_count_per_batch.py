# Databricks notebook source
# Get batch_id from widget
import uuid

dbutils.widgets.text("batch_id", "")
batch_id = dbutils.widgets.get("batch_id").strip()

# If not provided, generate a UUID string
if not batch_id:
    batch_id = str(uuid.uuid4())

# COMMAND ----------

# Create a dbname from widget
import uuid

dbutils.widgets.text("dbname", "")
dbname = dbutils.widgets.get("dbname").strip()

# If not provided, generate a UUID string
if not dbname:
    dbname = "gold_db"

# COMMAND ----------

# Function to collect row counts
def collect_row_counts(db_name: str):
    tables = [r.tableName for r in spark.sql(f"SHOW TABLES IN {db_name}").collect()]
    
    data = [(db_name, t, spark.table(f"{db_name}.{t}").count()) for t in tables]

    row_count_schema = StructType([
        StructField("db_name", StringType(), False),
        StructField("table_name", StringType(), False),
        StructField("row_count", LongType(), False)
    ])
    return spark.createDataFrame(data, row_count_schema)

# COMMAND ----------

# Get dataframe schema
from pyspark.sql.types import StructType, StructField, StringType, LongType
from pyspark.sql.functions import current_timestamp, current_date, lit

row_count_df = (
    collect_row_counts(dbname)  # bronze_db / silver_db
        .withColumn("batch_id", lit(batch_id))
        .withColumn("run_ts", current_timestamp())
        .withColumn("run_date", current_date())
)

# COMMAND ----------

# Save row counts per batch run
row_count_df.write \
    .format("delta") \
    .mode("append") \
    .insertInto("audit_db.table_row_counts")

# COMMAND ----------

# MAGIC %sql 
# MAGIC select * from audit_db.table_row_counts

# COMMAND ----------

# %sql 
# select * from audit_db.table_row_counts

# COMMAND ----------

# %sql
# SELECT
#     db_name,
#     table_name,
#     row_count AS curr_count,
#     LAG(row_count) OVER (PARTITION BY db_name, table_name ORDER BY run_ts ASC) AS prev_row_count,
#     run_ts AS curr_run_ts,
#     LAG(run_ts) OVER (PARTITION BY db_name, table_name ORDER BY run_ts ASC) AS prev_run_ts,
#     batch_id AS curr_batch_id
# FROM audit_db.table_row_counts
# ORDER BY db_name, table_name, curr_run_ts
# limit 1


# COMMAND ----------

from datetime import datetime

dbutils.notebook.exit("Success at: " + str(datetime.now())) 
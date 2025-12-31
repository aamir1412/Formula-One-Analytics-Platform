# Databricks notebook source
# get last_watermark for incremental load
def get_last_watermark(db, table, file_name_):

    last_watermark = spark.sql(f"""
        SELECT COALESCE(
            MAX(last_watermark),
            TIMESTAMP('1900-01-01 00:00:00')
        ) AS last_watermark
        FROM {db}.{table}
        WHERE source_system = 'ergast_sys'
          AND file_name = '{file_name_}'
    """).collect()[0][0]

    return last_watermark

# COMMAND ----------

# Add bronze layer metadata columns
def add_bronze_metadata(df, source_system, file_name, pk_hash_cols, row_hash_cols=None):

    from pyspark.sql.functions import (
        lit, current_timestamp, year, sha2, concat_ws, col
    )

    # Default row_hash to pk_hash_cols if not provided
    if row_hash_cols is None:
        row_hash_cols = pk_hash_cols

    # Build hash expressions dynamically
    pk_expr = concat_ws("||", *[col(c) for c in pk_hash_cols])

    metadata_df = (
        df.withColumn("source_system", lit(source_system))
        .withColumn("file_name", lit(file_name))
        .withColumn("load_ts", current_timestamp())
        .withColumn("ingest_year", year(current_timestamp()))
        .withColumn("pk_hash", sha2(pk_expr, 256))        
        .withColumn("batch_id",current_timestamp())
    )

    return metadata_df


# COMMAND ----------

# get max(load_ts) from a df batch and add bronze layer metadata columns.
from pyspark.sql.functions import max, current_timestamp, lit

def compute_last_watermark(df, source_system, file_name, watermark_type, updated_by):
    
    if df.head(1):  # Only run if df is not empty
        max_load_ts_df = (
            df.agg(max("load_ts").alias("last_watermark"))
            .withColumn("source_system", lit(source_system))
            .withColumn("file_name", lit(file_name))
            .withColumn("watermark_type", lit(watermark_type))
            .withColumn("status", lit("SUCCESS"))
            .withColumn("updated_at", current_timestamp())
            .withColumn("updated_by", lit(updated_by))
        )
        return max_load_ts_df
    else:
        return None

# COMMAND ----------

# Updates the watermark table with the last_watermark value for each batch
def update_watermark_table(temp_view_name, target_table):
  
    spark.sql(f"""
        MERGE INTO {target_table} AS target
        USING {temp_view_name} AS source
        ON target.source_system = source.source_system
           AND target.file_name = source.file_name

        WHEN MATCHED THEN
          UPDATE SET
            target.last_watermark = source.last_watermark,
            target.status         = source.status,
            target.updated_at     = source.updated_at,
            target.updated_by     = source.updated_by

        WHEN NOT MATCHED THEN
          INSERT (
            source_system,
            file_name,
            watermark_type,
            last_watermark,
            status,
            updated_at,
            updated_by
          )
          VALUES (
            source.source_system,
            source.file_name,
            source.watermark_type,
            source.last_watermark,
            source.status,
            source.updated_at,
            source.updated_by
          )
    """)

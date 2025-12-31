# Databricks notebook source
# MAGIC %run "./dl_config"

# COMMAND ----------

# MAGIC %sql
# MAGIC use bronze_db

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create delta lake tables

# COMMAND ----------

# Watermark table for incremental load
spark.sql(f"""
CREATE TABLE IF NOT EXISTS bronze_db.inc_load_watermarks (
  source_system   STRING  NOT NULL,
  file_name       STRING  NOT NULL,
  watermark_type  STRING  NOT NULL,   
  last_watermark  TIMESTAMP,
  status          STRING,
  updated_at      TIMESTAMP,
  updated_by      STRING
)
USING DELTA 
TBLPROPERTIES (
  delta.columnMapping.mode = 'name',
  delta.autoOptimize.optimizeWrite = true,
  delta.autoOptimize.autoCompact = true
)
""")

# COMMAND ----------

# Create circuits_bronze delta table
spark.sql("""
CREATE TABLE IF NOT EXISTS bronze_db.circuits_bronze (
    circuitId INTEGER NOT NULL,
    circuitRef STRING,
    `name` STRING,
    `location` STRING,
    country STRING,
    lat DOUBLE,
    lng DOUBLE,
    alt INTEGER,
    `url` STRING,
    source_system STRING,
    file_name STRING,
    ingest_year INTEGER,
    
    pk_hash STRING,    
    batch_id TIMESTAMP,
    file_upload_ts TIMESTAMP,
    load_ts TIMESTAMP
)
USING DELTA
PARTITIONED BY (ingest_year)
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
)
""")

# COMMAND ----------

# MAGIC %sql
# MAGIC desc bronze_db.circuits_bronze

# COMMAND ----------

# Create races_bronze delta table
spark.sql("""
CREATE TABLE IF NOT EXISTS bronze_db.races_bronze (
    raceId INT NOT NULL,
    year INT,
    round INT,
    circuitId INT,
    name STRING,
    date DATE,
    time STRING,
    url STRING,
    source_system STRING,
    file_name STRING,
    ingest_year INT,
    
    pk_hash STRING,    
    batch_id TIMESTAMP,    
    file_upload_ts TIMESTAMP,
    load_ts TIMESTAMP
)
USING DELTA
PARTITIONED BY (ingest_year)
TBLPROPERTIES ( 
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
)
""")


# COMMAND ----------

# Create constructors_bronze delta table
spark.sql(f"""
CREATE TABLE IF NOT EXISTS bronze_db.constructors_bronze (
  constructorId INTEGER,
  constructorRef STRING,
  name STRING,
  nationality STRING,
  url STRING,  
  source_system STRING,
  file_name STRING, 
  ingest_year INTEGER,
  
  pk_hash STRING,
  batch_id TIMESTAMP,
  file_upload_ts TIMESTAMP,
  load_ts TIMESTAMP
)
USING DELTA
PARTITIONED BY (ingest_year)
TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact'  = 'true'
)
""")


# COMMAND ----------

# Create drivers_bronze delta table
spark.sql(
    """
    CREATE TABLE IF NOT EXISTS bronze_db.drivers_bronze (
        driverId INTEGER NOT NULL,
        driverRef STRING,
        number INTEGER,
        code STRING,
        name STRUCT<
            forename: STRING,
            surname: STRING
        >,
        dob DATE,
        nationality STRING,
        url STRING,        
        source_system STRING,
        file_name STRING,        
        ingest_year INTEGER,
        
        pk_hash STRING,        
        batch_id TIMESTAMP,
        file_upload_ts TIMESTAMP,
        load_ts TIMESTAMP
    )
    USING DELTA
    PARTITIONED BY (ingest_year)
    TBLPROPERTIES (
        'delta.autoOptimize.optimizeWrite' = 'true',
        'delta.autoOptimize.autoCompact'  = 'true'
    )
    """
)


# COMMAND ----------

# Create results_bronze delta table
spark.sql(
    
    """
    CREATE TABLE IF NOT EXISTS bronze_db.results_bronze (
        resultId INTEGER NOT NULL,
        raceId INTEGER,
        driverId INTEGER,
        constructorId INTEGER,
        number INTEGER,
        grid INTEGER,
        position INTEGER,
        positionText STRING,
        positionOrder INTEGER,
        points FLOAT,
        laps INTEGER,
        time STRING,
        milliseconds INTEGER,
        fastestLap INTEGER,
        rank INTEGER,
        fastestLapTime STRING,
        fastestLapSpeed FLOAT,
        statusId STRING,
        source_system STRING NOT NULL,
        file_name STRING NOT NULL,
        ingest_year INTEGER NOT NULL,
        
        pk_hash STRING,    
        batch_id TIMESTAMP NOT NULL,        
        file_upload_ts TIMESTAMP NOT NULL,
        load_ts TIMESTAMP NOT NULL
    )
    USING DELTA
    PARTITIONED BY (ingest_year)
    TBLPROPERTIES (
        'delta.autoOptimize.optimizeWrite' = 'true',
        'delta.autoOptimize.autoCompact'  = 'true'
    )
    """
)

# COMMAND ----------

# Create pit_Stops_bronze delta table
spark.sql(
    f"""
    CREATE TABLE IF NOT EXISTS bronze_db.pit_Stops_bronze (
        raceId INTEGER NOT NULL,
        driverId INTEGER,
        stop STRING,
        lap INTEGER,
        time STRING,
        duration STRING,
        milliseconds INTEGER,
        source_system STRING,
        file_name STRING,
        ingest_year INTEGER,
        
        pk_hash STRING,        
        batch_id TIMESTAMP,
        file_upload_ts TIMESTAMP,
        load_ts TIMESTAMP
    )
    USING DELTA
    PARTITIONED BY (ingest_year)
    TBLPROPERTIES (
        'delta.autoOptimize.optimizeWrite' = 'true',
        'delta.autoOptimize.autoCompact' = 'true'
    )
    """
)

# COMMAND ----------

# Create lap_times_bronze delta table
spark.sql(
    f"""
    CREATE TABLE IF NOT EXISTS bronze_db.lap_times_bronze (
        raceId INTEGER NOT NULL, 
        driverId INTEGER,
        lap INTEGER,
        position INTEGER,
        time STRING,
        milliseconds INTEGER,
        source_system STRING,
        file_name STRING,
        ingest_year INTEGER,
        
        pk_hash STRING,        
        batch_id TIMESTAMP,
        file_upload_ts TIMESTAMP,
        load_ts TIMESTAMP
    )
    USING DELTA
    PARTITIONED BY (ingest_year)
    TBLPROPERTIES (
        'delta.autoOptimize.optimizeWrite' = 'true',
        'delta.autoOptimize.autoCompact' = 'true'
    )
    """
)


# COMMAND ----------

# Create qualifying_bronze delta table
spark.sql(
    """
    CREATE TABLE IF NOT EXISTS bronze_db.qualifying_bronze (
        qualifyId INTEGER NOT NULL,
        raceId INTEGER,
        driverId INTEGER,
        constructorId INTEGER,
        number INTEGER,
        position INTEGER,
        q1 STRING,
        q2 STRING,
        q3 STRING,
        source_system STRING,
        file_name STRING,
        ingest_year INTEGER,
        
        pk_hash STRING,        
        batch_id TIMESTAMP,
        file_upload_ts TIMESTAMP,
        load_ts TIMESTAMP
    )
    USING DELTA
    TBLPROPERTIES (
        'delta.autoOptimize.optimizeWrite' = 'true',
        'delta.autoOptimize.autoCompact' = 'true'
    )
    """
)


# COMMAND ----------

# MAGIC %sql 
# MAGIC show tables in bronze_db
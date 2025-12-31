# Databricks notebook source
# MAGIC %sql
# MAGIC -- Create circuits_silver delta table
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS silver_db.circuits_silver (
# MAGIC     circuit_id      INT NOT NULL,
# MAGIC     circuit_ref     STRING ,
# MAGIC     circuit_name    STRING ,
# MAGIC     `location`      STRING,
# MAGIC     country         STRING,
# MAGIC     latitude        DOUBLE,
# MAGIC     longitude       DOUBLE,
# MAGIC     altitude        INT,
# MAGIC     
# MAGIC     pk_hash         STRING  NOT NULL,    
# MAGIC     source_system   STRING,
# MAGIC     batch_id        TIMESTAMP,
# MAGIC     create_ts       TIMESTAMP,
# MAGIC     update_ts       TIMESTAMP
# MAGIC )
# MAGIC USING DELTA
# MAGIC TBLPROPERTIES (
# MAGIC delta.columnMapping.mode = 'name',
# MAGIC delta.autoOptimize.optimizeWrite = true,
# MAGIC delta.autoOptimize.autoCompact = true
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create races_silver delta table
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS silver_db.races_silver (
# MAGIC     race_id         INT     NOT NULL,
# MAGIC     race_year       INT,
# MAGIC     round           INT,
# MAGIC     circuit_id      INT,
# MAGIC     race_name       STRING,
# MAGIC     ingestion_date  DATE,
# MAGIC     race_timestamp  TIMESTAMP,
# MAGIC     
# MAGIC     pk_hash         STRING  NOT NULL,    
# MAGIC     source_system   STRING,
# MAGIC     batch_id        TIMESTAMP,
# MAGIC     create_ts       TIMESTAMP,
# MAGIC     update_ts       TIMESTAMP
# MAGIC )
# MAGIC USING DELTA
# MAGIC PARTITIONED BY (race_year)
# MAGIC TBLPROPERTIES (
# MAGIC delta.columnMapping.mode = 'name',
# MAGIC delta.autoOptimize.optimizeWrite = true,
# MAGIC delta.autoOptimize.autoCompact = true
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create constructors_silver delta table
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS silver_db.constructors_silver (
# MAGIC     constructor_id      INT     NOT NULL,
# MAGIC     constructor_ref     STRING,
# MAGIC     constructor_name    STRING,
# MAGIC     nationality         STRING,
# MAGIC     
# MAGIC     pk_hash         STRING  NOT NULL,
# MAGIC     source_system   STRING,
# MAGIC     batch_id        TIMESTAMP,
# MAGIC     create_ts       TIMESTAMP,
# MAGIC     update_ts       TIMESTAMP
# MAGIC )
# MAGIC USING DELTA
# MAGIC TBLPROPERTIES (
# MAGIC delta.columnMapping.mode = 'name',
# MAGIC delta.autoOptimize.optimizeWrite = true,
# MAGIC delta.autoOptimize.autoCompact = true
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create drivers_silver delta table
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS silver_db.drivers_silver (
# MAGIC     driver_id         INT       NOT NULL,
# MAGIC     driver_ref        STRING,
# MAGIC     driver_number     INT,
# MAGIC     `code`            STRING,
# MAGIC     driver_name       STRING,
# MAGIC     dob               DATE,
# MAGIC     driver_nationality       STRING,
# MAGIC     
# MAGIC     pk_hash         STRING  NOT NULL,    
# MAGIC     source_system   STRING,
# MAGIC     batch_id        TIMESTAMP,
# MAGIC     create_ts       TIMESTAMP,
# MAGIC     update_ts       TIMESTAMP
# MAGIC )
# MAGIC USING DELTA
# MAGIC TBLPROPERTIES (
# MAGIC delta.columnMapping.mode = 'name',
# MAGIC delta.autoOptimize.optimizeWrite = true,
# MAGIC delta.autoOptimize.autoCompact = true
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create results_silver delta table
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS silver_db.results_silver (
# MAGIC     result_id             INT   NOT NULL,
# MAGIC     race_id               INT,
# MAGIC     driver_id             INT,
# MAGIC     constructor_id        INT,
# MAGIC     `number`              INT,
# MAGIC     grid                  INT,
# MAGIC     `position`            STRING,
# MAGIC     position_text         STRING,
# MAGIC     position_order        INT,
# MAGIC     points                FLOAT,
# MAGIC     laps                  INT,
# MAGIC     `time`                STRING,
# MAGIC     `milliseconds`        STRING,
# MAGIC     fastest_lap           STRING,
# MAGIC     rank                  INT,
# MAGIC     fastest_lap_time      STRING,
# MAGIC     fastest_lap_speed     STRING,
# MAGIC     status_id             INT,
# MAGIC     
# MAGIC     pk_hash               STRING NOT NULL,
# MAGIC     source_system         STRING,
# MAGIC     batch_id              TIMESTAMP,
# MAGIC     create_ts             TIMESTAMP,
# MAGIC     update_ts             TIMESTAMP
# MAGIC )
# MAGIC USING DELTA
# MAGIC PARTITIONED BY (race_id)
# MAGIC TBLPROPERTIES (
# MAGIC delta.columnMapping.mode = 'name',
# MAGIC delta.autoOptimize.optimizeWrite = true,
# MAGIC delta.autoOptimize.autoCompact = true
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create pit_stops_silver delta table
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS silver_db.pit_stops_silver (
# MAGIC     race_id           INT   NOT NULL,
# MAGIC     driver_id         INT,
# MAGIC     stop              STRING,
# MAGIC     lap               INT,
# MAGIC     `time`            STRING,
# MAGIC     duration          STRING,
# MAGIC     `milliseconds`    INT,
# MAGIC     
# MAGIC     pk_hash         STRING  NOT NULL,    
# MAGIC     source_system   STRING,
# MAGIC     batch_id        TIMESTAMP,
# MAGIC     create_ts       TIMESTAMP,
# MAGIC     update_ts       TIMESTAMP
# MAGIC )
# MAGIC USING DELTA
# MAGIC PARTITIONED BY (race_id)
# MAGIC TBLPROPERTIES (
# MAGIC delta.columnMapping.mode = 'name',
# MAGIC delta.autoOptimize.optimizeWrite = true,
# MAGIC delta.autoOptimize.autoCompact = true
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create lap_times_silver delta table
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS silver_db.lap_times_silver (
# MAGIC     race_id           INT NOT NULL,
# MAGIC     driver_id         INT,
# MAGIC     lap               INT,
# MAGIC     `position`        INT,
# MAGIC     `time`            STRING,
# MAGIC     `milliseconds`    INT,
# MAGIC
# MAGIC     pk_hash           STRING  NOT NULL,    
# MAGIC     source_system     STRING,
# MAGIC     batch_id          TIMESTAMP,
# MAGIC     create_ts         TIMESTAMP,
# MAGIC     update_ts         TIMESTAMP
# MAGIC )
# MAGIC USING DELTA
# MAGIC PARTITIONED BY (race_id)
# MAGIC TBLPROPERTIES (
# MAGIC delta.columnMapping.mode = 'name',
# MAGIC delta.autoOptimize.optimizeWrite = true,
# MAGIC delta.autoOptimize.autoCompact = true
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create qualifying_silver delta table
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS silver_db.qualifying_silver (
# MAGIC     qualify_id        INT   NOT NULL,
# MAGIC     race_id           INT,
# MAGIC     driver_id         INT,
# MAGIC     constructor_id    INT,
# MAGIC     `number`          INT,
# MAGIC     `position`        INT,
# MAGIC     q1                STRING,
# MAGIC     q2                STRING,
# MAGIC     q3                STRING,
# MAGIC     
# MAGIC     pk_hash           STRING  NOT NULL,
# MAGIC     source_system     STRING,
# MAGIC     batch_id          TIMESTAMP,
# MAGIC     create_ts         TIMESTAMP,
# MAGIC     update_ts         TIMESTAMP
# MAGIC )
# MAGIC USING DELTA
# MAGIC PARTITIONED BY (race_id)
# MAGIC TBLPROPERTIES (
# MAGIC delta.columnMapping.mode = 'name',
# MAGIC delta.autoOptimize.optimizeWrite = true,
# MAGIC delta.autoOptimize.autoCompact = true
# MAGIC );
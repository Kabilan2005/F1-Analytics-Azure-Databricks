-- Databricks notebook source
CREATE DATABASE demo;
CREATE DATABASE IF NOT EXISTS demo;

-- COMMAND ----------

SHOW DATABASES;
DESCRIBE DATABASE EXTENDED demo;

-- COMMAND ----------

SELECT current_database()

-- COMMAND ----------

SHOW TABLES;
SHOW TABLES IN demo;

-- COMMAND ----------

USE DATABASE demo;

-- COMMAND ----------

-- MAGIC %run "../includes/configuration"

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC race_results_df = spark.read.parquet(f'{presentation_folder_path}/race_results')

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC race_results_df.write.format('parquet').saveAsTable("demo.race_results_python")

-- COMMAND ----------

use demo;

-- COMMAND ----------

DESC EXTENDED race_results_python

-- COMMAND ----------

SELECT * 
FROM demo.race_results_python
WHERE race_year = 2020

-- COMMAND ----------

CREATE OR REPLACE TABLE demo.race_results_sql AS (
  SELECT * 
  FROM demo.race_results_python
  WHERE race_year = 2020
)

-- COMMAND ----------

DESC EXTENDED demo.race_results_sql

-- COMMAND ----------

drop table demo.race_results_sql

-- COMMAND ----------

show tables

-- COMMAND ----------

-- MAGIC %python 
-- MAGIC race_results_df.write.mode('overwrite').format('parquet') \
-- MAGIC .option("path",f'{presentation_folder_path}/race_results_external_python')\
-- MAGIC .saveAsTable("demo.race_results_external_python")

-- COMMAND ----------

DESC EXTENDED race_results_external_python

-- COMMAND ----------

CREATE OR REPLACE TABLE demo.race_results_external_sql
LOCATION "abfss://presentation@f1storageaccforanalytics.dfs.core.windows.net/race_results_external_sql"
 AS (
  SELECT *
  FROM demo.race_results_external_python
)

-- COMMAND ----------

SHOW TABLES IN demo

-- COMMAND ----------

DROP TABLE demo.race_results_external_sql

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW v_race_results AS(
  SELECT *
  FROM demo.race_results_python
  WHERE race_year = 2018
)

-- COMMAND ----------

SELECT *
FROM v_race_results

-- COMMAND ----------

CREATE OR REPLACE GLOBAL TEMP VIEW gv_race_results AS(
  SELECT *
  FROM demo.race_results_python
  WHERE race_year = 2012
)

-- COMMAND ----------

SELECT *
FROM global_temp.gv_race_results

-- COMMAND ----------

SHOW TABLES in global_temp

-- COMMAND ----------

CREATE OR REPLACE VIEW pv_race_results AS(
  SELECT *
  FROM demo.race_results_python
  WHERE race_year = 2000
)

-- COMMAND ----------

SHOW TABLES

-- COMMAND ----------

SELECT *
FROM demo.pv_race_results
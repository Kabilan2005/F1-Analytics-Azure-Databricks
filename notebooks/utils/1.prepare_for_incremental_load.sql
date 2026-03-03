-- Databricks notebook source
-- MAGIC %md
-- MAGIC ###Drop All the Tables and Create Again

-- COMMAND ----------

DROP DATABASE IF EXISTS f1_processed CASCADE;

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS f1_processed
LOCATION "abfss://processed@f1storageaccforanalytics.dfs.core.windows.net/";

-- COMMAND ----------

DROP DATABASE IF EXISTS f1_presentation CASCADE;

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS f1_presentation
LOCATION "abfss://presentation@f1storageaccforanalytics.dfs.core.windows.net/";

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ###Full Load - Circuits, Races, Contructors, Drivers

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ###Incremental Load - Results, Pit Stops, Lap Times, Qualifying

-- COMMAND ----------

DROP TABLE IF EXISTS f1_processed.qualifying

-- COMMAND ----------

SHOW TABLES IN f1_processed
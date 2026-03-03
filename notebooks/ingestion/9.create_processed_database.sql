-- Databricks notebook source
CREATE DATABASE IF NOT EXISTS f1_processed
LOCATION "abfss://processed@f1storageaccforanalytics.dfs.core.windows.net/"

-- COMMAND ----------

DESCRIBE EXTENDED f1_raw.circuits

-- COMMAND ----------

use f1_processed

-- COMMAND ----------

SELECT *
FROM circuits
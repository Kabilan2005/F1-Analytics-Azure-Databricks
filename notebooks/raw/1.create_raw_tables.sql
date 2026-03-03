-- Databricks notebook source
CREATE DATABASE IF NOT EXISTS f1_raw;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ###Create Circuits Table

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS f1_raw.circuits(
  circuitId INTEGER,
  circuitRef STRING,
  name STRING,
  location STRING,
  country STRING,
  lat DOUBLE,
  lng DOUBLE,
  alt INTEGER,
  url STRING
)
USING CSV
OPTIONS (
  path "abfss://raw@f1storageaccforanalytics.dfs.core.windows.net/circuits.csv",
  header True
)

-- COMMAND ----------

-- DROP TABLE IF EXISTS f1_raw.circuits

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS f1_raw.races (
  raceId INTEGER,
  year INTEGER,
  round INTEGER,
  circuitId INTEGER,
  name STRING,
  date DATE,
  time STRING,
  url STRING
)
USING CSV
OPTIONS (
  path "abfss://raw@f1storageaccforanalytics.dfs.core.windows.net/races.csv",
  header True
)

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS f1_raw.constructors (
  constructorId INT, 
  constructorRef String, 
  name String, 
  nationality String, 
  url String
)
USING JSON
OPTIONS(
  path "abfss://raw@f1storageaccforanalytics.dfs.core.windows.net/constructors.json"
)

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS f1_raw.drivers (
  driverId INTEGER, 
  driverRef STRING, 
  number INTEGER,
  code STRING,
  name STRUCT<forename STRING,surname STRING>,
  dob DATE,
  nationality STRING,
  url STRING
)
USING JSON
OPTIONS (
  path "abfss://raw@f1storageaccforanalytics.dfs.core.windows.net/drivers.json"
)

-- COMMAND ----------

SELECT COUNT(1)
FROM f1_raw.lap_times

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS f1_raw.results (
  resultId INTEGER,
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
  fastestLapSpeed STRING,
  statusId INTEGER
)
USING JSON
OPTIONS (
  path "abfss://raw@f1storageaccforanalytics.dfs.core.windows.net/results.json"
)

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS f1_raw.pit_stops (
  raceId INTEGER,
  driverId INTEGER,
  stop INTEGER,
  lap INTEGER,
  time STRING,
  duration STRING,
  milliseconds INTEGER
)
USING JSON
OPTIONS (
  path "abfss://raw@f1storageaccforanalytics.dfs.core.windows.net/pit_stops.json",
  multiLine True
)

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS f1_raw.lap_times (
  raceId INTEGER,
  driverId INTEGER,
  lap INTEGER,
  position INTEGER,
  time STRING,
  milliseconds INTEGER
)
USING CSV
OPTIONS (
  path "abfss://raw@f1storageaccforanalytics.dfs.core.windows.net/lap_times"
)

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS f1_raw.qualifying (
  qualifyId INTEGER,
  raceId INTEGER,
  driverId INTEGER,
  constructorId INTEGER,
  number INTEGER,
  position INTEGER,
  q1 STRING,
  q2 STRING,
  q3 STRING
)
USING JSON
OPTIONS (
  path "abfss://raw@f1storageaccforanalytics.dfs.core.windows.net/qualifying.json",
  multiLine True
)
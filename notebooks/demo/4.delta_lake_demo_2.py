# Databricks notebook source
# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS f1_demo.drivers_convert_to_delta (
# MAGIC   driverId INT,
# MAGIC   dob DATE,
# MAGIC   forename STRING,
# MAGIC   surname STRING,
# MAGIC   createdDate DATE,
# MAGIC   updatedDate DATE
# MAGIC )
# MAGIC USING PARQUET

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO f1_demo.drivers_convert_to_delta
# MAGIC SELECT * FROM f1_demo.drivers_merge

# COMMAND ----------

# MAGIC %sql
# MAGIC CONVERT TO DELTA f1_demo.drivers_convert_to_delta

# COMMAND ----------

df = spark.read.table("f1_demo.drivers_convert_to_delta")

# COMMAND ----------

df.write.mode('overwrite').parquet(f'{demo_folder_path}/drivers_convert_to_delta_new')

# COMMAND ----------

# MAGIC %sql
# MAGIC CONVERT TO DELTA parquet.`abfss://demo@f1storageaccforanalytics.dfs.core.windows.net/drivers_convert_to_delta_new`
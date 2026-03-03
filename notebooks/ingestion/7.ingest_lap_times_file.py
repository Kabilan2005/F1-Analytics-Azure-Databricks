# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC #Ingest Lap times File

# COMMAND ----------

# MAGIC %md
# MAGIC ###Step - 1: Read csv file using the spark dataframe reader 
# MAGIC ###Step - 2: Define the required Schema

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

dbutils.widgets.text("p_data_source","")
v_data_source = dbutils.widgets.get("p_data_source")

dbutils.widgets.text("p_file_date","")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType

lap_times_schema = StructType(fields = [
    StructField('raceId',IntegerType(),False),
    StructField('driverId',IntegerType(),False),
    StructField('lap',IntegerType(),False),
    StructField('position',IntegerType(),True),
    StructField('time',StringType(),True),
    StructField('milliseconds',IntegerType(),True)                          
])

# COMMAND ----------

lap_times_df = spark.read.csv(f'{raw_folder_path}/{v_file_date}/lap_times/lap_times_split*.csv',schema=lap_times_schema)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ###Step - 3 : Rename and Add ingestion date

# COMMAND ----------

from pyspark.sql.functions import current_timestamp,lit,col

lap_times_final_df = add_ingestion_date(lap_times_df).withColumnRenamed('raceId','race_Id') \
.withColumnRenamed('driverId','driver_Id') \
.withColumn('data_source',lit(v_data_source)) \
.withColumn('file_date',lit(v_file_date)) 

# COMMAND ----------

# MAGIC %md 
# MAGIC ###Step - 4 : Write data to datalake in parquet(Columnar Object Format)

# COMMAND ----------

columns = lap_times_final_df.columns

# COMMAND ----------

incremental_load_delta('f1_processed','lap_times',processed_folder_path,lap_times_final_df,'race_Id',"tgt.race_ID = src.race_ID AND tgt.driver_ID = src.driver_ID AND tgt.lap = src.lap")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ###Read and Check for verification

# COMMAND ----------

# spark.read.parquet(f'{processed_folder_path}/lap_times').display()

# COMMAND ----------

# dbutils.notebook.exit("Success")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT race_Id,COUNT(1)
# MAGIC FROM f1_processed.lap_times
# MAGIC GROUP BY race_Id
# MAGIC ORDER BY race_Id DESC
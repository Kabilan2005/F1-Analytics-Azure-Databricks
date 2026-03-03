# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC #Ingest Qualifying Multiple Multi Line JSON File

# COMMAND ----------

# MAGIC %md
# MAGIC ###Step - 1: Read JSON files using the spark dataframe reader 
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

qualifying_schema = StructType(fields = [
    StructField('qualifyId', IntegerType(), False),
    StructField('raceId', IntegerType(), False),
    StructField('driverId', IntegerType(), False),
    StructField('constructorId', IntegerType(), False),
    StructField('number', IntegerType(), False),
    StructField('position', IntegerType(), True),
    StructField('q1', StringType(), True),
    StructField('q2', StringType(), True),
    StructField('q3', StringType(), True)
])

# COMMAND ----------

qualifying_df = spark.read.json(f'{raw_folder_path}/{v_file_date}/qualifying',multiLine=True, schema= qualifying_schema)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ###Step - 3 : Rename and Add ingestion date

# COMMAND ----------

from pyspark.sql.functions import current_timestamp,lit
qualifying_final_df = qualifying_df.withColumnRenamed('qualifyId','qualifying_Id') \
.withColumnRenamed('raceId','race_Id') \
.withColumnRenamed('driverId','driver_Id') \
.withColumnRenamed('constructorId','constructor_Id') \
.withColumn('data_source',lit(v_data_source)) \
.withColumn('file_date',lit(v_file_date))

qualifying_final_df = add_ingestion_date(qualifying_final_df)

# COMMAND ----------

# MAGIC %md 
# MAGIC ###Step - 4 : Write data to datalake in parquet(Columnar Object Format)

# COMMAND ----------

incremental_load_delta('f1_processed','qualifying',processed_folder_path,qualifying_final_df,'race_Id',"tgt.race_Id = src.race_Id AND tgt.qualifying_Id = src.qualifying_Id")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ###Read and Check for verification

# COMMAND ----------

# spark.read.parquet(f'{processed_folder_path}/qualifying').display()

# COMMAND ----------

dbutils.notebook.exit("Success")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT race_Id,count(1)
# MAGIC FROM f1_processed.qualifying
# MAGIC GROUP BY race_Id
# MAGIC ORDER BY race_Id DESC;
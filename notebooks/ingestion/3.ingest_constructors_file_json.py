# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC #Ingest Constructors File

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ###Step - 0 : Define the DDL based Schema
# MAGIC ###Step - 1 : Read the JSON file using the spark dataframe reader
# MAGIC

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

dbutils.widgets.text("p_data_source","")
v_data_source = dbutils.widgets.get("p_data_source")

dbutils.widgets.text("p_file_date","2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

constructors_schema = "constructorId INT, constructorRef String, name String, nationality String, url String"

# COMMAND ----------

constructors_df = spark.read.json(f'{raw_folder_path}/{v_file_date}/constructors.json',schema=constructors_schema)

# COMMAND ----------

# MAGIC %md
# MAGIC ###Step - 2 : Drop unnecessary Columns

# COMMAND ----------

constructors_dropped_df = constructors_df.drop("url")
# constructors_dropped_df = constructors_df.drop(constructors_df.url)
# constructors_dropped_df = constructors_df.drop(col("url"))


# COMMAND ----------

# MAGIC %md
# MAGIC ###Step - 3 : Rename and Add ingestion Date

# COMMAND ----------

from pyspark.sql.functions import current_timestamp,lit
constructors_final_df = constructors_dropped_df.withColumnRenamed("constructorId","constructor_id") \
.withColumnRenamed("constructorRef","constructor_ref") \
.withColumn("data_source",lit(v_data_source)) \
.withColumn("file_date",lit(v_file_date))
constructors_final_df = add_ingestion_date(constructors_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ###Step - 4 : Write it in parquet format

# COMMAND ----------

constructors_final_df.write.mode('overwrite').format('delta').saveAsTable('f1_processed.constructors')

# COMMAND ----------

# MAGIC %md 
# MAGIC ###Read and check for the creation
# MAGIC

# COMMAND ----------

# spark.read.parquet(f'{processed_folder_path}/constructors').display()

# COMMAND ----------

# dbutils.notebook.exit("Success")

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT *
# MAGIC FROM f1_processed.constructors
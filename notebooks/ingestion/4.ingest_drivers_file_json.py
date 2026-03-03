# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC #Ingest Drivers File

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

# MAGIC %md
# MAGIC
# MAGIC ###Step - 0 : Define the DDL based Schema
# MAGIC ###Step - 1 : Read the JSON file using the spark dataframe reader
# MAGIC

# COMMAND ----------

from pyspark.sql.types import StructType, \
                              StructField, \
                              IntegerType, \
                              StringType, \
                              DateType

name_schema = StructType(fields = [
    StructField("forename",StringType(),True),
    StructField("surname",StringType(),True)
])
drivers_schema = StructType(fields = [
    StructField("driverId",IntegerType(),False),
    StructField("driverRef",StringType(),True),
    StructField("number",IntegerType(),True),
    StructField("code",StringType(),True),
    StructField("name",name_schema),
    StructField("dob",DateType(),True),
    StructField("nationality",StringType(),True),
    StructField("url",StringType(),True)
])

# COMMAND ----------

drivers_df = spark.read.json(f'{raw_folder_path}/{v_file_date}/drivers.json',schema=drivers_schema)

# COMMAND ----------

# MAGIC %md
# MAGIC ###Step - 2 : Rename and Add ingestion Date

# COMMAND ----------

from pyspark.sql.functions import current_timestamp,col,concat,lit
drivers_with_columns_df = drivers_df.withColumnRenamed("driverId","driver_Id") \
.withColumnRenamed("driverRef","driver_Ref") \
.withColumn("name", concat(col("name.forename"), lit(" "), col("name.surname"))) \
.withColumn("data_source",lit(v_data_source)) \
.withColumn("file_date",lit(v_file_date))

drivers_with_columns_df = add_ingestion_date(drivers_with_columns_df)
# drivers_with_columns_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###Step - 3 : Drop unnecessary Columns

# COMMAND ----------

drivers_final_df = drivers_with_columns_df.drop("url")


# COMMAND ----------

# MAGIC %md
# MAGIC ###Step - 4 : Write it in parquet format

# COMMAND ----------

drivers_final_df.write.mode('overwrite').format('delta').saveAsTable('f1_processed.drivers')

# COMMAND ----------

# MAGIC %md 
# MAGIC ###Read and check for the creation
# MAGIC

# COMMAND ----------

spark.read.format('delta').load(f'{processed_folder_path}/drivers').display()

# COMMAND ----------

dbutils.notebook.exit("Success")
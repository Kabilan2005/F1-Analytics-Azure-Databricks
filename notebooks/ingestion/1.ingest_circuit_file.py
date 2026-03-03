# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC #Ingest Circuits File

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

dbutils.widgets.text("p_data_source","")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

dbutils.widgets.text("p_file_date","")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ###Step - 1: Read csv file using the spark dataframe reader 
# MAGIC ###Step - 2: Define the required Schema
# MAGIC ###Step - 3: Use header = True for correct headers

# COMMAND ----------

from pyspark.sql.types import   StructType, \
                                StructField, \
                                IntegerType, \
                                StringType, \
                                DoubleType
circuits_schema = StructType(fields = [StructField('circuitId',IntegerType(),nullable=False),
                                       StructField('circuitRef',StringType(),True),
                                       StructField('name',StringType(),True),
                                       StructField('location',StringType(),True),
                                       StructField('country',StringType(),True),
                                       StructField('lat',DoubleType(),True),
                                       StructField('lon',DoubleType(),True),
                                       StructField('alt',IntegerType(),True),
                                       StructField('url',StringType(),True)])

# COMMAND ----------

circuits_df = spark.read.csv(f'{raw_folder_path}/{v_file_date}/circuits.csv',schema=circuits_schema,header=True)

# COMMAND ----------

# MAGIC %md
# MAGIC ###Step - 4 : Select the required columns

# COMMAND ----------

circuits_selected_df_list = circuits_df.select(['circuitId','circuitRef','name','location','country','lat','lon','alt'])
circuits_selected_df_name = circuits_df.select(circuits_df.circuitId,circuits_df.circuitRef,circuits_df.name,circuits_df.location,circuits_df.country,circuits_df.lat,circuits_df.lon,circuits_df.alt)
circuits_selected_df_index = circuits_df.select(circuits_df['circuitId'],circuits_df['circuitRef'],circuits_df['country'],circuits_df['alt'],
                                                circuits_df['lon'],circuits_df['lat'],circuits_df['name'],circuits_df['location'],circuits_df['circuitRef'])

# COMMAND ----------

from pyspark.sql.functions import col
circuits_selected_df = circuits_df.select(col('circuitId'),col('circuitRef'),col('name'),col('location'),col('country'),col('lat'),col('lon'),col('alt'))


# COMMAND ----------

# MAGIC %md
# MAGIC - ###Step - 5 : Renaming for the convention standard

# COMMAND ----------

circuits_renamed_df = circuits_selected_df.withColumnRenamed("circuitId","circuit_Id") \
    .withColumnRenamed('circuitRef','circuit_Ref') \
    .withColumnRenamed('lat','latitude') \
    .withColumnRenamed('lon','longitude') \
    .withColumnRenamed('alt','altitude')

# COMMAND ----------

# MAGIC %md
# MAGIC > ### Step - 6 : Add new column Ingestion_Date

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit
circuits_final_df = add_ingestion_date(circuits_renamed_df).withColumn("data_source",lit(v_data_source)) \
.withColumn("file_date",lit(v_file_date))

# COMMAND ----------

# MAGIC %md
# MAGIC ###Step - 7 : To make a value as columnObject use lit()

# COMMAND ----------

# MAGIC %md
# MAGIC ###Step - 8 : Write data to datalake in parquet(Columnar Object Format)

# COMMAND ----------

circuits_final_df.write.mode('overwrite').format('delta').saveAsTable('f1_processed.circuits')

# COMMAND ----------

# MAGIC %md
# MAGIC ###Read and Check for the creation

# COMMAND ----------

spark.read.format('delta').load(f'{processed_folder_path}/circuits').display()

# COMMAND ----------

dbutils.notebook.exit("Success")
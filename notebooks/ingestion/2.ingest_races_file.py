# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC #Ingest Races File

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ###Step - 1: Read csv file using the spark dataframe reader 
# MAGIC ###Step - 2: Define the required Schema
# MAGIC ###Step - 3: Use header = True for correct headers

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

from pyspark.sql.types import StructType, \
                            StructField, \
                            IntegerType, \
                            StringType, \
                            DateType, \
                            TimeType
races_schema = StructType (fields = [StructField('raceId',IntegerType(),False),
                                    StructField('year',IntegerType(),True),
                                    StructField('round',IntegerType(),True),
                                    StructField('circuitId',IntegerType(),True),
                                    StructField('name',StringType(),True),
                                    StructField('date',DateType(),True),
                                    StructField('time',StringType(),True),
                                    StructField('url',StringType(),True)])
                                                        

# COMMAND ----------

races_df = spark.read.csv(f'{raw_folder_path}/{v_file_date}/races.csv',header=True,schema=races_schema)

# COMMAND ----------

# MAGIC %md
# MAGIC ###Step - 4 : Select the required columns

# COMMAND ----------

from pyspark.sql.functions import col
races_selected_df = races_df.select(col('raceId'),col('year'),col('round'),col('circuitId'),col('name'),col('date'),col('time'))

# COMMAND ----------

# MAGIC %md
# MAGIC - ###Step - 5 : Renaming for the convention standard

# COMMAND ----------

races_renamed_df = races_selected_df.withColumnRenamed('circuitId','circuit_Id') \
    .withColumnRenamed('raceId','race_Id')

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Step - 6 : Replace \N in Timestamp with 00:00:00
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import col, when
races_modified_df = races_renamed_df.withColumn('time',
                                                when(col('time')=='\\N','00:00:00')
                                                .otherwise(col('time')))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step - 7 : Add new column Ingestion Date and Combine Date and Time to form timestamp

# COMMAND ----------

from pyspark.sql.functions import lit,current_timestamp,to_timestamp,concat
races_final_df = races_modified_df\
    .withColumn('race_timestamp',to_timestamp(concat(col('date'),lit(' '),col('time')),'yyyy-MM-dd HH:mm:ss'))\
    .withColumn("data_source",lit(v_data_source)) \
    .withColumn("file_date",lit(v_file_date))
races_final_df = add_ingestion_date(races_final_df).select(col('race_Id'),col('year').alias('race_year'),col('round'),col('circuit_Id'),col('name'),col('race_timestamp'),col('ingestion_date'),col('data_source'),col('file_date'))

# COMMAND ----------

# MAGIC %md
# MAGIC ###Step - 8 : Write data to datalake in parquet(Columnar Object Format)

# COMMAND ----------

# MAGIC %md
# MAGIC ###Read and Check for the creation

# COMMAND ----------

# MAGIC %md
# MAGIC ###Partitioning Based on Year

# COMMAND ----------

races_final_df.write.mode('overwrite').partitionBy('race_year').format('delta').saveAsTable('f1_processed.races')

# COMMAND ----------

spark.read.format('delta').load(f'{processed_folder_path}/races').display()

# COMMAND ----------

dbutils.notebook.exit("Success")
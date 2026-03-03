# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC #Ingest Results File

# COMMAND ----------

# %sql
# DROP TABLE f1_processed.results;

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ###Step - 1: Read json file using the spark dataframe reader 
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

from pyspark.sql.types import StructType, \
                            StructField, \
                            IntegerType, \
                            StringType, \
                            FloatType

results_schema = StructType(fields = [
    StructField('resultId',IntegerType(),False),
    StructField('raceId',IntegerType(),True),
    StructField('driverId',IntegerType(),True),
    StructField('constructorId',IntegerType(),True),
    StructField('number',IntegerType(),True),
    StructField('grid',IntegerType(),True),
    StructField('position',IntegerType(),True),
    StructField('positionText',StringType(),True),
    StructField('positionOrder',IntegerType(),True),
    StructField('points',FloatType(),True),
    StructField('laps',IntegerType(),True),
    StructField('time',StringType(),True),
    StructField('milliseconds',IntegerType(),True),
    StructField('fastestLap',IntegerType(),True), 
    StructField('rank',IntegerType(),True),
    StructField('fastestLapTime',StringType(),True),
    StructField('fastestLapSpeed',StringType(),True),
    StructField('statusId',IntegerType(),True)
])

# COMMAND ----------

# MAGIC %md
# MAGIC ###Step - 3 : Read the json file

# COMMAND ----------

results_df = spark.read.json(f'{raw_folder_path}/{v_file_date}/results.json',schema=results_schema)
# results_df.printSchema()
# results_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###Step - 4 : Rename and Add required Columns

# COMMAND ----------

from pyspark.sql.functions import current_timestamp,lit
results_renamed_df = results_df.withColumnRenamed('resultId','result_id') \
.withColumnRenamed('raceId','race_id') \
.withColumnRenamed('driverId','driver_id') \
.withColumnRenamed('constructorId','constructor_id') \
.withColumnRenamed('positionText','position_text') \
.withColumnRenamed('positionOrder','position_order') \
.withColumnRenamed('fastestLap','fastest_lap') \
.withColumnRenamed('fastestLapTime','fastest_lap_time') \
.withColumnRenamed('fastestLapSpeed','fastest_lap_speed') \
.withColumn('data_source',lit(v_data_source)) \
.withColumn('file_date',lit(v_file_date))

results_renamed_df = add_ingestion_date(results_renamed_df)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ###Step - 5 : Drop the unnecessary columns

# COMMAND ----------

results_final_df = results_renamed_df.drop('statusId')

# COMMAND ----------

results_final_df = results_final_df.dropDuplicates(['driver_id','race_id'])

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ###Step - 6 : Write it to ADLS in parquet format with partitioned by race_id

# COMMAND ----------

# for race_id_list in results_final_df.select("race_id").distinct().collect():
#   if spark.catalog.tableExists('f1_processed.results'):
#     spark.sql(f"ALTER TABLE f1_processed.results DROP IF EXISTS PARTITION (race_id = {race_id_list.race_id})")

# COMMAND ----------

# results_final_df.write.mode('append').partitionBy('race_id').format('parquet').saveAsTable('f1_processed.results')

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ###Method - 2 using insertInto 

# COMMAND ----------

# columns = ['result_id','driver_id','constructor_id','number','grid','position','position_text','position_order','points','laps','time','milliseconds','fastest_lap','rank','fastest_lap_time','fastest_lap_speed','data_source','file_date','ingestion_date','race_id']

# COMMAND ----------

# incremental_load('f1_processed.results',results_final_df,columns,'race_id')

# COMMAND ----------

db_name = 'f1_processed'
table_name = 'results'
partition_col = 'race_id'

# COMMAND ----------

incremental_load_delta(db_name,table_name,processed_folder_path,results_final_df,partition_col,"tgt.result_id = src.result_id AND tgt.race_id = src.race_id")

# COMMAND ----------

# MAGIC %md
# MAGIC ###Read and Check for the creation

# COMMAND ----------

# spark.read.parquet(f'{processed_folder_path}/results').display()  

# COMMAND ----------

# dbutils.notebook.exit("Success")

# COMMAND ----------

df = spark.sql("""SELECT race_id,driver_id FROM f1_processed.results
                GROUP BY race_id,driver_id
                HAVING COUNT(1) > 1
                ORDER BY race_id,driver_id DESC
""")

# COMMAND ----------

print(df)
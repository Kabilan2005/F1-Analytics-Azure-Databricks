# Databricks notebook source
from pyspark.sql.functions import year,to_date,col,current_timestamp

# COMMAND ----------

dbutils.widgets.text('p_file_date','')
v_file_date = dbutils.widgets.get('p_file_date')

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

races_df = spark.read.format('delta').load(f'{processed_folder_path}/races')

# COMMAND ----------


races__req_df = races_df.withColumn('race_year',year('race_timestamp')) \
.withColumn('race_date',to_date('race_timestamp')) \
.select(['race_Id','circuit_Id','race_year','race_date','name']) \
.withColumnRenamed('name','race_name')

# COMMAND ----------

circuits_df = spark.read.format('delta').load(f'{processed_folder_path}/circuits').select(['circuit_Id','location'])

# COMMAND ----------

races_circuits_joined = races__req_df.join(circuits_df,races__req_df.circuit_Id == circuits_df.circuit_Id, 'inner').select(['race_Id','race_year','race_name','race_date','location'])

# COMMAND ----------

races_circuits_final_df = races_circuits_joined.withColumnRenamed('location','circuit_location')

# COMMAND ----------

results_df = spark.read.format('delta').load(f'{processed_folder_path}/results')\
.filter(f"file_date='{v_file_date}'") \
.select(['race_id','driver_id','constructor_id','grid','points','fastest_lap','time','position','file_date']) \
.withColumnRenamed('time','race_time') \
.withColumnRenamed('race_id','result_race_id') \
.withColumnRenamed('file_date','result_file_date')

# COMMAND ----------

drivers_df = spark.read.format('delta').load(f'{processed_folder_path}/drivers').select(['driver_Id','name','number','nationality']) \
.withColumnRenamed('name','driver_name') \
.withColumnRenamed('number','driver_number') \
.withColumnRenamed('nationality','driver_nationality')

# COMMAND ----------

constructors_df = spark.read.format('delta').load(f'{processed_folder_path}/constructors').select(['constructor_Id','name']) \
.withColumnRenamed('name','team')

# COMMAND ----------

# Joining Results with Drivers and Constructors
results_step_1_df = results_df.join(drivers_df, results_df.driver_id == drivers_df.driver_Id, 'inner') \
.join(constructors_df, results_df.constructor_id == constructors_df.constructor_Id, 'inner')

# COMMAND ----------

# Joining Races_Circuits to form the final Dataset
results_final_df = results_step_1_df.join(races_circuits_final_df, results_step_1_df.result_race_id == races_circuits_final_df.race_Id, 'inner') \
.withColumn('creation_date',current_timestamp()) \
.select(['race_Id','race_year','race_name','race_date','circuit_location','driver_name','driver_number','driver_nationality','team','grid','points','position','fastest_lap','race_time','creation_date','result_file_date']) \
.withColumnRenamed('result_file_date','file_date')

# COMMAND ----------

results_final_df.display()

# COMMAND ----------

incremental_load_delta('f1_presentation','race_results',presentation_folder_path,results_final_df,'race_Id',"tgt.race_Id = src.race_Id AND tgt.driver_name = src.driver_name")

# COMMAND ----------

# spark.read.parquet(f'{presentation_folder_path}/race_results').display()

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT race_Id,COUNT(1)
# MAGIC FROM f1_presentation.race_results
# MAGIC GROUP BY race_Id
# MAGIC ORDER BY race_Id DESC;

# COMMAND ----------


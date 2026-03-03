# Databricks notebook source
from pyspark.sql.functions import sum,count,when,col,rank,desc,concat_ws,collect_set
from pyspark.sql.window import Window

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

dbutils.widgets.text("p_file_date","")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

race_results_list = spark.read.format('delta').load(f'{presentation_folder_path}/race_results')\
.filter(f"file_date = '{v_file_date}'") \
.select("race_year") \
.distinct() \
.collect()

# COMMAND ----------

race_year_list = list()
for race_year in race_results_list:
  race_year_list.append(race_year.race_year)
race_year_list.sort()
# race_year_list

# COMMAND ----------

race_results_df = spark.read.format('delta').load(f'{presentation_folder_path}/race_results')\
.filter(col("race_year").isin(race_year_list))

# COMMAND ----------

driver_standings_df = race_results_df \
.groupBy('race_year','driver_name','driver_nationality') \
.agg(sum('points').alias('total_points'),
     count(when(col('position') == 1,True)).alias('wins'),
     concat_ws(', ', collect_set('team')).alias('teams'))

# COMMAND ----------

driver_rank_spec = Window.partitionBy('race_year').orderBy(desc('total_points'),desc("wins"))
driver_ranked_df = driver_standings_df.withColumn("rank",rank().over(driver_rank_spec))

# COMMAND ----------

incremental_load_delta('f1_presentation','driver_standings',presentation_folder_path,driver_ranked_df,'race_year',"tgt.race_year = src.race_year AND tgt.driver_name = src.driver_name")

# COMMAND ----------

# MAGIC %sql
# MAGIC select *
# MAGIC from f1_presentation.driver_standings
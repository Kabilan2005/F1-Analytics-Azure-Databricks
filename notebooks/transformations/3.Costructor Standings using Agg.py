# Databricks notebook source
from pyspark.sql.window import Window
from pyspark.sql.functions import rank,count,when,sum,desc,col

# COMMAND ----------

dbutils.widgets.text("p_file_date","")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

results_list = spark.read.format('delta').load(f'{presentation_folder_path}/race_results')\
.filter(f"file_date = '{v_file_date}'") \
.select(col('race_year')) \
.distinct() \
.collect()

# COMMAND ----------

race_year_list = list()
for race_row in results_list:
  race_year_list.append(race_row.race_year)
race_year_list.sort()
race_year_list

# COMMAND ----------

results_df = spark.read.format('delta').load(f'{presentation_folder_path}/race_results')\
.filter(col("race_year").isin(race_year_list))

# COMMAND ----------

constructors_grouped_df = results_df.groupBy('race_year','team') \
.agg(sum('points').alias('total_points'),count(when(col('position') == 1, True)).alias('wins'))

# COMMAND ----------

constructors_rank_spec = Window.partitionBy('race_year').orderBy(desc('total_points'),desc('wins'))
constructors_ranked_df = constructors_grouped_df.withColumn('rank',rank().over(constructors_rank_spec))
display(constructors_ranked_df)

# COMMAND ----------

incremental_load_delta('f1_presentation','constructor_standings',presentation_folder_path,constructors_ranked_df,'race_year',"tgt.race_year = src.race_year AND tgt.team = src.team")

# COMMAND ----------

# MAGIC %sql
# MAGIC Select *
# MAGIC from f1_presentation.constructor_standings
# MAGIC WHERE race_year = 2021
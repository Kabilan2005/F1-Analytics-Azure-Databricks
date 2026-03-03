# Databricks notebook source
v_result = dbutils.notebook.run("1.ingest_circuit_file", 0, {"p_data_source":"Ergast_API","p_file_date":"2021-03-28"})
v_result

# COMMAND ----------

v_result = dbutils.notebook.run("2.ingest_races_file", 0, {"p_data_source":"Ergast_API","p_file_date":"2021-03-28"})
v_result

# COMMAND ----------

v_result = dbutils.notebook.run("3.ingest_constructors_file_json", 0, {"p_data_source":"Ergast_API","p_file_date":"2021-03-28"})
v_result

# COMMAND ----------

v_result = dbutils.notebook.run("4.ingest_drivers_file_json", 0, {"p_data_source":"Ergast_API","p_file_date":"2021-03-28"})
v_result

# COMMAND ----------

v_result = dbutils.notebook.run("5.igest_results_json_file", 0, {"p_data_source":"Ergast_API","p_file_date":"2021-03-28"})
v_result

# COMMAND ----------

v_result = dbutils.notebook.run("6.ingest_pitstops_multi_line_json_file", 0, {"p_data_source":"Ergast_API","p_file_date":"2021-03-28"})
v_result

# COMMAND ----------

v_result = dbutils.notebook.run("7.ingest_lap_times_file", 0, {"p_data_source":"Ergast_API","p_file_date":"2021-03-28"})
v_result

# COMMAND ----------

v_result = dbutils.notebook.run("8.ingest_qualifying_files", 0, {"p_data_source":"Ergast_API","p_file_date":"2021-03-28"})
v_result
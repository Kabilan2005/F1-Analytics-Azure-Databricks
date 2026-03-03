# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC #Ingest Pitstops File

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ###Step - 1: Read json file using the spark dataframe reader with multiline set true
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

pitstops_schema = StructType(fields = [
    StructField("raceId", IntegerType(), False),
    StructField("driverId", IntegerType(), False),
    StructField("stop", IntegerType(), False),
    StructField("lap", IntegerType(), False),
    StructField("time", StringType(), False),
    StructField("duration", StringType(), True),
    StructField("milliseconds", IntegerType(), True)
])

# COMMAND ----------

pitstops_df = spark.read.json(f'{raw_folder_path}/{v_file_date}/pit_stops.json', schema = pitstops_schema,multiLine=True)

# COMMAND ----------

# MAGIC %md
# MAGIC ###Step - 4 : Rename and Add required Columns

# COMMAND ----------

from pyspark.sql.functions import current_timestamp,lit,col

pitstops_df = pitstops_df.withColumn(
    "milliseconds",
    col("milliseconds").cast("int")
)

pitstops_final_df = add_ingestion_date(pitstops_df).withColumnRenamed('raceId','race_id') \
.withColumnRenamed('driverId','driver_id') \
.withColumn('data_source',lit(v_data_source)) \
.withColumn('file_date',lit(v_file_date))

# COMMAND ----------

columns = pitstops_final_df.columns

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ###Step - 5 : Write it to ADLS in parquet format

# COMMAND ----------

incremental_load_delta('f1_processed','pitstops',processed_folder_path,pitstops_final_df,'race_id',"tgt.race_id = src.race_id AND tgt.driver_id = src.driver_id AND tgt.stop = src.stop")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ###Read and check for creation

# COMMAND ----------

# spark.read.parquet(f'{processed_folder_path}/pitstops').display()

# COMMAND ----------

dbutils.notebook.exit("Success")

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT race_id,COUNT(1)
# MAGIC FROM f1_processed.pitstops
# MAGIC GROUP BY race_id
# MAGIC ORDER BY race_id DESC;
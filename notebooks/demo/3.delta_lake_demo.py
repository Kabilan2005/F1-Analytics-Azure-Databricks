# Databricks notebook source
# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS f1_demo
# MAGIC LOCATION 'abfss://demo@f1storageaccforanalytics.dfs.core.windows.net/'

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

results_df = spark.read.json(f"{raw_folder_path}/2021-03-28/results.json",schema=results_schema)

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS f1_demo.results_managed;

# COMMAND ----------

results_df.write.mode('overwrite').format("delta").saveAsTable('f1_demo.results_managed')

# COMMAND ----------

# MAGIC %sql
# MAGIC create table if not exists f1_demo.results_external
# MAGIC USING DELTA
# MAGIC LOCATION 'abfss://demo@f1storageaccforanalytics.dfs.core.windows.net/results_external'

# COMMAND ----------

results_df.write.mode('overwrite').format("delta").partitionBy('constructorid').saveAsTable('f1_demo.results_partitions')

# COMMAND ----------

from pyspark.sql.functions import col
results_df.withColumn('position', col('position').cast('int'))

# COMMAND ----------

from delta.tables import DeltaTable

deltaTable = DeltaTable.forPath(spark, f'{demo_folder_path}/results_managed')

deltaTable.update(
  condition = "position <= 10",
  set = { "points": "21 - position" }
)

# COMMAND ----------

from delta.tables import DeltaTable

deltaTable = DeltaTable.forPath(spark, f'{demo_folder_path}/results_managed')

deltaTable.delete("points = 0")

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

drivers_day1_df = spark.read.json(f'{raw_folder_path}/2021-03-28/drivers.json', schema=drivers_schema) \
.filter("driverId <= 10") \
.select("driverId","dob",col("name.forename").alias("forename"),col("name.surname").alias("surname"))

# COMMAND ----------

drivers_day1_df.createOrReplaceTempView("drivers_day1")

# COMMAND ----------

from pyspark.sql.functions import upper
drivers_day2_df = spark.read.json(f'{raw_folder_path}/2021-03-28/drivers.json', schema=drivers_schema) \
.filter("driverId BETWEEN 6 and 15") \
.select("driverId","dob",upper("name.forename").alias("forename"),upper("name.surname").alias("surname"))

# COMMAND ----------

drivers_day2_df.createOrReplaceTempView("drivers_day2")

# COMMAND ----------

from pyspark.sql.functions import upper
drivers_day3_df = spark.read.json(f'{raw_folder_path}/2021-03-28/drivers.json', schema=drivers_schema) \
.filter("driverId BETWEEN 1 and 5 OR driverId BETWEEN 16 and 20") \
.select("driverId","dob",upper("name.forename").alias("forename"),upper("name.surname").alias("surname"))

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS f1_demo.drivers_merge (
# MAGIC   driverId INT,
# MAGIC   dob DATE,
# MAGIC   forename STRING,
# MAGIC   surname STRING,
# MAGIC   createdDate DATE,
# MAGIC   updatedDate DATE
# MAGIC )
# MAGIC USING DELTA

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO f1_demo.drivers_merge ddm
# MAGIC USING drivers_day1 dd1
# MAGIC ON ddm.driverId = dd1.driverId
# MAGIC WHEN MATCHED
# MAGIC THEN UPDATE SET ddm.dob = dd1.dob,
# MAGIC                 ddm.forename = dd1.forename,
# MAGIC                 ddm.surname = dd1.surname,
# MAGIC                 ddm.updatedDate = current_timestamp()
# MAGIC WHEN NOT MATCHED
# MAGIC THEN INSERT (driverId, dob, forename, surname, createdDate)
# MAGIC VALUES (dd1.driverId, dd1.dob, dd1.forename, dd1.surname, current_timestamp())

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO f1_demo.drivers_merge ddm
# MAGIC USING drivers_day2 dd2
# MAGIC ON ddm.driverId = dd2.driverId
# MAGIC WHEN MATCHED
# MAGIC THEN UPDATE SET ddm.dob = dd2.dob,
# MAGIC                 ddm.forename = dd2.forename,
# MAGIC                 ddm.surname = dd2.surname,
# MAGIC                 ddm.updatedDate = current_timestamp()
# MAGIC WHEN NOT MATCHED
# MAGIC THEN INSERT (driverId, dob, forename, surname, createdDate)
# MAGIC VALUES (dd2.driverId, dd2.dob, dd2.forename, dd2.surname, current_timestamp())

# COMMAND ----------

from delta.tables import DeltaTable
from pyspark.sql.functions import current_timestamp

deltaTable = DeltaTable.forPath(spark, f'{demo_folder_path}/drivers_merge')

deltaTable.alias("tgt").merge(
    drivers_day3_df.alias("upd"),
    "tgt.driverId = upd.driverId") \
.whenMatchedUpdate(set = { "dob": "upd.dob", "forename": "upd.forename", "surname": "upd.surname","updatedDate":current_timestamp()}) \
.whenNotMatchedInsert(values = {
    "driverId": "upd.driverId",
    "dob": "upd.dob", "forename": "upd.forename", "surname": "upd.surname","createdDate":current_timestamp()
}) \
.execute()

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY f1_demo.drivers_merge

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM f1_demo.drivers_merge
# MAGIC VERSION AS OF 2;

# COMMAND ----------

spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "false")

# COMMAND ----------

# MAGIC %sql
# MAGIC VACUUM f1_demo.drivers_merge RETAIN 0 HOURS;

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO f1_demo.drivers_merge tgt
# MAGIC USING (SELECT * FROM f1_demo.drivers_merge VERSION AS OF 9) src
# MAGIC ON tgt.driverId = src.driverId
# MAGIC WHEN NOT MATCHED THEN INSERT *;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS f1_demo.txn (
# MAGIC   driverId INT,
# MAGIC   dob DATE,
# MAGIC   forename STRING,
# MAGIC   surname STRING,
# MAGIC   createdDate DATE,
# MAGIC   updatedDate DATE
# MAGIC )
# MAGIC USING DELTA

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO f1_demo.txn SELECT * FROM f1_demo.drivers_merge WHERE driverId = 1;
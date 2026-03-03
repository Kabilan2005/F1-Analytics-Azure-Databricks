# Databricks notebook source
spark.conf.set("fs.azure.account.auth.type.f1storageaccforanalytics.dfs.core.windows.net", "SAS")
spark.conf.set("fs.azure.sas.token.provider.type.f1storageaccforanalytics.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
spark.conf.set("fs.azure.sas.fixed.token.f1storageaccforanalytics.dfs.core.windows.net",dbutils.secrets.get(scope = 'Formula1Scope', key = 'F1-SAS-raw'))

# COMMAND ----------

display(dbutils.fs.ls("abfss://raw@f1storageaccforanalytics.dfs.core.windows.net"))

# COMMAND ----------

spark.read.csv("abfss://raw@f1storageaccforanalytics.dfs.core.windows.net/circuits.csv").show()
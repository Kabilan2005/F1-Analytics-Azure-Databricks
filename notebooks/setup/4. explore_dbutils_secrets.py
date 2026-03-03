# Databricks notebook source
dbutils.secrets.help()

# COMMAND ----------

dbutils.secrets.listScopes()

# COMMAND ----------

dbutils.secrets.list(scope = 'Formula1Scope')

# COMMAND ----------

dbutils.secrets.get(scope = 'Formula1Scope', key = 'F1SAS')
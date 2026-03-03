# Databricks notebook source
print("Hello")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT SUM(timestampdiff(second,usage_start_time,usage_end_time))/3600 as hours
# MAGIC FROM system.billing.usage
# MAGIC GROUP BY usage_metadata.cluster_id
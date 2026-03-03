# Databricks notebook source
display(dbutils.fs.ls("abfss://demo@f1storageaccforanalytics.dfs.core.windows.net"))
spark.read.csv("abfss://demo@f1storageaccforanalytics.dfs.core.windows.net/circuits.csv").show()
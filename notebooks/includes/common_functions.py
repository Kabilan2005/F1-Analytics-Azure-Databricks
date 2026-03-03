# Databricks notebook source
from pyspark.sql.functions import current_timestamp

def add_ingestion_date(input_df):
    output_df = input_df.withColumn('ingestion_date',current_timestamp())
    return output_df

# COMMAND ----------

def rearrange_partition_col(cols_list,partition_col):
    output_cols_list = list()
    for col in cols_list:
        if col != partition_col:
            output_cols_list.append(col)
    output_cols_list.append(partition_col)
    # print(output_cols_list)
    return output_cols_list

# COMMAND ----------

def incremental_load(table_name,input_df,columns_list,partition_col):
    final_df = input_df.select(rearrange_partition_col(columns_list,partition_col))
    spark.conf.set("spark.sql.sources.partitionOverwriteMode","dynamic")
    if spark.catalog.tableExists(table_name):
        final_df.write.mode('overwrite').insertInto(table_name)
    else:
        final_df.write.mode('overwrite').partitionBy(partition_col).format('parquet').saveAsTable(table_name)

# COMMAND ----------

def incremental_load_delta(db_name,table_name,processed_folder_path,final_df,partition_col,merge_condition):
    spark.conf.set("spark.databricks.optimizer.dynamicPartitionPruning.enabled","true")
    from delta.tables import DeltaTable
    if spark.catalog.tableExists(f'{db_name}.{table_name}'):
        deltatable = DeltaTable.forPath(spark,f'{processed_folder_path}/{table_name}')
        deltatable.alias("tgt").merge(
            final_df.alias("src"),
            merge_condition
        )\
        .whenMatchedUpdateAll()\
        .whenNotMatchedInsertAll()\
        .execute()
    else:
        final_df.write.mode('overwrite').partitionBy(partition_col).format('delta').saveAsTable(f"{db_name}.{table_name}")
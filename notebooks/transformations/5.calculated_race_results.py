# Databricks notebook source
dbutils.widgets.text("p_file_date","")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

spark.sql("""
          CREATE TABLE IF NOT EXISTS f1_presentation.calculated_race_results (
              race_year INT,
              team STRING,
              driver_id INT,
              driver_name STRING,
              race_id INT,
              points INT,
              position INT,
              calculated_points INT,
              created_date TIMESTAMP,
              updated_date TIMESTAMP
          )
          USING DELTA 
          """)

# COMMAND ----------

spark.sql(f"""
          CREATE OR REPLACE TEMP VIEW race_results_update AS (
                SELECT rc.race_year as race_year,
                    rc.race_id as race_id,
                    d.driver_id as driver_id,
                    c.name as team,
                    d.name as driver_name,
                    r.position,
                    r.points,
                    11 - r.position as calculated_points
                FROM f1_processed.results as r
                JOIN f1_processed.drivers d ON r.driver_id = d.driver_id
                JOIN f1_processed.constructors c ON r.constructor_id = c.constructor_id
                JOIN f1_processed.races rc ON r.race_id = rc.race_id
                WHERE r.position <=10 AND r.file_date = '{v_file_date}'
          )""")

# COMMAND ----------

spark.sql(f"""MERGE INTO f1_presentation.calculated_race_results tgt
USING race_results_update src
ON tgt.driver_id = src.driver_id AND tgt.race_id = src.race_id
WHEN MATCHED
THEN UPDATE SET tgt.position = src.position,
                tgt.points = src.points,
                tgt.calculated_points = src.calculated_points,
                tgt.updated_date = current_timestamp()
WHEN NOT MATCHED
THEN INSERT (race_year, race_id, driver_id, team, driver_name, position, points, calculated_points,created_date)
VALUES (src.race_year, src.race_id, src.driver_id, src.team, src.driver_name, src.position, src.points, src.calculated_points, current_timestamp())""")

# COMMAND ----------


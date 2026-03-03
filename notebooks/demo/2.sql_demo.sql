-- Databricks notebook source
-- LIMIT OFFSET

-- COMMAND ----------

SHOW DATABASES;

-- COMMAND ----------

select * from f1_raw.drivers
limit 10
offset 5 

-- COMMAND ----------

use database f1_processed

-- COMMAND ----------

select driver_Ref || "-" || code as new_Ref 
FROM drivers

-- COMMAND ----------

select split(name,' ')[0] as forename, split(name,' ')[1] as surname
from drivers

-- COMMAND ----------

select *
from drivers

-- COMMAND ----------

select count(*) as cnt,nationality 
from drivers
group by nationality
having count(*) >100
order by cnt desc
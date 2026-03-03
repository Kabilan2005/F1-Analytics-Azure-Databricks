-- Databricks notebook source
-- MAGIC %python
-- MAGIC html = """<h1 style="color:orange;text-align:center;font-family:Arial;font-size:50px">Report on F1 Dominant Teams </h1>"""
-- MAGIC displayHTML(html)

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW v_dominant_teams AS
SELECT team,
       COUNT(1) as total_races,
       SUM(calculated_points) as total_points,
       AVG(calculated_points) as average_points,
       RANK() OVER(
          ORDER BY AVG(calculated_points) DESC
       ) as Rank
FROM f1_presentation.calculated_race_results
GROUP BY team
HAVING COUNT(1) >= 100
ORDER BY average_points DESC;

-- COMMAND ----------

SELECT race_year,
       team,
       COUNT(1) as total_races,
       SUM(calculated_points) as total_points,
       AVG(calculated_points) as average_points
FROM f1_presentation.calculated_race_results
WHERE team IN (SELECT team FROM v_dominant_teams WHERE Rank <= 5)
GROUP BY team,race_year
ORDER BY race_year,average_points DESC

-- COMMAND ----------

SELECT race_year,
       team,
       COUNT(1) as total_races,
       SUM(calculated_points) as total_points,
       AVG(calculated_points) as average_points
FROM f1_presentation.calculated_race_results
WHERE team IN (SELECT team FROM v_dominant_teams WHERE Rank <= 10)
GROUP BY team,race_year
ORDER BY race_year,average_points DESC

-- COMMAND ----------

SELECT race_year,
       team,
       COUNT(1) as total_races,
       SUM(calculated_points) as total_points,
       AVG(calculated_points) as average_points
FROM f1_presentation.calculated_race_results
WHERE team IN (SELECT team FROM v_dominant_teams WHERE Rank <= 10)
GROUP BY team,race_year
ORDER BY race_year,average_points DESC

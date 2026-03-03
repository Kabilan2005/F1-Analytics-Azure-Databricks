-- Databricks notebook source
SELECT team,
       COUNT(1) as total_races,
       SUM(calculated_points) as total_points,
       AVG(calculated_points) as average_points
FROM f1_presentation.calculated_race_results
-- WHERE race_year BETWEEN 2001 AND 2010
GROUP BY team
HAVING COUNT(1) >= 100
ORDER BY average_points DESC
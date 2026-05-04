USE team29_projectdb;
DROP TABLE IF EXISTS q4_results;
CREATE EXTERNAL TABLE q4_results(storm_year INT, avg_severity DOUBLE, total_damage DOUBLE)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LOCATION 'project/hive/warehouse/q4'; 

SET hive.resultset.use.unique.column.names = false;
INSERT OVERWRITE TABLE q4_results
SELECT YEAR(begin_date) as storm_year, AVG(severity), SUM(damage_property + damage_crops)
FROM storm_events_part_buck
WHERE YEAR(begin_date) <= YEAR(CURRENT_DATE)
GROUP BY YEAR(begin_date)
ORDER BY storm_year ASC;
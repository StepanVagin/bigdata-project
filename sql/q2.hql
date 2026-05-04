USE team29_projectdb;
DROP TABLE IF EXISTS q2_results;
CREATE EXTERNAL TABLE q2_results(severity_level INT, event_count BIGINT, avg_damage DOUBLE, total_deaths BIGINT)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LOCATION 'project/hive/warehouse/q2'; 

SET hive.resultset.use.unique.column.names = false;
INSERT OVERWRITE TABLE q2_results
SELECT severity, COUNT(*) AS event_count, AVG(damage_property + damage_crops) AS avg_damage, SUM(deaths_direct + deaths_indirect) AS total_deaths
FROM storm_events_part_buck
GROUP BY severity
ORDER BY severity ASC;
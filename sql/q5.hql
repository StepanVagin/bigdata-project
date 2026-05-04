USE team29_projectdb;
DROP TABLE IF EXISTS q5_results;
CREATE EXTERNAL TABLE q5_results(event_type STRING, total_deaths BIGINT, total_damage DOUBLE)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LOCATION 'project/hive/warehouse/q5'; 

SET hive.resultset.use.unique.column.names = false;
INSERT OVERWRITE TABLE q5_results
SELECT event_type, SUM(deaths_direct + deaths_indirect) as total_deaths, SUM(damage_property + damage_crops) as total_damage
FROM storm_events_part_buck
GROUP BY event_type
ORDER BY total_deaths DESC
LIMIT 15;
USE team29_projectdb;
DROP TABLE IF EXISTS q1_results;
CREATE EXTERNAL TABLE q1_results(event_type STRING, total_financial_damage DOUBLE)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LOCATION 'project/hive/warehouse/q1'; 

SET hive.resultset.use.unique.column.names = false;
INSERT OVERWRITE TABLE q1_results
SELECT event_type, SUM(damage_property + damage_crops) AS total_financial_damage
FROM storm_events_part_buck
GROUP BY event_type
ORDER BY total_financial_damage DESC
LIMIT 15;
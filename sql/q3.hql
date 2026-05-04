USE team29_projectdb;
DROP TABLE IF EXISTS q3_results;
CREATE EXTERNAL TABLE q3_results(state STRING, extreme_event_count BIGINT)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LOCATION 'project/hive/warehouse/q3'; 

SET hive.resultset.use.unique.column.names = false;
INSERT OVERWRITE TABLE q3_results
SELECT state, COUNT(*) as extreme_event_count
FROM storm_events_part_buck
WHERE severity = 4
GROUP BY state
ORDER BY extreme_event_count DESC
LIMIT 15;
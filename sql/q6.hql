USE team29_projectdb;
DROP TABLE IF EXISTS q6_results;
CREATE EXTERNAL TABLE q6_results(storm_month INT, event_count BIGINT)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LOCATION 'project/hive/warehouse/q6'; 

SET hive.resultset.use.unique.column.names = false;
INSERT OVERWRITE TABLE q6_results
SELECT MONTH(begin_date) as storm_month, COUNT(*)
FROM storm_events_part_buck
GROUP BY MONTH(begin_date)
ORDER BY storm_month ASC;
CREATE DATABASE IF NOT EXISTS team29_projectdb;
USE team29_projectdb;

DROP TABLE IF EXISTS storm_events_raw;
CREATE EXTERNAL TABLE storm_events_raw
STORED AS AVRO 
LOCATION 'project/warehouse/storm_events' 
TBLPROPERTIES ('avro.schema.url'='project/warehouse/avsc/storm_events.avsc');

DROP TABLE IF EXISTS storm_events_part_buck;
CREATE EXTERNAL TABLE storm_events_part_buck(
    event_id BIGINT,
    event_type STRING,
    damage_property DOUBLE,
    damage_crops DOUBLE,
    deaths_direct INT,
    deaths_indirect INT,
    injuries_direct INT,
    injuries_indirect INT,
    begin_date TIMESTAMP,
    end_date TIMESTAMP,
    s DOUBLE,
    severity INT
) 
PARTITIONED BY (state STRING) 
CLUSTERED BY (event_id) INTO 8 BUCKETS
STORED AS AVRO 
LOCATION 'project/hive/warehouse/storm_events_part_buck' 
TBLPROPERTIES ('AVRO.COMPRESS'='SNAPPY');

SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;

INSERT OVERWRITE TABLE storm_events_part_buck PARTITION(state)
SELECT 
    event_id, 
    event_type, 
    damage_property, 
    damage_crops, 
    deaths_direct, 
    deaths_indirect, 
    injuries_direct, 
    injuries_indirect, 
    from_unixtime(CAST(begin_date/1000 AS BIGINT)) as begin_date,
    from_unixtime(CAST(end_date/1000 AS BIGINT)) as end_date,
    s, 
    severity, 
    state 
FROM storm_events_raw;

DROP TABLE storm_events_raw;

SELECT 'SUCCESS' as status, count(*) as row_count FROM storm_events_part_buck;
SELECT * FROM storm_events_part_buck LIMIT 5;
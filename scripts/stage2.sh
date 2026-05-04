#!/bin/bash
password=$(head -n 1 secrets/.hive.pass)

hdfs dfs -mkdir -p project/warehouse/avsc
hdfs dfs -put -f output/*.avsc project/warehouse/avsc/

echo "Building tables..."
beeline -u jdbc:hive2://hadoop-03.uni.innopolis.ru:10001 -n team29 -p $password -f sql/db.hql > output/hive_results.txt 2> /dev/null

for i in {1..6}; do
    beeline -u jdbc:hive2://hadoop-03.uni.innopolis.ru:10001 -n team29 -p $password -f sql/q$i.hql > /dev/null 2>&1
done

echo "Running Q1..."
echo "event_type,total_financial_damage" > output/q1.csv
hdfs dfs -cat project/hive/warehouse/q1/* >> output/q1.csv
echo "Running Q2..."
echo "severity_level,event_count,avg_damage,total_deaths" > output/q2.csv
hdfs dfs -cat project/hive/warehouse/q2/* >> output/q2.csv
echo "Running Q3..."
echo "state,extreme_event_count" > output/q3.csv
hdfs dfs -cat project/hive/warehouse/q3/* >> output/q3.csv
echo "Running Q4..."
echo "storm_year,avg_severity,total_damage" > output/q4.csv
hdfs dfs -cat project/hive/warehouse/q4/* >> output/q4.csv
echo "Running Q5..."
echo "event_type,total_deaths,total_damage" > output/q5.csv
hdfs dfs -cat project/hive/warehouse/q5/* >> output/q5.csv
echo "Running Q6..."
echo "storm_month,event_count" > output/q6.csv
hdfs dfs -cat project/hive/warehouse/q6/* >> output/q6.csv
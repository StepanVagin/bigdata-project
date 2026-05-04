#!/bin/bash
password=$(head -n 1 secrets/.psql.pass)

hdfs dfs -rm -r -f /user/team29/project/warehouse || true
hdfs dfs -mkdir -p /user/team29/project/warehouse

sqoop import-all-tables --connect jdbc:postgresql://hadoop-04.uni.innopolis.ru/team29_projectdb --username team29 --password $password --compression-codec=snappy --compress --as-avrodatafile --warehouse-dir=project/warehouse --m 1

mkdir -p output
cp *.avsc output/ || true
cp *.java output/ || true

rm -f *.avsc *.java
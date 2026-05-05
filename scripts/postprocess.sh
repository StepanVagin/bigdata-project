#!/bin/bash
set -e

hive_pass=$(head -n 1 secrets/.hive.pass)
psql_pass=$(head -n 1 secrets/.psql.pass)
PG="psql -h hadoop-04.uni.innopolis.ru -U team29 -d team29_projectdb"
BEELINE="beeline -u jdbc:hive2://hadoop-03.uni.innopolis.ru:10001 -n team29 -p $hive_pass"

echo "Creating external Hive tables for stage III outputs..."
$BEELINE << 'EOF'
USE team29_projectdb;

DROP TABLE IF EXISTS model1_predictions;
CREATE EXTERNAL TABLE model1_predictions (
    label      DOUBLE,
    prediction DOUBLE
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION 'project/output/model1_predictions'
TBLPROPERTIES ('skip.header.line.count'='1');

DROP TABLE IF EXISTS model2_predictions;
CREATE EXTERNAL TABLE model2_predictions (
    label      DOUBLE,
    prediction DOUBLE
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION 'project/output/model2_predictions'
TBLPROPERTIES ('skip.header.line.count'='1');

DROP TABLE IF EXISTS ml_evaluation;
CREATE EXTERNAL TABLE ml_evaluation (
    model    STRING,
    f1       DOUBLE,
    accuracy DOUBLE
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION 'project/output/evaluation'
TBLPROPERTIES ('skip.header.line.count'='1');

DROP TABLE IF EXISTS feature_importance;
CREATE EXTERNAL TABLE feature_importance (
    feature    STRING,
    importance DOUBLE
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION 'project/output/feature_importance'
TBLPROPERTIES ('skip.header.line.count'='1');

DROP TABLE IF EXISTS cv_metrics;
CREATE EXTERNAL TABLE cv_metrics (
    model      STRING,
    best_cv_f1 DOUBLE
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION 'project/output/cv_metrics'
TBLPROPERTIES ('skip.header.line.count'='1');

SELECT 'model1_predictions' AS tbl, COUNT(*) AS cnt FROM model1_predictions
UNION ALL SELECT 'model2_predictions', COUNT(*) FROM model2_predictions
UNION ALL SELECT 'ml_evaluation',      COUNT(*) FROM ml_evaluation
UNION ALL SELECT 'feature_importance', COUNT(*) FROM feature_importance
UNION ALL SELECT 'cv_metrics',         COUNT(*) FROM cv_metrics;
EOF

echo "Creating ML result tables in PostgreSQL..."
PGPASSWORD=$psql_pass $PG << 'EOF'
DROP TABLE IF EXISTS model1_predictions;
CREATE TABLE model1_predictions (
    label      FLOAT,
    prediction FLOAT
);

DROP TABLE IF EXISTS model2_predictions;
CREATE TABLE model2_predictions (
    label      FLOAT,
    prediction FLOAT
);

DROP TABLE IF EXISTS ml_evaluation;
CREATE TABLE ml_evaluation (
    model    TEXT,
    f1       FLOAT,
    accuracy FLOAT
);

DROP TABLE IF EXISTS feature_importance;
CREATE TABLE feature_importance (
    feature    TEXT,
    importance FLOAT
);

DROP TABLE IF EXISTS cv_metrics;
CREATE TABLE cv_metrics (
    model      TEXT,
    best_cv_f1 FLOAT
);
EOF


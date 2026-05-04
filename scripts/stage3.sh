#!/bin/bash
export HADOOP_CONF_DIR=/etc/hadoop/conf
export YARN_CONF_DIR=/etc/hadoop/conf

spark-submit --master yarn scripts/model.py

pylint scripts/model.py || true

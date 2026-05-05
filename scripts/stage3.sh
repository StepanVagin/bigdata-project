#!/bin/bash
export HADOOP_CONF_DIR=/etc/hadoop/conf
export YARN_CONF_DIR=/etc/hadoop/conf

python3 -m venv .venv
source .venv/bin/activate


pip install --upgrade pip 
pip install -r requirements.txt

export PYSPARK_PYTHON="$(pwd)/.venv/bin/python3"          
spark-submit --master yarn scripts/model.py

pylint scripts/model.py || true

deactivate

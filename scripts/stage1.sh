#!/bin/bash
python3 scripts/build_projectdb.py

bash scripts/data_ingestion.sh

pylint scripts/build_projectdb.py || true
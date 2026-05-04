#!/bin/bash
mkdir -p data

wget -r -np -nH --cut-dirs=5 \
     -A "*.csv.gz" \
     https://www.ncei.noaa.gov/pub/data/swdi/stormevents/csvfiles/ \
     -P data/

gunzip data/*.gz

wget \
https://www.ncei.noaa.gov/pub/data/swdi/stormevents/csvfiles/ugc_areas.csv \
-P data/
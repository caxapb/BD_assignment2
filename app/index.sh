#!/bin/bash

INPUT="/index/data"

cd /app/mapreduce/
mapred streaming \
    -files mapper1.py,reducer1.py \
    -mapper 'python3 mapper1.py' \
    -reducer 'python3 reducer1.py' \
    -input $INPUT \
    -output /tmp/index_output

cd ..


# RUN APP.PY
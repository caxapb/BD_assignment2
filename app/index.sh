#!/bin/bash

cd /app/mapreduce/

INPUT="/index/data"
mapred streaming \
    -files mapper1.py,reducer1.py \
    -mapper 'python3 mapper1.py' \
    -reducer 'python3 reducer1.py' \
    -input $INPUT \
    -output /tmp/index_output

echo "Data is indexed, the reducer output is saved in /tmp/index_output/part-00000"
cd ..

python3 /app/app.py
echo "Data is loaded into Cassandra"